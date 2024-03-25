use std::cmp::Ordering;

use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

use log::debug;

use crate::key::user_key;
use crate::key::InternalKey;
use crate::key::InternalKeyKind;
use crate::cmp;
use crate::cmp::InternalComparator;
use crate::cmp::UserKeyComparator;
use crate::options::Options;
use crate::table::ChainedIterator;
use crate::table::MergingIterator;
use crate::table::PeekableIter;
use crate::table::TableBuilder;
use crate::version::FileMetadata;
use crate::version::Version;
use crate::version::VersionSet;
use crate::version::NUM_OF_LEVELS;

use crate::version::VersionEdit;

#[allow(unused)]
struct CompactionLevel {
    // files in this level
    files: Vec<Rc<FileMetadata>>,
    // level index
    level: usize,
}

// TODO: Compaction vs PickedCompaction
#[allow(unused)]
pub struct Compaction {
    socre: f64,
    opt: Options,
    db_path: PathBuf,
    // ver: &'a Version,
    // immutable
    // inputs[0] 长度为1，如果用了compact_range就会大于1
    // 或者level=0的话也会大于1。
    start_level: CompactionLevel, // start level 不可变
    // inputs[1]
    output_level: CompactionLevel,
    grandparent_level: CompactionLevel, // 用来计算 level+2 的overlap
    // extra_levels: Vec<CompactionLevel<'a>>, // 用来计算重复的。
    // mutable
    // inputs: Vec<CompactionLevel<'a>>, // inputs 包含了start_level和output_level的文件
    edit: VersionEdit,
}
impl Compaction {
    fn is_trivial_move(&self) -> bool {
        // trivial move 的判断
        // 如果是一个文件向level+1移动，但是grandparent的overlap太大就不移动。
        // 因为啥? 这里隐含了一个假设，key的分布是均匀的。
        // 如果不是均匀的，那么这个判断就不准确了。
        // leveldb有个隐含的假设就是key的分布是均匀的。
        // 对应leveldb里面 db/db_impl.cc 的731行左右。
        let input_size = self
            .grandparent_level
            .files
            .iter()
            .fold(0, |acc, f| acc + f.size);
        debug!("start level files len {}", self.start_level.files.len());
        debug!("max grandparent overlap size {}", self.max_grandparent_overlap_bytes());
        debug!("output level files len {}", self.output_level.files.len());
        debug!("grandparent overlap size {}", input_size);
        self.start_level.files.len() == 1
            && self.output_level.files.is_empty()
            && input_size < self.max_grandparent_overlap_bytes()
    }
    // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
    // stop building a single file in a level->level+1 compaction.
    fn max_grandparent_overlap_bytes(&self) -> usize {
        10 * self.opt.max_file_size
    }
}
pub fn key_range(
    file_slice: impl Iterator<Item = Rc<FileMetadata>>,
) -> Option<(InternalKey, InternalKey)> {
    // smallest and largest pair
    let mut slp: Option<(InternalKey, InternalKey)> = None;
    for file in file_slice {
        slp = Some(slp.map_or(
            (file.smallest.clone(), file.largest.clone()),
            |(smallest, largest)| {
                if (InternalComparator.cmp)(&file.smallest, &smallest) == Ordering::Less {
                    (file.smallest.clone(), largest)
                } else if (InternalComparator.cmp)(&file.largest, &largest) == Ordering::Greater {
                    (smallest, file.largest.clone())
                } else {
                    (smallest, largest)
                }
            },
        ));
    }
    slp
}

// setup inputs 的作用就是make clean boundry.
// 一个轮转文件挑选器
#[allow(unused)]
pub struct Compactor {
    opt: Options,
    db_path: PathBuf,
    // 上次 compaction 选择的文件的 largest key
    compaction_pointer: Vec<InternalKey>,
}

impl Version {
    pub fn level_size(&self, level: usize) -> usize {
        if self.files.len() <= level {
            0
        } else {
            self.files[level].iter().fold(0, |acc, f| acc + f.size)
        }
    }
    pub fn overlaps(
        &self,
        cmp: cmp::Comparator,
        level: usize,
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<Rc<FileMetadata>> {
        assert_eq!(cmp.name, "BytewiseComparator");
        let mut all_smallest = user_key(smallest);
        let mut all_largest = user_key(largest);
        let mut continue_searching = false;
        loop {
            let overlapped_files: Vec<Rc<FileMetadata>> = self.files[level]
                .iter()
                .filter(|f| {
                    let overlaps = !((cmp.cmp)(user_key(&f.largest), all_smallest)
                        == Ordering::Less
                        || (cmp.cmp)(user_key(&f.smallest), all_largest) == Ordering::Greater);
                    // level 0 是可以重叠的，所以新文件的边界如果更宽
                    // 就需要更新新的边界，然后重新搜索。
                    if overlaps && level == 0 {
                        if (cmp.cmp)(user_key(&f.smallest), all_smallest) == Ordering::Less {
                            all_smallest = user_key(&f.smallest);
                            continue_searching = true;
                        }
                        if (cmp.cmp)(user_key(&f.largest), all_largest) == Ordering::Greater {
                            all_largest = user_key(&f.largest);
                            continue_searching = true
                        }
                    }
                    overlaps
                })
                .cloned()
                .collect();
            if continue_searching {
                continue_searching = false;
                continue;
            } else {
                return overlapped_files;
            }
        }
    }
}

fn find_largest_key(
    cmp: cmp::Comparator,
    files: &[Rc<FileMetadata>],
) -> Option<&InternalKey> {
    files.iter().fold(None, |acc, f| match acc {
        None => Some(&f.largest),
        Some(largest) if (cmp.cmp)(&f.largest, largest) == Ordering::Greater => Some(&f.largest),
        Some(_) => acc,
    })
}
// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
fn find_smallest_boundary_file(
    files: &[Rc<FileMetadata>],
    largest_key: &InternalKey,
) -> Option<Rc<FileMetadata>> {
    // user_key(l2) = user_key(u1)
    // find smallest boundary file
    files
        .iter()
        .filter(|f| {
            // filter l2 > u1 and user_key(l2) == user_key(u1)
            (cmp::InternalComparator.cmp)(&f.smallest, largest_key) == Ordering::Greater
                && (cmp::UserKeyComparator.cmp)(user_key(&f.smallest), user_key(largest_key))
                    == Ordering::Equal
        })
        .fold(None, |acc, f| match acc {
            None => Some(f.clone()),
            Some(file)
                if (cmp::InternalComparator.cmp)(&f.smallest, &file.smallest) == Ordering::Less =>
            {
                Some(f.clone())
            }
            Some(_) => acc,
        })
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.

// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.

// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
//
// pebble: expaned_to_atomic_unit
// leveldb: AddBoundaryInputs
// TODO: Consider left side compaction files if range delete added like pebble.
fn add_boundary_inputs(
    cmp: cmp::Comparator,
    compaction_files: &mut Vec<Rc<FileMetadata>>,
    level_files: &[Rc<FileMetadata>],
) {
    if let Some(key) = find_largest_key(cmp, compaction_files) {
        let mut largest_key = key.clone();
        println!("largest key {:?}", largest_key);
        while let Some(smallest_boundary_file) =
            find_smallest_boundary_file(level_files, &largest_key)
        {
            println!("find boundary file num {}", smallest_boundary_file.num);
            // Add smallest_boundary_file to compaction_files
            // nd update largest_key.
            largest_key = smallest_boundary_file.largest.clone();
            compaction_files.push(smallest_boundary_file);
        }
    };
}
impl Compactor {
    // TODO: consider compactions in progress.
    #[allow(unused)]
    pub fn init_level_max_bytes() {}
    #[allow(unused)]
    pub fn new(db_path: PathBuf, opt: Options) -> Self {
        let mut compaction_pointer = vec![];
        for _ in 0..opt.max_level {
            compaction_pointer.push(InternalKey::new(b"", 0, InternalKeyKind::Value));
        }
        Compactor {
            opt,
            db_path,
            compaction_pointer,
        }
    }

    fn expanded_compaction_byte_size_limit(&self) -> usize {
        25 * self.opt.max_file_size
    }
    #[allow(unused)]
    pub fn start_compaction(&mut self, vs: &mut VersionSet) {
        // TODO: Sized Compaction
        if let Some(compaction) = self.pick_compaction(vs.current()) {
            // delete obsolete files
            // trivial move 的判断
            // 如果是一个文件向level+1移动，但是grandparent的overlap太大就不移动。
            // 因为啥? 这里隐含了一个假设，key的分布是均匀的。
            // 如果不是均匀的，那么这个判断就不准确了。
            // leveldb有个隐含的假设就是key的分布是均匀的。
            // 对应leveldb里面 db/db_impl.cc 的731行左右。
            if compaction.is_trivial_move() {
                debug!("is trival move");
                let start_level = compaction.start_level.level;
                let output_level = compaction.output_level.level;
                let file_num = compaction.start_level.files[0].num;
                let mut edit = VersionEdit::new();
                edit.delete_file(start_level as u32, file_num as u64);
                edit.add_file(
                    output_level as u32,
                    Rc::new(FileMetadata {
                        num: file_num,
                        size: compaction.start_level.files[0].size,
                        smallest: compaction.start_level.files[0].smallest.clone(),
                        largest: compaction.start_level.files[0].largest.clone(),
                        ..Default::default()
                    }),
                );
                vs.log_and_apply(edit);
                debug!("after trivial move compaction");
                for (level, files) in vs.current().files.iter().enumerate() {
                    debug!("level {level}");
;                    for file in files{
                        debug!("file num {}", file.num);
                    }
                }
            } else {
                // merge and save
                // leveldb::DBImpl::DoCompactionWork
                debug!(
                    "Compacting  {}@{} + {}@{} files",
                    compaction.start_level.files.len(),
                    compaction.start_level.level,
                    compaction.output_level.files.len(),
                    compaction.output_level.level
                );
                assert!(compaction.start_level.level > 0);
                // TODO: CompactionState
                // smallest_snapshot
                // 考虑到 level 0 是可以重叠的，如果是 level 0 那每个文件作为单独一个iterator
                // 如果不是 level 0 则整个level的files串联作为一个iterator即可。
                let iter = self.make_input_iterator(vs, &compaction);

                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(format!("{}/{}.sst", self.db_path.to_string_lossy(), vs.next_file_num))
                    .unwrap();
                let mut table_builder = TableBuilder::new(&mut file, self.opt);
                for (k, v) in iter {
                    table_builder.add(&k, &v);
                }
                table_builder.finish();
                // after compaction, try to reclaim unused files.
                vs.reclaim_obselete_files();
                debug!("after compaction");
                for (level, files) in vs.current().files.iter().enumerate() {
                    debug!("level {level}");
;                    for file in files{
                        debug!("file num {}", file.num);
                    }
                }

            }
        }
    }
    fn make_input_iterator(
        &self,
        vs: &mut VersionSet,
        compaction: &Compaction,
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        let cap = if compaction.start_level.level == 0 {
            compaction.start_level.files.len()
        } else {
            1
        } + 1; // output level iterator cap;
        let mut iters = Vec::<PeekableIter>::with_capacity(cap);
        if compaction.start_level.level == 0 {
            let start_level_iters: Vec<PeekableIter> = compaction
                .start_level
                .files
                .iter()
                .map(|f| vs.table_cache.get_table(f.num as u64).iter().peekable())
                .collect();
            iters.extend(start_level_iters);
            // initialize output level iterator with empty file.
        } else {
            let mut start_level_iter = ChainedIterator::new(vec![]);
            compaction.start_level.files.iter().for_each(|f| {
                let file = vs.table_cache.get_table(f.num as u64);
                start_level_iter.push(file.iter())
            });
            iters.push(
                (Box::new(start_level_iter) as Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>)
                    .peekable(),
            );
        }
        let mut output_level_iter = ChainedIterator::new(vec![]);
        compaction.output_level.files.iter().for_each(|f| {
            let file = vs.table_cache.get_table(f.num as u64);
            output_level_iter.push(file.iter())
        });
        iters.push(
            (Box::new(output_level_iter) as Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>)
                .peekable(),
        );
        debug!("new merging iterator");
        MergingIterator::new(iters, self.opt)
    }
    #[allow(unused)]
    fn setup_inputs(&mut self, pc: &mut Compaction, ver: Rc<Version>) {
        add_boundary_inputs(
            InternalComparator,
            &mut pc.start_level.files,
            &ver.clone().files[pc.start_level.level],
        );
        for f in &pc.start_level.files {
            debug!("add boundary input num  {}", f.num);
        }
        // range of the start level
        if let Some((mut smallest, mut largest)) =
            key_range(pc.start_level.files.clone().into_iter())
        {
            pc.output_level.files = ver.overlaps(
                UserKeyComparator, // user key comparator
                pc.output_level.level,
                &smallest,
                &largest,
            );
            add_boundary_inputs(
                InternalComparator,
                &mut pc.output_level.files,
                &ver.files[pc.output_level.level],
            );
            // TODO: make the iterator a sorted data structure
            // 这样就不用遍历files去找最小值和最大值。
            // range of the start and output level.
            if let Some((mut all_smallest, mut all_largest)) = key_range(
                pc.start_level
                    .files
                    .clone()
                    .into_iter()
                    .chain(pc.output_level.files.clone()),
            ) {
                // 尝试加入更多的level中的文件，提高合并率。
                // See if we can grow the number of inputs in "level" without
                // changing the number of "level+1" files we pick up.
                if !pc.output_level.files.is_empty() {
                    let mut start_expanded = ver.overlaps(
                        UserKeyComparator,
                        pc.start_level.level,
                        &all_smallest,
                        &all_largest,
                    );
                    add_boundary_inputs(
                        InternalComparator,
                        &mut start_expanded,
                        &ver.files[pc.start_level.level],
                    );
                    let start_level_size: usize =
                        pc.start_level.files.iter().map(|file| file.size).sum();
                    let output_level_size: usize =
                        pc.output_level.files.iter().map(|file| file.size).sum();
                    let start_expaned_size: usize =
                        start_expanded.iter().map(|file| file.size).sum();
                    if start_expanded.len() > pc.start_level.files.len()
                        && start_expaned_size + output_level_size
                            < self.expanded_compaction_byte_size_limit()
                    {
                        if let Some((new_smallest, new_largest)) =
                            key_range(start_expanded.clone().into_iter())
                        {
                            let mut output_expanded = ver.overlaps(
                                UserKeyComparator,
                                pc.output_level.level,
                                &new_smallest,
                                &new_largest,
                            );
                            add_boundary_inputs(
                                InternalComparator,
                                &mut output_expanded,
                                &ver.files[pc.output_level.level],
                            );

                            if output_expanded.len() == pc.output_level.files.len() {
                                println!("expanding inputs");
                                pc.start_level.files = start_expanded;
                                pc.output_level.files = output_expanded;
                                if let Some((new_all_smallest, new_all_largest)) = key_range(
                                    pc.start_level
                                        .files
                                        .clone()
                                        .into_iter()
                                        .chain(pc.output_level.files.clone()),
                                ) {
                                    all_largest = new_all_largest;
                                    all_smallest = new_all_smallest;
                                    largest = new_largest;
                                    smallest = new_smallest;
                                }
                            }
                        }
                    }
                }
                // TODO: grandparents
                // 暂时不知道parents的作用
                // TODO: compaction pointer
                // 标记 compaction pointer 需要增加一种 version edit
                // 但是roundroubin的方式每次重启的时候从头开始感觉也没啥关系。
                self.compaction_pointer[pc.start_level.level] = largest;
                if pc.start_level.level + 2 < pc.opt.max_level {
                    pc.grandparent_level.files = ver.overlaps(
                        UserKeyComparator,
                        pc.start_level.level + 2,
                        &all_smallest,
                        &all_largest,
                    );
                }
            }
        }
    }
    // picked compaction 和 version 的生命周期一致。
    #[allow(unused)]
    fn pick_compaction(&mut self, ver: Rc<Version>) -> Option<Compaction> {
        // scores
        let mut best_score_lvl = None;
        let mut max_bytes = self.opt.base_level_size;
        for l in 0..self.opt.max_level - 1 {
            max_bytes *= self.opt.level_size_multiplier;
            let mut score = 0f64;
            if l == 0 {
                score = ver.files[l].len() as f64 / self.opt.level0_file_num as f64;
            } else {
                score = ver.level_size(l) as f64 / max_bytes as f64;
            }
            debug!("level {} score {}", l, score);
            // update best score which larget than 1
            // 副作用更新 level
            best_score_lvl = best_score_lvl
                .map(|(s, lvl)| if s < score  { (score, l) } else { (s, lvl) })
                .or(Some((score, l)))
            
        }

        // pick file in round roubin style.
        let mut picked_file: Option<Rc<FileMetadata>> = None;
        if let Some(best_score_lvl) = best_score_lvl {
            let this_level = best_score_lvl.1;
            let score = best_score_lvl.0;
            if score < 1.0{
                debug!("needs no compaction.");
                return None;
            }
            debug!(
                "pick compaction best score {}, level {}",
                best_score_lvl.0, best_score_lvl.1
            );
            // files
            for file in &ver.files[this_level][..] {
                if self.compaction_pointer[this_level].is_empty()
                    || (InternalComparator.cmp)(&self.compaction_pointer[this_level], &file.largest)
                        == Ordering::Less
                {
                    debug!("compac pointer {:?}", self.compaction_pointer[this_level]);
                    debug!("file largest {:?}", file.largest);
                    debug!("picking file {}", file.num);
                    picked_file = Some(file.clone());
                    break;
                }
            }
            if picked_file.is_none() && !ver.files[this_level].is_empty() {
                picked_file = Some(ver.files[this_level][0].clone());
            }
            // do not pick file from max level
            if this_level + 1 == NUM_OF_LEVELS {
                return None;
            }
            debug!(
                "picked file num {} [{:?}:{:?}] at level {}",
                picked_file.clone().unwrap().num,
                picked_file.clone().unwrap().smallest,
                picked_file.clone().unwrap().largest,
                this_level
            );
            let mut pc = Compaction {
                socre: score,
                //ver,
                opt: self.opt,
                db_path: self.db_path.clone(),
                start_level: CompactionLevel {
                    files: vec![picked_file.clone().unwrap()],
                    level: this_level,
                },
                output_level: CompactionLevel {
                    files: ver.files[this_level + 1].clone(),
                    level: this_level + 1,
                },
                grandparent_level: CompactionLevel {
                    files: vec![],
                    level: this_level + 2,
                },
                edit: VersionEdit::new(),
            };
            self.setup_inputs(&mut pc, ver);
            return Some(pc);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::key::{InternalKey, InternalKeyKind};
    use super::super::version::FileMetadata;
    use std::path::PathBuf;
    fn new_ikey(s: &[u8], n: u64) -> InternalKey {
        InternalKey::new(s, n, InternalKeyKind::Value)
    }
    fn new_file(
        num: usize,
        size: usize,
        smallest: InternalKey,
        largest: InternalKey,
    ) -> Rc<FileMetadata> {
        Rc::new(FileMetadata {
            num,
            size,
            smallest,
            largest,
            ..Default::default()
        })
    }
    #[test]
    fn test_pick_compaction() {
        // level 0 to empty level 1.
        let mut picker = Compactor::new(PathBuf::new(),Options::default());
        let mut v = Version::new();
        v.files[0] = vec![
            new_file(1, 1, new_ikey(b"a", 1), new_ikey(b"b", 3)),
            new_file(2, 1, new_ikey(b"b", 2), new_ikey(b"c", 2)),
            new_file(3, 1, new_ikey(b"c", 1), new_ikey(b"d", 5)),
            new_file(4, 1, new_ikey(b"d", 4), new_ikey(b"d", 3)),
            new_file(5, 1, new_ikey(b"d", 2), new_ikey(b"d", 1)),
            new_file(6, 1, new_ikey(b"e", 3), new_ikey(b"e", 2)),
        ];
        let v = Rc::new(v);
        let c = picker.pick_compaction(v.clone());
        assert!(c.is_some());
        // pick file 1
        if let Some(c) = c {
            assert_eq!(c.start_level.files.len(), 5);
            assert_eq!(c.start_level.files[0].num, 1);
            assert_eq!(c.output_level.files.len(), 0);
        }
        assert_eq!(picker.compaction_pointer[0], new_ikey(b"d", 1));
        let c = picker.pick_compaction(v);
        // pick file 2
        if let Some(c) = c {
            assert_eq!(c.start_level.files.len(), 1);
            assert_eq!(c.start_level.files[0].num, 6);
            assert_eq!(c.output_level.files.len(), 0);
        }
    }

    #[test]
    fn test_overlaps() {
        let f1 = new_file(1, 1, new_ikey(b"a", 1), new_ikey(b"b", 2));
        let f2 = new_file(2, 1, new_ikey(b"b", 3), new_ikey(b"c", 2));
        let f3 = new_file(3, 1, new_ikey(b"c", 1), new_ikey(b"d", 5));
        let f4 = new_file(4, 1, new_ikey(b"d", 4), new_ikey(b"d", 3));
        let f5 = new_file(5, 1, new_ikey(b"d", 2), new_ikey(b"d", 1));
        let f6 = new_file(6, 1, new_ikey(b"e", 3), new_ikey(b"e", 2));

        let mut v = Version::new();
        v.files[0] = vec![f1, f2, f3, f4, f5, f6];
        let overlaps = v.overlaps(
            UserKeyComparator,
            0,
            &InternalKey::new(b"a", 1, InternalKeyKind::Value),
            &InternalKey::new(b"b", 2, InternalKeyKind::Value),
        );
        assert_eq!(overlaps.len(), 5);
        let overlaps = v.overlaps(
            UserKeyComparator,
            0,
            &InternalKey::new(b"e", 3, InternalKeyKind::Value),
            &InternalKey::new(b"e", 1, InternalKeyKind::Value),
        );
        assert_eq!(overlaps.len(), 1);
    }
    #[test]
    fn add_boundary_files() {
        let b1 = new_file(1, 1, new_ikey(b"a", 1), new_ikey(b"b", 2));
        let b2 = new_file(2, 1, new_ikey(b"b", 1), new_ikey(b"c", 2));
        let b3 = new_file(3, 1, new_ikey(b"c", 1), new_ikey(b"d", 5));
        let b4 = new_file(4, 1, new_ikey(b"d", 4), new_ikey(b"d", 3));
        let b5 = new_file(5, 1, new_ikey(b"d", 2), new_ikey(b"d", 1));
        let b6 = new_file(6, 1, new_ikey(b"e", 3), new_ikey(b"e", 2));

        let mut compaction_files = vec![b1.clone()];
        let level_files = vec![b1, b2, b3, b4, b5, b6];
        add_boundary_inputs(InternalComparator, &mut compaction_files, &level_files);
        compaction_files
            .iter()
            .zip(vec![1, 2, 3, 4, 5])
            .for_each(|(f, n)| {
                assert_eq!(f.num, n);
            });
    }
    #[test]
    fn test_key_range() {
        let files = vec![
            new_file(1, 1, new_ikey(b"1", 1), new_ikey(b"1", 1)),
            new_file(2, 1, new_ikey(b"2", 1), new_ikey(b"2", 1)),
            new_file(3, 1, new_ikey(b"3", 1), new_ikey(b"3", 1)),
        ];
        if let Some((smallest, largest)) = key_range(files.into_iter()) {
            assert_eq!(smallest, InternalKey::new(b"1", 1, InternalKeyKind::Value));
            assert_eq!(largest, InternalKey::new(b"3", 1, InternalKeyKind::Value));
        }
    }
}
