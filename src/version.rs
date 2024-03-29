use crate::record::{RecordReader, RecordWriter};

use super::key::InternalKey;
use super::options::Options;
use super::table::TableCache;

use anyhow::Result;
use integer_encoding::{VarInt, VarIntWriter};
use log::debug;

use std::collections::{BTreeSet, HashSet, LinkedList};

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use std::rc::Rc;

pub(crate) const NUM_OF_LEVELS: usize = 7;

pub const MANIFEST_FILE: &str = "MANIFEST";
#[allow(unused)]
pub const MANIFEST_REWRITE_FILE: &str = "MANIFEST-REWRITE";
#[derive(Clone, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct FileMetadata {
    pub allowed_seeks: usize,  // allowed seeks for the file (not used)
    pub num: usize,            // file number
    pub size: usize,           // file size
    pub smallest: InternalKey, // smallest internal key
    pub largest: InternalKey,  // largest internal key
}
impl Default for FileMetadata {
    fn default() -> Self {
        FileMetadata {
            allowed_seeks: 1 << 30,
            num: 0,
            size: 0,
            smallest: InternalKey::default(),
            largest: InternalKey::default(),
        }
    }
}

pub struct Version {
    pub files: Vec<Vec<Rc<FileMetadata>>>, // TODO: pub to pub(crate)
}

impl Version {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Version {
            files: vec![vec![]; NUM_OF_LEVELS], // TODO: init with opt.max_level.
        }
    }
}

impl Drop for Version {
    fn drop(&mut self) {
        // println!("self drop version {:p}", &*self);
    }
}
enum RecordID {
    // WAL的日志文件编号，用于标记当前正在使用的log的file_num
    LogNumber = 2,
    // 用于生成下一个文件编号（包括 WAL 和 SST文件)
    NextFileNumber = 3,
    // 最新的 Sequence
    LastSequence = 4,
    // 删除文件
    DeleteFile = 6,
    // 新增文件
    NewFile = 7,

    Unknow = 255,
}
impl From<u8> for RecordID {
    fn from(value: u8) -> Self {
        match value {
            2 => RecordID::LogNumber,
            3 => RecordID::NextFileNumber,
            4 => RecordID::LastSequence,
            6 => RecordID::DeleteFile,
            7 => RecordID::NewFile,
            _ => RecordID::Unknow,
        }
    }
}

pub struct VersionEdit {
    log_num: u64,
    next_file_num: u64,
    last_sequence: u64,
    deleted_files: Vec<(u32, u64)>,
    new_files: Vec<(u32, Rc<FileMetadata>)>, // u64 for which level to add
}

impl VersionEdit {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        VersionEdit {
            log_num: 0,
            next_file_num: 0,
            last_sequence: 0,
            deleted_files: vec![],
            new_files: vec![],
        }
    }
    pub fn set_log_number(&mut self, num: u64) {
        self.log_num = num;
    }
    pub fn set_next_file(&mut self, num: u64) {
        self.next_file_num = num;
    }
    pub fn set_last_sequence(&mut self, num: u64) {
        self.last_sequence = num;
    }
    pub fn delete_file(&mut self, level: u32, file_num: u64) {
        self.deleted_files.push((level, file_num));
    }

    pub fn add_file(&mut self, level: u32, file: Rc<FileMetadata>) {
        self.new_files.push((level, file));
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![];
        // record type + varint
        if self.last_sequence != 0 {
            buf.push(RecordID::LastSequence as u8);
            buf.write_varint(self.last_sequence).unwrap();
        }
        if self.next_file_num != 0 {
            buf.push(RecordID::NextFileNumber as u8);
            buf.write_varint(self.next_file_num).unwrap();
        }
        if self.log_num != 0 {
            buf.push(RecordID::LogNumber as u8);
            buf.write_varint(self.log_num).unwrap();
        }
        // record type + level(varint) + file num(varint)
        self.deleted_files.iter().for_each(|(level, file_num)| {
            buf.push(RecordID::DeleteFile as u8);
            buf.write_varint(*level).unwrap();
            buf.write_varint(*file_num).unwrap();
        });
        // record type + level(varint) + file num(varint) + file size(varint)
        // + smallest key  len(varint) + smallest key (bytes)
        // + largest key len(varint) + largest key (bytes)
        self.new_files.iter().for_each(|(level, file)| {
            buf.push(RecordID::NewFile as u8);
            buf.write_varint(*level).unwrap();
            buf.write_varint(file.num).unwrap();
            buf.write_varint(file.size).unwrap();
            buf.write_varint(file.smallest.size()).unwrap();
            buf.write_all(&file.smallest.clone()).unwrap();
            buf.write_varint(file.largest.size()).unwrap();
            buf.write_all(&file.largest.clone()).unwrap();
        });
        buf
    }

    #[allow(unused)]
    pub fn decode(buf: &[u8]) -> Self {
        let mut edit = Self::new();
        let mut pos = 0;
        while pos < buf.len() {
            let record_id = buf[pos];
            pos += 1;
            debug!("record id {}", record_id);
            match record_id.into() {
                RecordID::DeleteFile => {
                    let (level, bytes_read) = u32::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    let (file_num, bytes_read) = usize::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    edit.delete_file(level, file_num as u64);
                }
                RecordID::NewFile => {
                    let (level, bytes_read) = u32::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    debug!("new file level {level}");
                    let (file_num, bytes_read) = usize::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    debug!("new file num {file_num}");
                    let (size, bytes_read) = usize::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    debug!("new file size {size}");
                    let (smallest_size, bytes_read) = usize::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    let smallest = InternalKey::decode(&buf[pos..pos + smallest_size]);
                    pos += smallest_size;
                    debug!("new file len {smallest_size} smallest {:?}", smallest);
                    let (largest_size, bytes_read) = usize::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    let largest = InternalKey::decode(&buf[pos..pos + largest_size]);
                    debug!("new file len {largest_size} largest {:?}", largest);
                    pos += largest_size;
                    edit.add_file(
                        level,
                        Rc::new(FileMetadata {
                            num: file_num,
                            size,
                            ..Default::default()
                        }),
                    );
                }
                RecordID::LogNumber => {
                    let (log_num, bytes_read) = usize::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    edit.log_num = log_num as u64;
                }
                RecordID::NextFileNumber => {
                    let (next_file_num, bytes_read) = usize::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    edit.next_file_num = next_file_num as u64;
                }
                RecordID::LastSequence => {
                    let (last_sequence, bytes_read) = usize::decode_var(&buf[pos..]).unwrap();
                    pos += bytes_read;
                    edit.last_sequence = last_sequence as u64;
                }
                RecordID::Unknow => {
                    panic!("Unknown RecordID {}", record_id);
                }
            }
        }
        edit
    }
}

// Version 是 immutable 的
// 借助 VersionBuilder 来 apply VersionEdit 来生成 Version
struct VersionBuilder {
    opt: Options,
    // vset: &'a VersionSet,
    base: Rc<Version>,

    deleted_files: Vec<HashSet<u64>>,
    added_files: Vec<BTreeSet<Rc<FileMetadata>>>,
}

impl VersionBuilder {
    fn new(opt: Options, ver: Rc<Version>) -> Self {
        let mut added_files = Vec::new();
        let mut deleted_files = Vec::new();
        for _ in 0..opt.max_level {
            added_files.push(BTreeSet::new());
            deleted_files.push(HashSet::new());
        }
        VersionBuilder {
            opt,
            // vset,
            base: ver,
            deleted_files,
            added_files,
        }
    }
    fn apply(&mut self, edit: &VersionEdit) {
        // apply edit
        edit.deleted_files.iter().for_each(|(level, file_num)| {
            self.deleted_files[*level as usize].insert(*file_num);
        });
        edit.new_files.iter().for_each(|(level, file)| {
            self.added_files[*level as usize].insert(file.clone());
            self.deleted_files[*level as usize].remove(&(file.num as u64));
        });
    }
    fn save_to(&self, v: &mut Version) {
        // save to
        for level in 0..self.opt.max_level {
            let base_files = &self.base.files[level];
            let added_files = &self.added_files[level];
            v.files[level] = Vec::with_capacity(base_files.len() + added_files.len());
            let mut i = 0;
            for added_file in added_files {
                // add smallesr files in base file
                // TODO: use internal compare
                while i < base_files.len() && base_files[i].smallest < added_file.smallest {
                    self.maybe_add_file(v, level, base_files[i].clone());
                    i += 1;
                }
                // add added files
                self.maybe_add_file(v, level, added_file.clone());
            }
            // add rest of base files
            while i < base_files.len() {
                self.maybe_add_file(v, level, base_files[i].clone());
                i += 1;
            }
        }
    }
    fn maybe_add_file(&self, v: &mut Version, level: usize, f: Rc<FileMetadata>) {
        // maybe add file
        if !self.deleted_files[level].contains(&(f.num as u64)) {
            // deleted

            if level > 0 && !v.files[level].is_empty() {
                // level > 0 key 不可以重叠
                assert!(v.files[level].last().unwrap().largest < f.smallest);
            }
            v.files[level].push(f.clone());
        }
        // else deleted
    }
}
// VersionSet是Manifest在内存中的表现，
// 并且保留了多个时间点的Version，这些Version
// 代表了某一个时刻的Maniftest。
pub struct VersionSet {
    db_path: PathBuf,
    pub next_file_num: u64,
    pub log_num: u64,
    pub last_sequence: u64,
    current: Rc<Version>,
    versios: LinkedList<Rc<Version>>,
    pub table_cache: TableCache,
    #[allow(unused)]
    opt: Options,
}

impl<'a> VersionSet {
    pub fn current(&'a self) -> Rc<Version> {
        self.current.clone()
    }
    pub fn reclaim_obselete_files(&self) -> Result<()> {
        // reclaim obselete files
        // list files in db path
        let set: HashSet<u64> = self.live_files().iter().map(|f| f.num as u64).collect();
        for entry in std::fs::read_dir(&self.db_path)?.flatten() {
            if entry.path().extension().unwrap() == "sst" {
                let file_num = entry
                    .path()
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<u64>()?;
                if !set.contains(&file_num) {
                    std::fs::remove_file(entry.path())?;
                }
            }
        }
        Ok(())
    }
    // 从 MANIFEST 恢复 VersionSet 并且构建出一个 Version作为 Current.
    #[allow(clippy::new_without_default)]
    pub fn new(dir: &str, opt: Options) -> Result<Self> {
        let mut reader = RecordReader::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(format!("{dir}/{MANIFEST_FILE}"))?,
        );
        let mut vb = VersionBuilder::new(opt, Rc::new(Version::new()));
        let mut next_file_num = 0;
        let mut log_num = 0;
        let mut last_sequence = 0;
        loop {
            match reader.read_record() {
                Ok(record) => {
                    println!("replay record {:?}", record);
                    let edit = VersionEdit::decode(&record);
                    println!("apply edit record");
                    if edit.last_sequence != 0 {
                        last_sequence = edit.last_sequence;
                    }
                    if edit.next_file_num != 0 {
                        next_file_num = edit.next_file_num;
                    }
                    if edit.log_num != 0 {
                        log_num = edit.log_num;
                    }
                    vb.apply(&edit);
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
        let mut current = Version::new();
        vb.save_to(&mut current);
        Ok(VersionSet {
            db_path: PathBuf::from(dir),
            last_sequence,
            log_num,
            next_file_num,
            current: Rc::new(current),
            versios: LinkedList::new(),
            table_cache: TableCache::new(Options::default()),
            opt,
        })
    }
    // 应用 VersionEdit 生成一个新的 Version 作为 Current，
    // 并且持久化到MANIFEST当中，MANIFEST和WAL一样是基于Record的追加写文件。
    pub fn log_and_apply(&mut self, mut edit: VersionEdit) -> Result<()> {
        if self.last_sequence != 0 {
            edit.set_last_sequence(self.last_sequence);
        }
        if self.log_num != 0 {
            edit.set_log_number(self.log_num);
        }
        if self.next_file_num != 0 {
            edit.set_next_file(self.next_file_num);
        }
        // log and apply
        let mut v = Version::new();
        // build version
        let mut builder = VersionBuilder::new(Options::default(), self.current());
        builder.apply(&edit);
        builder.save_to(&mut v);
        let mut writer = RecordWriter::new(
            OpenOptions::new()
                .append(true)
                .open(self.db_path.join(MANIFEST_FILE))?,
        );
        writer.write_record(&edit.encode())?;
        writer.flush()?;
        self.append(v);
        Ok(())
    }
    // 更新 Version
    pub fn append(&mut self, v: Version) {
        let current = Rc::new(v);
        self.current = current.clone();
        self.versios.push_back(current);
    }
    // 获取被使用的files
    // 其中包括 current，和被引用的Version包含的文件
    // Version的rc count > 1 说明被引用，1 本身是被 VersionSet 引用。
    pub fn live_files(&self) -> Vec<Rc<FileMetadata>> {
        let mut live = BTreeSet::new();
        for v in &self.versios {
            // has ref count other than self.versions
            if Rc::strong_count(v) > 1 {
                for level in &v.files {
                    for f in level {
                        live.insert(f.clone());
                    }
                }
            }
        }
        live.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::key::InternalKeyKind;

    use super::*;
    use anyhow::Result;
    #[test]
    fn test_version_edit() {
        let mut edit = VersionEdit::new();
        edit.set_last_sequence(1024);
        edit.set_log_number(2);
        edit.set_next_file(3);
        edit.delete_file(1, 2);
        edit.add_file(
            3,
            Rc::new(FileMetadata {
                num: 4,
                ..Default::default()
            }),
        );
        let buf = edit.encode();
        let edit2 = VersionEdit::decode(&buf);
        assert_eq!(edit.deleted_files, edit2.deleted_files);
        assert_eq!(edit.new_files, edit2.new_files);
        assert_eq!(edit.last_sequence, edit2.last_sequence);
        assert_eq!(edit.log_num, edit2.log_num);
        assert_eq!(edit.next_file_num, edit2.next_file_num);
    }
    #[test]
    fn test_version_set() -> Result<()> {
        let db_path = "./test_dir/test_version_set";
        fs::create_dir_all(db_path)?;
        // let log_num = 0;
        // let log_path = format!("test_dir/test_version_set/{log_num}.log");
        // let wal = Arc::new(RwLock::new(WriteAheadLog::new(Path::new(&log_path))?));
        let mut vs = VersionSet::new("test_dir/test_version_set", Options::default()).unwrap();
        let v = Version::new();
        vs.append(v);
        //println!("V2");
        let v2 = Version::new();
        //v2.inc_ref();
        vs.append(v2);
        Ok(())
    }
    #[test]
    fn test_version_builder() {
        let mut v = Version::new();
        v.files[0] = vec![Rc::new(FileMetadata {
            num: 1,
            size: 100,
            smallest: InternalKey::new(b"a", 0, InternalKeyKind::Value),
            largest: InternalKey::new(b"z", 0, InternalKeyKind::Value),
            ..Default::default()
        })];
        let mut vb = VersionBuilder::new(Options::default(), Rc::new(v));
        let mut edit = VersionEdit::new();

        edit.delete_file(0, 1);
        edit.add_file(
            1,
            Rc::new(FileMetadata {
                num: 1,
                size: 100,
                smallest: InternalKey::new(b"a", 0, InternalKeyKind::Value),
                largest: InternalKey::new(b"z", 0, InternalKeyKind::Value),
                ..Default::default()
            }),
        );
        vb.apply(&edit);
        let mut v2 = Version::new();
        vb.save_to(&mut v2);
        assert_eq!(v2.files[0].len(), 0);
        assert_eq!(v2.files[1].len(), 1);
    }
}
