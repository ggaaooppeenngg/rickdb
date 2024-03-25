use std::fs::File;
use std::io::{self, Result};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
};

pub const MANIFEST_FILE: &str = "MANIFEST";
pub const MANIFEST_REWRITE_FILE: &str = "MANIFEST-REWRITE";
#[allow(unused)]
#[repr(u8)]
enum RecordID {
    DeleteFile = 6,
    NewFile = 7,

    Unknow = 255,
}

#[allow(unused)]
fn u8_to_id(byte: u8) -> RecordID {
    match byte {
        6 => RecordID::DeleteFile,
        7 => RecordID::NewFile,
        _ => RecordID::Unknow,
    }
}

#[allow(unused)]
#[derive(Clone)]
struct FileMetaData {
    file_num: u64,
    file_size: u64,
    smallest: Vec<u8>,
    largest: Vec<u8>,
}

impl FileMetaData {
    #[allow(unused)]
    pub fn with_id(id: u64) -> Self {
        FileMetaData {
            file_num: id,
            file_size: 0,
            smallest: vec![],
            largest: vec![],
        }
    }
}
#[allow(unused)]
#[derive(Default)]
pub struct ManifestEdit {
    deleted_files: Vec<(u32, u64)>,
    new_files: Vec<(u32, u64)>, // u64 for which level to add
}

impl ManifestEdit {
    pub fn new() -> Self {
        ManifestEdit {
            deleted_files: vec![],
            new_files: vec![],
        }
    }
    pub fn delete_file(&mut self, level: u32, file_num: u64) {
        self.deleted_files.push((level, file_num));
    }
    pub fn add_file(&mut self, level: u32, file_num: u64) {
        self.new_files.push((level, file_num));
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![];
        self.deleted_files.iter().for_each(|(level, file_num)| {
            buf.push(RecordID::DeleteFile as u8);
            buf.append(&mut level.to_varint_bytes());
            buf.append(&mut file_num.to_varint_bytes());
        });
        self.new_files.iter().for_each(|(level, file_num)| {
            buf.push(RecordID::NewFile as u8);
            buf.append(&mut level.to_varint_bytes());
            buf.append(&mut file_num.to_varint_bytes());
        });
        buf
    }

    #[allow(unused)]
    pub fn decode(buf: &[u8]) -> Self {
        let mut edit = ManifestEdit {
            deleted_files: vec![],
            new_files: vec![],
        };
        let mut pos = 0;
        while pos < buf.len() {
            let record_id = buf[pos];
            pos += 1;
            let (level, bytes_read) = u32::from_varint_bytes(&buf[pos..]);
            pos += bytes_read;
            let (file_num, bytes_read) = u64::from_varint_bytes(&buf[pos..]);
            pos += bytes_read;
            match u8_to_id(record_id) {
                RecordID::DeleteFile => {
                    edit.delete_file(level, file_num);
                }
                RecordID::NewFile => {
                    edit.add_file(level, file_num);
                }
                RecordID::Unknow => {
                    panic!("Unknown RecordID");
                }
            }
        }
        edit
    }
}
// #[allow(unused)]
// pub struct Version {
//     level_files: Vec<Vec<FileMetaData>>,
// }
// #[allow(unused)]
// pub struct VersionSet {
//     next_file_num: u64,
//     level_files: Vec<Vec<FileMetaData>>,
// }
#[allow(unused)]
pub struct Manifest {
    writer: RecordWriter<File>,
    file_path: PathBuf,
    directory: PathBuf,

    levels: Vec<HashSet<u64>>,
    tables: HashMap<u64, usize>,

    rewrite_threshold: usize,
    shrink_ratio: usize,

    deletions: usize,
    creations: usize,
}

impl Manifest {
    pub fn tables(&self) -> &HashMap<u64, usize> {
        &self.tables
    }
    pub fn levels(&self) -> &Vec<HashSet<u64>> {
        &self.levels
    }
    pub fn new(directory: &Path, name: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(directory.join(name))?;
        let writer = RecordWriter::new(file);
        let directory = PathBuf::from(directory);
        Ok(Manifest {
            file_path: directory.join(name),
            writer,
            directory,
            levels: vec![],
            tables: HashMap::new(),
            rewrite_threshold: 1000,
            shrink_ratio: 10,
            deletions: 0,
            creations: 0,
        })
    }
    #[allow(unused)]
    pub fn as_edit(&self) -> ManifestEdit {
        let mut edit = ManifestEdit {
            deleted_files: vec![],
            new_files: vec![],
        };
        self.levels.iter().enumerate().for_each(|(level, files)| {
            files.iter().for_each(|file_num| {
                edit.add_file(level as u32, *file_num);
            })
        });
        edit
    }
    // TODO: rewrite 要加锁，防止rewrite时有其他的append。
    #[allow(unused)]
    pub fn rewrite(&self) -> Result<()> {
        let mut manifest = Manifest::new(&self.directory, Path::new(MANIFEST_REWRITE_FILE))?;
        manifest.apply_and_append_edit(&self.as_edit())?;
        // println!("rename");
        fs::rename(self.directory.join(MANIFEST_REWRITE_FILE), &self.file_path)?;
        Ok(())
    }
    #[allow(unused)]
    pub fn replay(&mut self) -> Result<()> {
        let mut reader = RecordReader::new(OpenOptions::new().read(true).open(&self.file_path)?);
        loop {
            match reader.read_record() {
                Ok(record) => {
                    println!("replay record {:?}", record);
                    let edit = ManifestEdit::decode(&record);
                    println!("apply edit record");
                    self.apply_edit(&edit);
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }
    fn apply_edit(&mut self, edit: &ManifestEdit) {
        edit.deleted_files.iter().for_each(|(level, file_num)| {
            if self.levels.len() < *level as usize + 1 {
                self.levels.resize(*level as usize + 1, HashSet::new());
            }
            self.levels[*level as usize].remove(file_num);
            self.tables.remove(file_num);
            self.deletions += 1;
        });
        edit.new_files.iter().for_each(|(level, file_num)| {
            if self.levels.len() < *level as usize + 1 {
                self.levels.resize(*level as usize + 1, HashSet::new());
            }
            self.levels[*level as usize].insert(*file_num);
            self.tables.insert(*file_num, *level as usize);
            self.creations += 1;
        });
    }
    #[allow(unused)]
    pub fn apply_and_append_edit(&mut self, edit: &ManifestEdit) -> Result<()> {
        self.apply_edit(edit);
        // TODO: 重写文件要加锁。
        if self.deletions > self.rewrite_threshold
            && self.deletions > self.shrink_ratio * (self.creations - self.deletions)
        {
            todo!();
        } else {
            let record = edit.encode();
            println!("append edit {:?}", &record);
            self.writer.write_record(&record)?;
            self.writer.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    #[test]
    fn test_manifest_edit() {
        let mut edit = ManifestEdit {
            deleted_files: vec![],
            new_files: vec![],
        };
        edit.delete_file(0, 1);
        edit.add_file(0, 2);
        let buf = edit.encode();
        let edit = ManifestEdit::decode(&buf);
        assert_eq!(edit.deleted_files.len(), 1);
        assert_eq!(edit.new_files.len(), 1);
    }
    #[test]
    fn test_manifest() -> Result<()> {
        // ignore error
        let _ = fs::remove_file(PathBuf::from(MANIFEST_FILE));
        let mut manifest = Manifest::new(Path::new("./"), Path::new(MANIFEST_FILE))?;
        manifest.apply_and_append_edit(&ManifestEdit {
            deleted_files: vec![],
            new_files: vec![(0, 2)],
        })?;
        manifest.apply_and_append_edit(&ManifestEdit {
            deleted_files: vec![(0, 2)],
            new_files: vec![(0, 3)],
        })?;
        // add 4 and 5 at level 1
        manifest.apply_and_append_edit(&ManifestEdit {
            deleted_files: vec![],
            new_files: vec![(1, 4), (1, 5)],
        })?;
        // remove 4 at level 1
        manifest.apply_and_append_edit(&ManifestEdit {
            deleted_files: vec![(1, 4)],
            new_files: vec![],
        })?;
        let mut m = Manifest::new(Path::new("./"), Path::new(MANIFEST_FILE))?;
        m.replay()?;
        assert!(!m.levels.get(0).unwrap().contains(&2));
        assert!(m.levels.get(0).unwrap().contains(&3));
        assert!(m.levels.get(1).unwrap().contains(&5));
        assert!(!m.levels.get(1).unwrap().contains(&4));
        let edit = m.as_edit();
        edit.new_files.iter().for_each(|(level, file_num)| {
            assert!(m.levels[*level as usize].contains(file_num));
        });
        m.rewrite()?;
        let mut m = Manifest::new(Path::new("./"), Path::new(MANIFEST_FILE))?;
        m.replay()?;
        assert!(!m.levels.get(0).unwrap().contains(&2));
        assert!(m.levels.get(0).unwrap().contains(&3));
        assert!(m.levels.get(1).unwrap().contains(&5));
        assert!(!m.levels.get(1).unwrap().contains(&4));
        Ok(())
    }
}
