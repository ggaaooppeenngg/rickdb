use crate::compaction::Compactor;
use crate::options::Options;
use crate::table::TableBuilder;
use crate::version::{FileMetadata, VersionEdit, VersionSet};
use anyhow::Result;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{rc::Rc, sync::RwLock};

use super::batch::Batch;
use super::key::{InternalKey, InternalKeyKind};
use super::memtable::MemTable;
use super::wal::WriteAheadLog;
use log::debug;

struct GuardedState {
    mt: MemTable,
    imm: MemTable,
    vs: VersionSet,
}

pub struct DB {
    shared_state: Arc<RwLock<GuardedState>>,
    opt: Options,
    db_path: PathBuf,
    log: Arc<RwLock<WriteAheadLog>>,
}

impl DB {
    pub fn new(dir: &str, opt: Options) -> Result<Self> {
        // TODO: manifest
        fs::create_dir_all(dir)?;
        let vs = VersionSet::new(dir, opt)?;
        debug!("vs last sequence {}", vs.last_sequence);
        debug!("vs log num {}", vs.log_num);
        debug!("vs file num {}", vs.next_file_num);
        let log_num = vs.log_num;
        let log_path = format!("{dir}/{log_num}.log");
        let wal = Arc::new(RwLock::new(WriteAheadLog::new(Path::new(&log_path))?));
        let shared_state = Arc::new(RwLock::new(GuardedState {
            mt: MemTable::new(),
            imm: MemTable::new(),
            vs,
        }));

        let mut write_guard = shared_state.write().unwrap();
        // replay wal
        wal.write()
            .unwrap()
            .read_records()?
            .into_iter()
            .for_each(|record| {
                let batch = Batch::from(record);
                let mut seq = batch.get_seqnum();
                seq = seq.wrapping_add(1);
                batch.iter().for_each(|(key, value, kind)| {
                    if kind == InternalKeyKind::Value as u8 {
                        let search_key = InternalKey::new(key, seq, InternalKeyKind::Value);
                        debug!("replay insert key {}", search_key);
                        write_guard.mt.insert(search_key, value.unwrap().to_vec());
                    } else {
                        // skip kind == deletion
                    }
                    seq += 1;
                });
                // 还没有log apply 到 MANIFEST 生成新Version，
                // sequence需要根据WAL重放回来。
                write_guard.vs.last_sequence = seq;
            });

        let db = DB {
            // Instantiate the WriteAheadLog struct
            log: wal,
            shared_state: shared_state.clone(),
            db_path: PathBuf::from(dir),
            opt,
        };
        Ok(db)
    }
    pub fn range_all(&self) -> Vec<(InternalKey, Vec<u8>)> {
        self.shared_state.read().unwrap().mt.range_all()
    }
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let seq = self.shared_state.read().unwrap().vs.last_sequence;
        self.get_internal(key, seq)
    }
    #[allow(unused)]
    fn get_internal(&self, key: &[u8], seq: u64) -> Option<Vec<u8>> {
        let lookup = InternalKey::new(key, seq, InternalKeyKind::Value); //TODO: Kind==Searchc
        if let Some(value) = self.shared_state.read().unwrap().mt.get_ge(&lookup) {
            if lookup.user_key() == key {
                debug!(
                    "get internal key {}, trailer {}",
                    lookup,
                    lookup.trailer() & 0xff
                );
                return Some(value.clone());
            }
        }
        None
    }
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut batch = Batch::new();
        batch.put(&key, &value);
        self.write(batch, true)
    }
    pub fn write(&mut self, batch: Batch, sync: bool) -> Result<()> {
        self.make_room_for_write()?;
        let mut protected_data = self.shared_state.write().unwrap();
        let seq = protected_data.vs.last_sequence + 1;
        let mut count = 0;
        batch.iter().for_each(|(key, value, _)| {
            if let Some(value) = value {
                let value_key = InternalKey::new(key, seq + count, InternalKeyKind::Value);
                debug!("insert key {}", value_key);
                protected_data.mt.insert(value_key, value.to_vec());
            } else {
                let delete_key = InternalKey::new(key, seq + count, InternalKeyKind::Deletion);
                debug!("delete key {}", delete_key);
                protected_data.mt.insert(delete_key, vec![]);
            }
            count += 1;
        });
        self.log
            .write()
            .unwrap()
            .append_record(&batch.encode(seq))?;
        if sync {
            self.log.write().unwrap().flush()?;
        }
        protected_data.vs.last_sequence += count;
        Ok(())
    }

    fn make_room_for_write(&mut self) -> Result<()> {
        let usage = self
            .shared_state
            .read()
            .unwrap()
            .mt
            .approximate_memory_usage();
        debug!("db usage {usage}");
        if usage > self.opt.write_buffer_size {
            let mut write_guard = self.shared_state.write().unwrap();
            let mut new_mt = MemTable::new();
            write_guard.vs.next_file_num += 1;
            let file_num = write_guard.vs.next_file_num;
            let wal = WriteAheadLog::new(&self.db_path.join(format!("{}.log", file_num)))?;
            self.log = Arc::new(RwLock::new(wal));
            let old_log_num = write_guard.vs.log_num;
            // ignore intentionally
            let _ = fs::remove_file(self.db_path.join(format!("{}.log", old_log_num)));
            write_guard.vs.log_num = file_num;
            debug!("new log number {}", file_num);
            std::mem::swap(&mut write_guard.mt, &mut new_mt);
            write_guard.imm = new_mt;
            drop(write_guard);
            self.maybe_do_compactiion()?;
        }
        Ok(())
    }
    fn maybe_do_compactiion(&mut self) -> Result<()> {
        if !self.shared_state.read().unwrap().imm.is_empty() {
            let mut ve = VersionEdit::new(); // imm not empyt, write to l0 level.
                                             // compact memtable
            self.write_to_l0_table(&mut ve)?;
            ve.set_log_number(self.shared_state.read().unwrap().vs.log_num); // TODO: plus one?
            ve.set_next_file(self.shared_state.read().unwrap().vs.next_file_num);
            let mut write_guard = self.shared_state.write().unwrap();
            write_guard.vs.log_and_apply(ve)?;
        }
        // compact sst files
        let mut write_guard = self.shared_state.write().unwrap();
        let mut compactor = Compactor::new(self.db_path.clone(), self.opt);
        compactor.start_compaction(&mut write_guard.vs);
        Ok(())
    }
    #[allow(unused)]
    fn manual_compact(&mut self) -> Result<()> {
        debug!("manually compact");
        self.maybe_do_compactiion()
    }
    fn write_to_l0_table(&mut self, ve: &mut VersionEdit) -> Result<()> {
        debug!("write to l0");
        let mut write_guard = self.shared_state.write().unwrap();
        let imm = std::mem::take(&mut write_guard.imm);
        let file_num = write_guard.vs.next_file_num;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.db_path.join(format!("{}.sst", file_num)))?;
        let mut builder = TableBuilder::new(&mut file, self.opt);
        for (k, v) in imm.iter() {
            builder.add(&k.clone().encode(), v)?;
        }
        builder.finish()?;
        ve.add_file(
            0,
            Rc::new(FileMetadata {
                allowed_seeks: 100, //TODO: what is this?
                num: file_num as usize,
                size: file.metadata()?.len() as usize,
                largest: imm.largest().unwrap(),
                smallest: imm.smallest().unwrap(),
            }),
        );
        write_guard.vs.next_file_num += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::cmp::MAX_SEQUENCE_NUMBER;

    use super::*;
    use log::debug;

    fn init_logger() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::max())
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();
    }

    #[test]
    fn it_works() -> Result<()> {
        init_logger();
        let db_path = "test_dir/it_works";
        // intentionally remove the log file but ignore error
        let _ = fs::remove_dir_all(db_path);
        let mut db = DB::new(
            db_path,
            Options {
                ..Options::default()
            },
        )?;
        // seq = 1
        db.put(b"key".to_vec(), b"value".to_vec())?;
        assert_eq!(db.get(b"key"), Some(b"value".to_vec()));

        debug!("open db");
        let mut db = DB::new(
            db_path,
            Options {
                ..Options::default()
            },
        )?;
        assert_eq!(db.get(b"key"), Some(b"value".to_vec()));
        // seq = 2
        db.put(b"key".to_vec(), b"value2".to_vec())?;
        // seq = 3
        db.put(b"key".to_vec(), b"value3".to_vec())?;
        assert_eq!(db.get_internal(b"key", 1), Some(b"value".to_vec()));
        assert_eq!(db.get_internal(b"key", 3), Some(b"value3".to_vec()));
        assert_eq!(db.get_internal(b"key", 4), Some(b"value3".to_vec()));
        assert_eq!(db.get(b"key"), Some(b"value3".to_vec()));

        let mut batch = Batch::new();
        batch.delete(b"key");
        batch.put(b"key2", b"value1");
        db.write(batch, true)?;
        for (k, v) in db.range_all() {
            debug!("{}: {:?}", k, v);
        }
        assert_eq!(db.get(b"key"), None);
        assert_eq!(db.get(b"key2"), Some(b"value1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_make_room() -> Result<()> {
        init_logger();
        let db_path = "./test_dir/db_test_make_room";
        let _ = fs::remove_dir_all(db_path);
        // intentionally remove the log file but ignore error
        let opt = Options {
            write_buffer_size: 1,
            ..Options::default()
        };
        let mut db = DB::new(db_path, opt)?;
        db.put(b"key2".to_vec(), b"value2".to_vec())?;
        // make room and flush (key2, value2) to sst
        db.put(b"key".to_vec(), b"value".to_vec())?;
        assert_eq!(db.get(b"key"), Some(b"value".to_vec()));
        let db = DB::new(db_path, Options::default())?;
        // make_room_for_write push the "value2" to level 0, but "value" is kept in cache.
        // 两次打开应该是一致的，这里有问题。
        assert_eq!(db.get(b"key"), Some(b"value".to_vec()));
        // assert_eq!(db.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(
            db.shared_state.read().unwrap().mt.get(&InternalKey::new(
                b"key2",
                MAX_SEQUENCE_NUMBER,
                InternalKeyKind::Value
            )),
            None
        );
        fs::remove_dir_all(db_path)?;
        Ok(())
    }

    #[test]
    fn test_compaction() -> Result<()> {
        init_logger();
        let db_path = "./test_dir/test_compaction";
        let _ = fs::remove_dir_all(db_path);
        // intentionally remove the log file but ignore error
        let opt = Options {
            write_buffer_size: 10,
            level0_file_num: 5,
            base_level_size: 10,
            ..Options::default()
        };
        let mut db = DB::new(db_path, opt)?;
        // size = 10
        db.put(b"key1".to_vec(), b"value1".to_vec())?;
        // make room and flush (key1, value1) to sst
        db.put(b"key2".to_vec(), b"value2".to_vec())?;
        // 0.log removed
        assert!(Path::new(db_path).join("1.sst").exists());
        // size = 20
        // make room and flush (key2, value2) to sst
        db.put(b"key3".to_vec(), b"value3".to_vec())?;
        // 2.log removed
        assert!(Path::new(db_path).join("3.sst").exists());
        // size = 30
        // make room and flush (key3, value3) to sst
        db.put(b"key4".to_vec(), b"value4".to_vec())?;
        // 4.log removed
        assert!(Path::new(db_path).join("5.sst").exists());
        // size = 40
        // make room and flush (key4, value4) to sst
        db.put(b"key5".to_vec(), b"value5".to_vec())?;
        // 6.log removed
        assert!(Path::new(db_path).join("7.sst").exists());
        // size = 50
        // make room and flush (key5, value5) to sst
        // compact 1.sst to level2
        db.put(b"key6".to_vec(), b"value6".to_vec())?;
        // 8.log removed
        assert!(Path::new(db_path).join("9.sst").exists());
        // size = 60
        // make room and flush (key6, value6) to sst
        // compact 3.sst to level2
        db.put(b"key7".to_vec(), b"value7".to_vec())?;
        // 10.log removed
        assert!(Path::new(db_path).join("11.sst").exists());

        // level 0 score = 1.0, level 1 score = 0.3
        db.put(b"key8".to_vec(), b"value8".to_vec())?;
        // level 0 score = 1.0, level 1 score = 0.4
        db.put(b"key9".to_vec(), b"value9".to_vec())?;
        // level 0 score = 1.0, level 1 score = 0.5
        db.put(b"key10".to_vec(), b"value10".to_vec())?;
        // level 0 score = 1.0, level 1 score = 0.6
        db.put(b"key11".to_vec(), b"value11".to_vec())?;
        // level 0 score = 1.0, level 1 score = 0.7
        db.put(b"key12".to_vec(), b"value12".to_vec())?;
        // level 0 score = 1.0, level 1 score = 0.8
        db.put(b"key13".to_vec(), b"value13".to_vec())?;
        // level 0 score = 1.0, level 1 score = 0.9
        db.put(b"key14".to_vec(), b"value14".to_vec())?;
        // level 0 score = 1.0, level 1 score = 1.0
        db.put(b"key15".to_vec(), b"value15".to_vec())?;
        db.manual_compact()?;
        db.manual_compact()?;
        db.manual_compact()?;
        db.manual_compact()?;
        Ok(())
    }
}
