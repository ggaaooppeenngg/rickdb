use super::record::{RecordReader, RecordWriter};
use anyhow::{Context, Result};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

// WAL 的实现
pub struct WriteAheadLog {
    // 日志文件路径
    log_path: PathBuf,
    // 日志文件写入器
    writer: RecordWriter<File>,
}

impl WriteAheadLog {
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let writer = RecordWriter::new(file);
        Ok(Self {
            writer,
            log_path: path.to_path_buf(),
        })
    }
    pub fn append_record(&mut self, data: &[u8]) -> Result<()> {
        // concat head key and value into a record and append to writer
        let mut record = Vec::new();
        record.extend_from_slice(data);
        Ok(self.writer.write_record(&record)?)
    }
    // 如果要同步写入要掉用flush。
    pub fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }

    pub fn read_records(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.log_path)
            .with_context(|| format!("failed to open file {}", self.log_path.display()))?;
        file.seek(SeekFrom::Start(0))?;
        let mut reader = RecordReader::new(&file);
        let mut records = Vec::new();
        while let Ok(record) = reader.read_record() {
            records.push(record);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::super::batch::Batch;

    use super::*;
    use std::fs;

    #[test]
    fn test_wal() -> Result<()> {
        let wal_path = "test_wal.log";
        let _ = fs::remove_file(wal_path);
        let mut wal = WriteAheadLog::new(Path::new(wal_path))?;
        let key = b"key";
        let value = b"value";
        let mut batch = Batch::new();
        batch.put(key, value);
        wal.append_record(&batch.encode(1))?;
        wal.flush()?;
        fs::remove_file(wal_path)?;

        Ok(())
    }
}
