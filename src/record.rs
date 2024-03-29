use std::io::{self, BufWriter, Read, Result, Write};
const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 4 + 2 + 1; // checksum + length + type

#[derive(Clone, Copy)]
pub enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

pub struct RecordWriter<W: Write> {
    writer: BufWriter<W>,
    block_offset: usize,
    block_size: usize,

    padding_helper: [u8; 7],
}

impl<W: Write> RecordWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::with_capacity(512, writer),
            block_offset: 0,
            block_size: BLOCK_SIZE,
            padding_helper: [0; 7],
        }
    }
    pub fn write_record(&mut self, data: &[u8]) -> Result<()> {
        let mut first_fragment = true;
        let mut data = data;
        assert!(self.block_offset <= self.block_size);
        while !data.is_empty() {
            let space_left = self.block_size - self.block_offset;
            if space_left < HEADER_SIZE {
                if space_left != 0 {
                    self.writer.write_all(&self.padding_helper[..space_left])?;
                }
                self.block_offset = 0;
            }
            let available = self.block_size - self.block_offset - HEADER_SIZE;
            let fragment_length = std::cmp::min(data.len(), available);
            let record_type = if first_fragment {
                if fragment_length == data.len() {
                    RecordType::Full
                } else {
                    RecordType::First
                }
            } else if fragment_length == data.len() {
                RecordType::Last
            } else {
                RecordType::Middle
            };
            let checksum = crc32fast::hash(&data[..fragment_length]);
            self.writer.write_all(&checksum.to_le_bytes())?;
            self.writer
                .write_all(&(fragment_length as u16).to_le_bytes())?;
            self.writer.write_all(&[record_type as u8])?;
            self.writer.write_all(&data[..fragment_length])?;
            self.block_offset += HEADER_SIZE + fragment_length;
            data = &data[fragment_length..];
            first_fragment = false;
        }
        Ok(())
    }
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}
pub struct RecordReader<R: Read> {
    reader: R,
    block_offset: usize,
    block_size: usize,
    head_buf: [u8; 7],
    record_buf: Vec<u8>,
}

impl<R: Read> RecordReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            block_offset: 0,
            block_size: BLOCK_SIZE,
            head_buf: [0; 7],
            record_buf: Vec::with_capacity(512),
        }
    }

    #[allow(unused)]
    pub fn records(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut records = vec![];
        loop {
            match self.read_record() {
                Ok(record) => records.push(record),
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        Ok(records)
    }
    pub fn read_record(&mut self) -> Result<Vec<u8>> {
        self.record_buf.clear();
        let mut record_off = 0;
        assert!(self.block_offset <= self.block_size);
        loop {
            if self.block_size - self.block_offset < HEADER_SIZE {
                // skip padding
                self.reader
                    .read_exact(&mut self.head_buf[..self.block_size - self.block_offset])?;
                // begin a new block
                self.block_offset = 0;
            }
            self.reader.read_exact(&mut self.head_buf)?;
            // header
            let checksum = u32::from_le_bytes(self.head_buf[..4].try_into().unwrap());
            let length = u16::from_le_bytes(self.head_buf[4..6].try_into().unwrap());
            let record_type = self.head_buf[6];
            self.block_offset += HEADER_SIZE;
            // data
            self.record_buf.resize(record_off + length as usize, 0);
            self.reader.read_exact(&mut self.record_buf[record_off..])?;
            self.block_offset += length as usize;
            if checksum != crc32fast::hash(&self.record_buf[record_off..]) {
                return Err(io::Error::new(io::ErrorKind::Other, "checksum error"));
            }

            record_off += length as usize;

            if record_type == RecordType::Full as u8 || record_type == RecordType::Last as u8 {
                return Ok(self.record_buf.clone());
            } else {
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_record() {
        let mut writer = RecordWriter::new(Vec::new());
        let data1 = [1u8; 10];
        let data2 = [2u8; 10];
        // 故意制造跨block的情况
        let data3 = [3u8; BLOCK_SIZE - HEADER_SIZE + 1];
        let data4 = [4u8; 2 * BLOCK_SIZE - 2 * HEADER_SIZE + 1];
        writer.write_record(&data1).unwrap();
        writer.write_record(&data2).unwrap();
        writer.write_record(&data3).unwrap();
        writer.write_record(&data4).unwrap();
        let bytes = writer.writer.into_inner().unwrap();
        let mut reader = RecordReader::new(&bytes[..]);
        let read_record1 = reader.read_record().unwrap();
        let read_record2 = reader.read_record().unwrap();
        let read_record3 = reader.read_record().unwrap();
        let read_record4 = reader.read_record().unwrap();

        assert_eq!(read_record1, [1; 10]);
        assert_eq!(read_record2, [2; 10]);
        assert_eq!(read_record3, [3; BLOCK_SIZE - HEADER_SIZE + 1]);
        assert_eq!(read_record4, [4; 2 * BLOCK_SIZE - 2 * HEADER_SIZE + 1]);
    }
}
