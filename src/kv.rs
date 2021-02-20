use failure::Fail;
use io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::{collections::HashMap, io, path::Path, usize};
use std::{
    fs,
    fs::{File, OpenOptions},
    path::PathBuf,
    result,
};

const THRESHOLD: u64 = 1024;

#[derive(Debug, Fail)]
pub enum KvStoreError {
    #[fail(display = "Open kv file error: {}", _0)]
    IoError(io::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "(De)serialization error: {}", _0)]
    SerDeError(serde_json::Error),
}

impl From<io::Error> for KvStoreError {
    fn from(e: io::Error) -> Self {
        KvStoreError::IoError(e)
    }
}

impl From<serde_json::Error> for KvStoreError {
    fn from(e: serde_json::Error) -> Self {
        KvStoreError::SerDeError(e)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum CommandType {
    Set,
    Remove,
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

pub type Result<T> = result::Result<T, KvStoreError>;

pub struct KvStore {
    path: PathBuf,
    index_map: HashMap<String, CommandPos>,
    log_id: u64,
    uncompacted: u64,
    readers: HashMap<u64, Reader<File>>,
    writer: Writer<File>,
}

#[derive(Debug)]
struct CommandPos {
    log_id: u64,
    pos: u64,
    len: u64,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let mut readers = HashMap::new();
        let mut index_map = HashMap::new();
        let log_list = get_log_list(&path)?;
        let mut uncompacted = 0u64;
        for &log_id in &log_list {
            let mut reader = Reader::new(File::open(get_log_path(&path, log_id))?)?;
            uncompacted += load_log(log_id, &mut reader, &mut index_map)?;
            readers.insert(log_id, reader);
        }
        let log_id = *log_list.last().unwrap_or(&0);
        let writer = new_log(&path, log_id, &mut readers)?;

        Ok(KvStore {
            path,
            readers,
            index_map,
            uncompacted,
            writer,
            log_id,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.writer.pos;
        serde_json::to_writer(
            &mut self.writer,
            &Command::Set {
                key: key.clone(),
                value,
            },
        )?;
        self.writer.flush()?;
        if let Some(deprecated) = self.index_map.insert(
            key,
            CommandPos {
                log_id: self.log_id,
                pos,
                len: self.writer.pos - pos,
            },
        ) {
            self.uncompacted += deprecated.len;
        }

        if self.uncompacted > THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index_map.get(&key) {
            Some(cmd_pos) => {
                if let Some(reader) = self.readers.get_mut(&cmd_pos.log_id) {
                    reader.seek(SeekFrom::Start(cmd_pos.pos))?;
                    let r = reader.take(cmd_pos.len);
                    if let Command::Set { key: _, value } = serde_json::from_reader(r)? {
                        Ok(Some(value))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index_map.contains_key(&key) {
            Err(KvStoreError::KeyNotFound)
        } else {
            serde_json::to_writer(&mut self.writer, &Command::Remove { key: key.clone() })?;
            self.writer.flush()?;
            if let Some(depracted) = self.index_map.remove(&key) {
                self.uncompacted += depracted.len;
            }
            Ok(())
        }
    }

    fn compact(&mut self) -> Result<()> {
        let new_log_id = self.log_id + 1;
        self.log_id += 2;
        self.writer = new_log(&self.path, self.log_id, &mut self.readers)?;
        let mut compaction_writer = new_log(&self.path, new_log_id, &mut self.readers)?;
        let mut cur = 0u64;
        for cmd_pos in &mut self.index_map.values_mut() {
            if let Some(reader) = self.readers.get_mut(&cmd_pos.log_id) {
                if reader.pos != cmd_pos.pos {
                    reader.seek(SeekFrom::Start(cmd_pos.pos))?;
                }
                let mut kv_reader = reader.take(cmd_pos.len);
                let len = io::copy(&mut kv_reader, &mut compaction_writer)?;
                *cmd_pos = CommandPos {
                    log_id: new_log_id,
                    pos: cur,
                    len,
                };
                cur += len;
            }
        }
        compaction_writer.flush()?;
        let depreacted_logs: Vec<u64> = self
            .readers
            .keys()
            .filter(|log| **log < new_log_id)
            .cloned()
            .collect();
        for log in depreacted_logs {
            self.readers.remove(&log);
            fs::remove_file(get_log_path(&self.path, log))?;
        }
        self.uncompacted = 0;
        Ok(())
    }
}

fn get_log_list(path: &Path) -> Result<Vec<u64>> {
    let mut list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|r| -> Result<_> { Ok(r?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    list.sort_unstable();
    Ok(list)
}

fn new_log(
    path: &Path,
    log_id: u64,
    readers: &mut HashMap<u64, Reader<File>>,
) -> Result<Writer<File>> {
    let path = get_log_path(path, log_id);
    let writer = Writer::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(log_id, Reader::new(File::open(&path)?)?);
    Ok(writer)
}

fn load_log(
    log_id: u64,
    reader: &mut Reader<File>,
    index_map: &mut HashMap<String, CommandPos>,
) -> Result<u64> {
    let mut uncompacted = 0;
    let mut cur = reader.seek(SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let tail = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(depreacted) = index_map.insert(
                    key,
                    CommandPos {
                        log_id,
                        pos: cur,
                        len: tail - cur,
                    },
                ) {
                    uncompacted += depreacted.len;
                }
            }
            Command::Remove { key } => {
                if let Some(deprecated) = index_map.remove(&key) {
                    uncompacted += deprecated.len;
                }
                uncompacted += tail - cur;
            }
        }
        cur = tail;
    }
    Ok(uncompacted)
}

fn get_log_path(path: &Path, log_id: u64) -> PathBuf {
    path.join(format!("{}.log", log_id))
}

struct Reader<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> Reader<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Reader {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for Reader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct Writer<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> Writer<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(Writer {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for Writer<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
