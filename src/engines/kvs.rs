use crate::{KvsEngine, KvsError, Result};
use crossbeam_skiplist::SkipMap;
use io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use serde::{Deserialize, Serialize};
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc, Mutex};
use std::{cell::RefCell, ffi::OsStr};
use std::{collections::HashMap, io, path::Path, usize};
use std::{
    fs,
    fs::{File, OpenOptions},
    path::PathBuf,
};

const THRESHOLD: u64 = 1024;

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

#[derive(Clone)]
pub struct KvStore {
    index_map: Arc<SkipMap<String, CommandPos>>,
    reader: KvStoreReader,
    writer: Arc<Mutex<KvStoreWriter>>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;
        let mut readers = HashMap::new();
        let index_map = Arc::new(SkipMap::new());
        let log_list = get_log_list(&path)?;
        let mut uncompacted = 0u64;
        for &log_id in &log_list {
            let mut reader = Reader::new(File::open(get_log_path(&path, log_id))?)?;
            uncompacted += load_log(log_id, &mut reader, &index_map)?;
            readers.insert(log_id, reader);
        }
        let log_id = *log_list.last().unwrap_or(&0);
        let writer = new_log(&path, log_id)?;
        let reader = KvStoreReader {
            path: Arc::clone(&path),
            latest_compacted_log_id: Arc::new(AtomicU64::new(0)),
            readers: RefCell::new(readers),
        };
        let writer = Arc::new(Mutex::new(KvStoreWriter {
            reader: reader.clone(),
            writer,
            log_id,
            uncompacted,
            path: Arc::clone(&path),
            index_map: Arc::clone(&index_map),
        }));

        Ok(KvStore {
            reader,
            index_map,
            writer,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CommandPos {
    log_id: u64,
    pos: u64,
    len: u64,
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index_map.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(KvsError::WrongCommandError)
            }
        } else {
            Ok(None)
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
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

fn new_log(path: &Path, log_id: u64) -> Result<Writer<File>> {
    let path = get_log_path(path, log_id);
    let writer = Writer::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(&path)?,
    )?;
    Ok(writer)
}

fn load_log(
    log_id: u64,
    reader: &mut Reader<File>,
    index_map: &SkipMap<String, CommandPos>,
) -> Result<u64> {
    let mut uncompacted = 0;
    let mut cur = reader.seek(SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let tail = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(depreacted) = index_map.get(&key) {
                    uncompacted += depreacted.value().len;
                }
                index_map.insert(
                    key,
                    CommandPos {
                        log_id,
                        pos: cur,
                        len: tail - cur,
                    },
                );
            }
            Command::Remove { key } => {
                if let Some(deprecated) = index_map.remove(&key) {
                    uncompacted += deprecated.value().len;
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

struct KvStoreReader {
    path: Arc<PathBuf>,
    latest_compacted_log_id: Arc<AtomicU64>,
    readers: RefCell<HashMap<u64, Reader<File>>>,
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        KvStoreReader {
            path: Arc::clone(&self.path),
            latest_compacted_log_id: Arc::clone(&self.latest_compacted_log_id),
            readers: RefCell::new(HashMap::new()),
        }
    }
}

impl KvStoreReader {
    fn close_depracted_logs(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let log_id = *readers.keys().next().unwrap();
            if self.latest_compacted_log_id.load(Ordering::SeqCst) <= log_id {
                break;
            }
            readers.remove(&log_id);
        }
    }

    fn read_and<F, R>(&self, cmd_pos: CommandPos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut Reader<File>>) -> Result<R>,
    {
        self.close_depracted_logs();
        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&cmd_pos.log_id) {
            let reader = Reader::new(File::open(get_log_path(&self.path, cmd_pos.log_id))?)?;
            readers.insert(cmd_pos.log_id, reader);
        }
        let reader = readers.get_mut(&cmd_pos.log_id).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let reader = reader.take(cmd_pos.len);
        f(reader)
    }

    fn read_command(&self, cmd_pos: CommandPos) -> Result<Command> {
        self.read_and(cmd_pos, |reader| Ok(serde_json::from_reader(reader)?))
    }
}

struct KvStoreWriter {
    reader: KvStoreReader,
    writer: Writer<File>,
    log_id: u64,
    uncompacted: u64,
    path: Arc<PathBuf>,
    index_map: Arc<SkipMap<String, CommandPos>>,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.writer.pos;
        serde_json::to_writer(
            &mut self.writer,
            &Command::Set {
                key: key.clone(),
                value,
            },
        )?;
        self.writer.flush()?;
        if let Some(deprecated) = self.index_map.get(&key) {
            self.uncompacted += deprecated.value().len;
        }
        self.index_map.insert(
            key,
            CommandPos {
                log_id: self.log_id,
                pos,
                len: self.writer.pos - pos,
            },
        );

        if self.uncompacted > THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if !self.index_map.contains_key(&key) {
            Err(KvsError::KeyNotFound)
        } else {
            let pos = self.writer.pos;
            serde_json::to_writer(&mut self.writer, &Command::Remove { key: key.clone() })?;
            self.writer.flush()?;
            if let Some(depracted) = self.index_map.remove(&key) {
                self.uncompacted += depracted.value().len;
                self.uncompacted += self.writer.pos - pos;
            }
            if self.uncompacted > THRESHOLD {
                self.compact()?;
            }
            Ok(())
        }
    }

    fn compact(&mut self) -> Result<()> {
        let new_log_id = self.log_id + 1;
        self.log_id += 2;
        self.writer = new_log(&self.path, self.log_id)?;
        let mut compaction_writer = new_log(&self.path, new_log_id)?;
        let mut cur = 0u64;
        for entry in &mut self.index_map.iter() {
            let len = self.reader.read_and(*entry.value(), |mut reader| {
                Ok(io::copy(&mut reader, &mut compaction_writer)?)
            })?;
            self.index_map.insert(
                entry.key().clone(),
                CommandPos {
                    log_id: new_log_id,
                    pos: cur,
                    len,
                },
            );
            cur += len;
        }
        compaction_writer.flush()?;
        self.reader
            .latest_compacted_log_id
            .store(new_log_id, Ordering::SeqCst);
        self.reader.close_depracted_logs();
        let depreacted_logs = get_log_list(&self.path)?
            .into_iter()
            .filter(|&log_id| log_id < new_log_id);
        for log in depreacted_logs {
            fs::remove_file(get_log_path(&self.path, log))?;
        }
        self.uncompacted = 0;
        Ok(())
    }
}
