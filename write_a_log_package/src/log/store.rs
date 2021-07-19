use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

const LEN_WIDTH: u64 = 8;

pub struct StoreInner {
    file: File,
    buf: BufWriter<File>,
    path: Box<Path>,
    pub size: u64,
}

pub type Store = Mutex<StoreInner>;

pub fn new_store(path: Box<Path>) -> std::io::Result<Store> {
    let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
    let read_instance = f.try_clone().unwrap();
    let meta = f.metadata()?;
    let buf = BufWriter::new(f);

    Ok(Mutex::new(StoreInner{
        file: read_instance,
        buf: buf,
        path: path,
        size: meta.len(),
    }))
}

pub trait StoreExt {
    fn append(&mut self, p: Vec<u8>) -> std::io::Result<(u64, u64)>;
    fn read(&self, pos: u64) -> std::io::Result<Vec<u8>>;
    fn read_at(&self, buf: &mut Vec<u8>, offset: u64);
    fn close(&mut self);
    fn name(&self) -> String;
}

impl StoreExt for Store {
    fn append(&mut self, p: Vec<u8>) -> std::io::Result<(u64, u64)> {
        let mut st = self.lock().unwrap();
        let pos = st.size;

        st.buf.write(&p.len().to_be_bytes())?;
        let w = st.buf.write(&p)?;
        let written = u64::try_from(w).unwrap() + LEN_WIDTH;
        st.size += written;

        Ok((written, pos))
    }

    fn read(&self, pos: u64) -> std::io::Result<Vec<u8>> {
        let mut size_buf: Vec<u8> = vec![0; LEN_WIDTH.try_into().unwrap()];
        self.read_at(&mut size_buf, pos);
        let size_bytes: [u8; 8] = size_buf.try_into().unwrap();
        let size = usize::try_from(u64::from_be_bytes(size_bytes)).unwrap();

        let mut record_buf: Vec<u8> = Vec::with_capacity(size);
        record_buf.resize(size, 0);
        self.read_at(&mut record_buf, pos + LEN_WIDTH);

        Ok(record_buf)
    }

    fn read_at(&self, buf: &mut Vec<u8>, offset: u64) {
        let mut st = self.lock().unwrap();
        st.buf.flush().unwrap();

        st.file.seek(SeekFrom::Start(offset)).unwrap();
        st.file.read_exact(buf).unwrap();
    }

    fn close(&mut self) {
        let mut s = self.lock().unwrap();
        s.buf.flush().unwrap();
        s.file.sync_all().unwrap();
    }

    fn name(&self) -> String {
        let l = self.lock().unwrap();
        String::from_str(l.path.to_str().unwrap()).unwrap()
    }
}