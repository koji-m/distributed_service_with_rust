use std::convert::TryInto;
use std::path::Path;
use ::protobuf::Message;
use crate::log::config::Config;
use crate::log::index::{Index, new_index};
use crate::log::store::{Store, StoreExt, new_store};
use crate::api::v1::log::{Record};

pub struct Segment<'a> {
    store: Store,
    index: Index,
    pub base_offset: u64,
    pub next_offset: u64,
    config: &'a Config,
}

pub fn new_segment<'a>(dir: &String, base_offset: u64, config: &'a Config) -> Result<Segment<'a>, String> {
    let mut store_path = Path::new(dir).join(base_offset.to_string());
    store_path.set_extension("store");
    let store = new_store(store_path.into_boxed_path()).unwrap();

    let mut index_path = Path::new(dir).join(base_offset.to_string());
    index_path.set_extension("index");
    let index = new_index(index_path.into_boxed_path(), &config).unwrap();

    let next_offset: u64;
    match index.read(-1) {
        Ok((off, _)) => {
            next_offset = base_offset + off as u64;
        },
        Err(_) => {
            next_offset = base_offset;
        }
    }
    Ok(Segment {
        store,
        index,
        base_offset,
        next_offset,
        config,
    })
}

impl<'a> Segment<'a> {
    pub fn append(&mut self, mut record: Record) -> Result<u64, String> {
        let cur = self.next_offset;
        record.set_offset(cur);
        let (_, pos) = self.store.append(record.write_to_bytes().unwrap()).unwrap();
        let off: u32 = (self.next_offset - self.base_offset).try_into().unwrap();
        self.index.write(off, pos).unwrap();
        self.next_offset += 1;
        Ok(cur)
    }

    pub fn read(&self, off: u64) -> Result<Record, String> {
        let (_, pos) = self.index.read((off - self.base_offset).try_into().unwrap()).unwrap();
        let p = self.store.read(pos).unwrap();
        let record = Record::parse_from_bytes(&p).unwrap();
        Ok(record)
    }

    pub fn is_maxed(&self) -> bool {
        self.store.lock().unwrap().size >= self.config.segment.max_store_bytes ||
        self.index.size >= self.config.segment.max_index_bytes.try_into().unwrap()
    }

    pub fn close(&mut self) {
        self.index.close();
        self.store.close();
    }

    pub fn remove(&mut self) {
        self.close();
        std::fs::remove_file(self.index.name()).unwrap();
        std::fs::remove_file(self.store.name()).unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::log::config;
    use crate::log::index::ENT_WIDTH;
    #[test]
    fn test_segment() {
        let mut rec = Record::new();
        rec.set_value(b"hello world".to_vec());

        let conf = config::Config {
            segment: config::Segment {
                max_store_bytes: 1024,
                max_index_bytes: (ENT_WIDTH * 3) as u64,
                initial_offset: 0,
            }
        };
        let mut s = new_segment(&".".to_string(), 16, &conf).unwrap();
        let off = s.append(rec).unwrap();
        let got = s.read(off).unwrap();

        assert_eq!(std::str::from_utf8(&got.get_value()).unwrap(), std::str::from_utf8(b"hello world").unwrap());
    }
}