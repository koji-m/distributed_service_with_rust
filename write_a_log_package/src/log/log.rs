use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Mutex;
use crate::api::v1::log::{Record};
use crate::log::config::Config;
use crate::log::segment::{Segment, new_segment};

pub struct LogInner<'a> {
    dir: String,
    config: &'a Config,
    pub active_segment: Rc<RefCell<Segment<'a>>>,
    pub segments: Vec<Rc<RefCell<Segment<'a>>>>,
}

type Log<'a> = Mutex<LogInner<'a>>;

pub fn new_log<'a>(dir: String, config: &'a mut Config) -> Log<'a> {
    if config.segment.max_store_bytes == 0 {
       config.segment.max_store_bytes = 1024;
    }
    if config.segment.max_index_bytes == 0 {
        config.segment.max_index_bytes = 1024;
    }

    let (segments, active_segment) = create_segments(&dir, config).unwrap();

    Mutex::new(LogInner{
        dir,
        config,
        active_segment,
        segments,
    })
}

pub fn get_base_offsets(dir: &String) -> std::io::Result<Option<Vec<u64>>> {
    let mut base_offsets = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            ()
        } else {
            let stem = path.file_stem().unwrap().to_str().unwrap();
            base_offsets.push(u64::from_str_radix(stem, 10).unwrap());
        }
    }
    if base_offsets.len() == 0 {
        Ok(None)
    } else {
        base_offsets.sort();
        Ok(Some(base_offsets))
    }
}

pub fn create_segments<'a>(dir: &String, config: &'a Config) -> Result<(Vec<Rc<RefCell<Segment<'a>>>>, Rc<RefCell<Segment<'a>>>), String> {
    let mut segments = Vec::new();
    if let Some(base_offsets) = get_base_offsets(&dir).unwrap() {
        for base_offset in base_offsets {
            let seg = Rc::new(RefCell::new(new_segment(&dir, base_offset, config).unwrap()));
            segments.push(seg.clone());
        }
    } else {
        let seg = Rc::new(RefCell::new(new_segment(&dir, config.segment.initial_offset, config).unwrap()));
        segments.push(seg.clone());
    }

    let active_segment = segments.last().unwrap().clone();
    Ok((segments, active_segment))
}

impl <'a> LogInner<'a> {
    pub fn new_segment(&mut self, off: u64) -> Result<(), String> {
        let s = Rc::new(RefCell::new(new_segment(&self.dir, off, &self.config).unwrap()));
        self.segments.push(s.clone());
        self.active_segment = s;
        Ok(())
    }
}

trait LogExt {
    fn append(&mut self, record: Record) -> Result<u64, String>;
    fn read(&mut self, off: u64) -> Result<Record, String>;
    fn close(&mut self);
    fn remove(&mut self);
    fn reset(&mut self);
    fn lowest_offset(&self) -> Result<u64, String>;
    fn highest_offset(&self) -> Result<u64, String>;
    fn truncate(&mut self, lowest: u64);
}

impl <'a> LogExt for Log<'a> {
    fn append(&mut self, record: Record) -> Result<u64, String> {
        let mut l = self.lock().unwrap();
        let maxed;
        let off;
        {
            let mut s = l.active_segment.borrow_mut();
            off = s.append(record).unwrap();
            maxed = s.is_maxed();
        }

        if maxed {
            l.new_segment(off + 1).unwrap();
        }

        Ok(off)
    }

    fn read(&mut self, off: u64) -> Result<Record, String> {
        let rec;
        {
            let l = self.lock().unwrap();
            let segs = &l.segments;
            let mut segs_itr = segs.iter();
            let seg = &segs_itr.find(|&target| {
                let s = target.borrow_mut();
                s.base_offset <= off && s.next_offset < off
            }).unwrap();
            rec = seg.borrow_mut().read(off).unwrap();
        }
        Ok(rec)
    }

    fn close(&mut self) {
        let l = self.lock().unwrap();
        l.segments.iter().for_each(|target| {
            target.borrow_mut().close();
        })
    }

    fn remove(&mut self) {
        self.close();
        let l = self.lock().unwrap();
        std::fs::remove_dir_all(&l.dir).unwrap();
    }

    fn reset(&mut self) {
        self.remove();
        let mut l = self.lock().unwrap();
        let (segments, active_segment) = create_segments(&l.dir, &l.config).unwrap();
        l.segments = segments;
        l.active_segment = active_segment;
    }

    fn lowest_offset(&self) -> Result<u64, String> {
        let l = self.lock().unwrap();
        let off = l.segments[0].borrow().base_offset;
        Ok(off)
    }

    fn highest_offset(&self) -> Result<u64, String> {
        let l = self.lock().unwrap();
        let off = l.segments.last().unwrap().borrow().next_offset;
        Ok(off - 1)
    }

    fn truncate(&mut self, lowest: u64) {
        let mut l = self.lock().unwrap();
        let mut segments = Vec::new();
        l.segments.iter().for_each(|target| {
            let mut s = target.borrow_mut();
            if s.next_offset <= lowest + 1 {
                s.remove();
            } else {
                segments.push(target.clone());
            }
        });
        l.segments = segments;
    }
}