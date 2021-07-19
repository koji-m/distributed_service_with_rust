pub struct Segment {
    pub max_store_bytes: u64,
    pub max_index_bytes: u64,
    pub initial_offset: u64,
}

pub struct Config {
    pub segment: Segment,
}
