

use super::cmp::{UserKeyComparator, Comparator};

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub struct Options {
    pub block_restart_interval: usize,
    pub block_size: usize,
    pub max_level: usize,
    pub cmp: Comparator,
    pub write_buffer_size: usize,


    pub base_level_size: usize,
    pub level_size_multiplier: usize,

    pub level0_file_num: usize,
    pub max_file_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            block_restart_interval: 32,
            block_size: 4096,
            max_level: 7,
            cmp: UserKeyComparator,
            write_buffer_size: 4 * 1024 * 1024, //4MB
            base_level_size: 10 * 1024 * 1024, //10MB
            level_size_multiplier: 10,         // 10
            level0_file_num: 5,
            max_file_size: 2 * 1024 * 1024, //2MB
        }
    }
}
