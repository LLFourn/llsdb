use anyhow::Result;
use std::io;
use std::{
    borrow::BorrowMut,
    io::{Read, Seek, Write},
};

pub trait Backend: Read + Write + Seek {
    fn truncate(&mut self, size: u64) -> Result<()>;
    fn init_max_size(&self) -> u64;
    fn init_page_size(&self) -> u16;
    fn sync_data(&self) -> Result<()>;
}

/// this is for tests
impl<'a, V: BorrowMut<Vec<u8>>> Backend for io::Cursor<V>
where
    io::Cursor<V>: Read + Write + Seek,
{
    fn truncate(&mut self, len: u64) -> Result<()> {
        self.get_mut().borrow_mut().truncate(len as usize);
        Ok(())
    }

    fn init_max_size(&self) -> u64 {
        u64::MAX
    }

    fn init_page_size(&self) -> u16 {
        // smaller numbers make things easier to debug
        128
    }

    fn sync_data(&self) -> Result<()> {
        Ok(())
    }
}

impl Backend for std::fs::File {
    fn truncate(&mut self, size: u64) -> Result<()> {
        self.set_len(size)?;
        Ok(())
    }

    fn init_max_size(&self) -> u64 {
        u64::MAX
    }

    fn init_page_size(&self) -> u16 {
        4096
    }

    fn sync_data(&self) -> Result<()> {
        Ok(std::fs::File::sync_data(self)?)
    }
}
