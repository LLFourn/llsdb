mod freespace;
mod llsdb;
pub use llsdb::*;
mod linkedlist;
pub use linkedlist::*;
pub mod index;
mod pointer;
pub use pointer::*;

pub(crate) mod macros;

use bincode::config::{Configuration, LittleEndian, NoLimit, Varint};
const BINCODE_CONFIG: Configuration<LittleEndian, Varint, NoLimit> = bincode::config::standard();

pub type ListSlot = usize;
pub use anyhow::Result;
