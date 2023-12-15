#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, bincode::Encode, bincode::Decode,
)]
pub struct Pointer(pub(crate) u64);

impl Pointer {
    pub const NULL: Self = Self(0u64);
    pub const MAX: Self = Self(u64::MAX);
    pub const MIN: Self = Self(1u64);

    pub fn encoded_len(&self) -> u64 {
        if self.0 <= 250 {
            1
        } else if self.0 <= u16::MAX as u64 {
            3
        } else if self.0 <= u32::MAX as u64 {
            4
        } else {
            5
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct EntryPointer {
    pub this_entry: Pointer,
    pub next_entry_possibly_stale: Pointer,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EntryHandle {
    pub(crate) entry_pointer: EntryPointer,
    pub(crate) value_len: u64,
}

impl EntryHandle {
    pub fn entry_len(&self) -> u64 {
        self.entry_pointer.next_entry_possibly_stale.encoded_len() + self.value_len
    }

    pub fn value_pointer(&self) -> Pointer {
        self.entry_pointer.value_pointer()
    }

    pub fn pointer_to_end(&self) -> Pointer {
        Pointer(self.entry_pointer.this_entry.0 + self.entry_len())
    }
}

impl EntryPointer {
    pub fn value_pointer(&self) -> Pointer {
        Pointer(self.this_entry.0 + self.next_entry_possibly_stale.encoded_len())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, bincode::Encode, bincode::Decode)]
pub struct Remap {
    pub from: Pointer,
    pub to: Pointer,
}
