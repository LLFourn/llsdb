use crate::{
    freespace::{Free, FreeSpace},
    index::{IndexStore, RefCellIndexStore},
    EntryHandle, EntryPointer, LinkedList, ListSlot, Pointer, Remap, BINCODE_CONFIG,
};
use anyhow::{anyhow, Context, Result};
use core::mem::size_of;
use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap},
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    rc::Rc,
};
const META_LIST: LinkedList<Meta> = LinkedList::new(0);
const MAGIC_BYTES: [u8; 5] = [0x26, 0xd3, 0x64, 0x62, 0x21];

pub struct LlsDb<F> {
    io: Option<Io<F>>,
    slots_by_name: HashMap<String, Meta>,
    indexers: Vec<Box<dyn RefCellIndexStore>>,
    list_refs: BTreeSet<ListSlot>,
    used_slots: BTreeSet<ListSlot>,
    free_space: Option<FreeSpace>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InitOptions {
    /// Page size of the underlying storage media
    ///
    /// default: `4096`
    page_size: u16,
    /// The maximum on disk size of the database
    ///
    /// default: `u64::MAX`
    max_size: u64,
}

impl Default for InitOptions {
    fn default() -> Self {
        Self {
            page_size: 4096,
            max_size: u64::MAX,
        }
    }
}

impl<F> LlsDb<F>
where
    F: Backend,
{
    fn new(io: Io<F>) -> Self {
        let free_space = FreeSpace::new_from_persist_state(io.free_state());
        Self {
            io: Some(io),
            used_slots: FromIterator::from_iter([META_LIST.slot()]),
            slots_by_name: Default::default(),
            free_space: Some(free_space),
            list_refs: Default::default(),
            indexers: Default::default(),
        }
    }

    pub fn load(file: F) -> Result<Self> {
        let io = Io::load(file, MAGIC_BYTES)?;
        let mut loaded = Self::new(io);
        let (used_slots, slots_by_name) = loaded.execute(|tx| {
            let mut used_slots = BTreeSet::default();
            let mut slots_by_name = HashMap::default();
            let mut it = tx.io.iter(META_LIST.slot());
            while let Some(meta) = it.next::<Meta>() {
                let meta = meta?;
                used_slots.insert(meta.slot);
                slots_by_name.insert(meta.name.clone(), meta);
            }
            Ok((used_slots, slots_by_name))
        })?;
        loaded.used_slots = used_slots;
        loaded.slots_by_name = slots_by_name;

        Ok(loaded)
    }

    pub fn init(file: F) -> Result<Self> {
        let io = Io::init(
            Preamble {
                magic_bytes: MAGIC_BYTES,
                config: VersionedConfig::zero(file.init_page_size()),
            },
            file.init_max_size(),
            file,
        )?;

        Ok(Self::new(io))
    }

    pub fn backend(&self) -> &F {
        &self
            .io
            .as_ref()
            .expect("can't call backend during a tx")
            .file
    }

    fn io(&mut self) -> &mut Io<F> {
        self.io
            .as_mut()
            .expect("attempt to take io during a transaction")
    }

    fn free_space(&mut self) -> &mut FreeSpace {
        self.free_space
            .as_mut()
            .expect("attempt to take free space during a transaction")
    }

    pub fn load_or_init(mut file: F) -> Result<Self> {
        if file.seek(SeekFrom::End(0))? == 0 {
            Self::init(file)
        } else {
            Self::load(file)
        }
    }

    pub fn into_backend(self) -> F {
        self.io.unwrap().file
    }

    pub fn get_list<T>(&mut self, list: &str) -> Result<LinkedList<T>> {
        let meta = self
            .slots_by_name
            .get(list)
            .ok_or(anyhow!("no such list '{}'", list))?;
        if !self.list_refs.insert(meta.slot) {
            return Err(anyhow!("this list has already been taken"));
        }
        Ok(LinkedList::new(meta.slot))
    }

    pub fn lists(&self) -> impl Iterator<Item = &str> {
        self.slots_by_name.keys().map(|x| x.as_str())
    }

    pub fn execute<Func, R>(&mut self, query: Func) -> Result<R>
    where
        Func: for<'a, 'tx> FnOnce(&'a mut Transaction<'tx, F>) -> Result<R>,
    {
        let starting_length = self.io().file.seek(SeekFrom::End(0))?;

        let indexers_before_tx = self.indexers.len();
        let mut tx = {
            let io = TxIo {
                inner: Rc::new(RefCell::new(TxIoInner {
                    io: Rc::new(RefCell::new(self.io.take().expect("must be there"))),
                    changed_heads: Default::default(),
                    free_space: Rc::new(RefCell::new(
                        self.free_space.take().expect("must be there"),
                    )),
                })),
                lifetime: PhantomData,
            };
            Transaction {
                io,
                slots_by_name: &self.slots_by_name,
                tx_slots_by_name: Default::default(),
                used_slots: &self.used_slots,
                tx_used_slots: Default::default(),
                indexers: &mut self.indexers,
                tx_list_refs: Default::default(),
                list_refs: &self.list_refs,
            }
        };
        let mut output = (query)(&mut tx);

        let Transaction {
            io,
            tx_list_refs: mut new_list_refs,
            tx_slots_by_name: new_slots,
            tx_used_slots: mut new_used_slots,
            ..
        } = tx;

        let TxIoInner {
            changed_heads,
            free_space,
            io,
            ..
        } = io.into_inner();

        self.io = Some(RefCell::into_inner(
            Rc::into_inner(io).expect("refs cannot still exist"),
        ));
        self.free_space = Some(RefCell::into_inner(
            Rc::into_inner(free_space).expect("refs cannot still exist"),
        ));

        if output.is_ok() {
            for (slot, head) in changed_heads {
                self.io().set_head(slot, head);
            }
            let changed_free_slots = self.free_space().apply_pending_frees();
            for free_slot in changed_free_slots {
                let free = self.free_space().persist_state()[free_slot];
                self.io().set_free(free_slot, free);
            }

            output = match self.io().write_first_page() {
                Ok(_) => output,
                Err(e) => Err(e),
            }
        }

        if output.is_err() {
            for indexer in self.indexers.drain(indexers_before_tx..) {
                for list in indexer.owned_lists() {
                    self.list_refs.remove(&list);
                }
            }

            for indexer in &mut self.indexers {
                indexer.tx_fail_rollback();
            }

            self.free_space().tx_fail_rollback();
            let _ = self.io().file.truncate(starting_length);
        } else {
            self.free_space().tx_success();
            self.list_refs.append(&mut new_list_refs);
            self.slots_by_name.extend(new_slots);
            self.used_slots.append(&mut new_used_slots);
            for indexer in &mut self.indexers {
                indexer.tx_success();
            }

            if let Some(trim_to) = self.free_space().where_to_trim() {
                let truncate_to = self
                    .io()
                    .pointer_to_file_position(trim_to)
                    .expect("always returns a non-null pointer");
                let _ = self.io().file.truncate(truncate_to);
            }
        }
        output
    }
}

#[derive(bincode::Encode, bincode::Decode)]
pub struct Preamble {
    magic_bytes: [u8; 5],
    config: VersionedConfig,
}

#[derive(bincode::Encode, bincode::Decode, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub enum VersionedConfig {
    Zero { page_size: [u8; 2] },
}

impl VersionedConfig {
    pub fn page_size(&self) -> usize {
        match self {
            VersionedConfig::Zero { page_size } => u16::from_le_bytes(*page_size).into(),
        }
    }

    pub fn zero(page_size: u16) -> Self {
        Self::Zero {
            page_size: page_size.to_le_bytes(),
        }
    }
}

pub struct Io<F> {
    page_buf: Vec<u8>,
    n_free_slots: usize,
    n_list_slots: usize,
    file: F,
}

const PREAMBLE_LEN: usize = 8;

impl<F: Backend> Io<F> {
    pub fn load(mut file: F, check_magic: [u8; 5]) -> Result<Self> {
        file.rewind()?;
        let preamble: Preamble = bincode::decode_from_std_read(&mut file, BINCODE_CONFIG)
            .context("failed to read in llsdb preamble (is this really a llsdb database?)")?;
        if preamble.magic_bytes != check_magic {
            return Err(anyhow!(
                "magic bytes didn't match, expected {:?} got {:?}",
                check_magic,
                preamble.magic_bytes
            ));
        }
        let page_size = preamble.config.page_size();
        let (n_list_slots, n_free_slots) = Self::apportion_first_page(page_size);
        let mut page_buf = vec![0u8; page_size];
        file.rewind()?;
        file.read_exact(&mut page_buf)?;

        let io = Io {
            page_buf,
            n_list_slots,
            n_free_slots,
            file,
        };

        for free_slot in 0..n_free_slots {
            // check the free slots aren't totally cactus
            io.get_free_slot(free_slot)
                .context("reading free slots from disk")?;
        }

        Ok(io)
    }

    pub fn init(preamble: Preamble, max_size: u64, file: F) -> Result<Self> {
        let page_size = preamble.config.page_size();
        let mut page_buf = vec![0u8; page_size];
        let preamble_len = bincode::encode_into_slice(preamble, &mut page_buf[..], BINCODE_CONFIG)
            .context("Unable to write llsdb preamble")?;
        assert_eq!(preamble_len, PREAMBLE_LEN);

        let (n_list_slots, n_free_slots) = Self::apportion_first_page(page_size as usize);

        let remaining_free_space = max_size
            .checked_sub(page_size as u64)
            .expect("page size is larger than max size");
        let mut init = Io {
            page_buf,
            n_list_slots,
            n_free_slots,
            file,
        };

        let initial_free_space = Free::from_start_pointer(Pointer::MIN, remaining_free_space);
        init.set_free(0, initial_free_space);
        init.write_first_page()?;

        Ok(init)
    }

    fn apportion_first_page(page_size: usize) -> (usize, usize) {
        let space_left = page_size - PREAMBLE_LEN;
        let n_free_slots = space_left / (2 * size_of::<Free>());
        let rounded_free_slot_space = n_free_slots * size_of::<Free>();
        let list_slot_space = space_left - rounded_free_slot_space;
        let n_list_slots = list_slot_space / size_of::<Pointer>();
        assert!(
            n_free_slots > 0 && n_list_slots > 1,
            "page size not big enough to support adding entries!"
        );
        (n_list_slots, n_free_slots)
    }

    pub(crate) fn get_head(&mut self, list_slot: ListSlot) -> Pointer {
        let start = list_slot * size_of::<u64>();
        let end = start + size_of::<u64>();
        let mut slot = [0u8; size_of::<u64>()];
        slot.copy_from_slice(&self.list_slots_buf()[start..end]);
        Pointer(u64::from_le_bytes(slot))
    }

    fn set_head(&mut self, list_slot: ListSlot, head: Pointer) {
        let list_slots_buf = self.list_slots_buf_mut();
        let start = list_slot * size_of::<u64>();
        let end = start + size_of::<u64>();
        list_slots_buf[start..end].copy_from_slice(head.0.to_le_bytes().as_slice());
    }

    fn write_first_page(&mut self) -> Result<()> {
        self.file.rewind()?;
        self.file.write_all(&self.page_buf)?;
        Ok(())
    }

    fn list_slots_buf_mut(&mut self) -> &mut [u8] {
        let start = PREAMBLE_LEN;
        let end = start + self.n_list_slots * size_of::<Pointer>();
        &mut self.page_buf[start..end]
    }

    fn list_slots_buf(&self) -> &[u8] {
        let start = PREAMBLE_LEN;
        let end = start + self.n_list_slots * size_of::<Pointer>();
        &self.page_buf[start..end]
    }

    fn free_slots_buf_mut(&mut self) -> &mut [u8] {
        let start = PREAMBLE_LEN + self.n_list_slots * size_of::<Pointer>();
        let end = start + self.n_free_slots * size_of::<Free>();
        &mut self.page_buf[start..end]
    }

    fn free_slots_buf(&self) -> &[u8] {
        let start = PREAMBLE_LEN + self.n_list_slots * size_of::<Pointer>();
        let end = start + self.n_free_slots * size_of::<Free>();
        &self.page_buf[start..end]
    }

    fn free_state(&self) -> Vec<Free> {
        let mut ret = Vec::with_capacity(self.n_free_slots);
        for free_slot in 0..self.n_free_slots {
            let free = self
                .get_free_slot(free_slot)
                .expect("should have been checked at construction");
            ret.push(free);
        }
        ret
    }

    fn get_free_slot(&self, slot: usize) -> Result<Free> {
        let start = slot * size_of::<Free>();
        let end = start + size_of::<Free>();
        let free_slots_buf = self.free_slots_buf();
        let free = Free::read_from(&free_slots_buf[start..end])
            .ok_or(anyhow!("Free slot {} has an invalid value in it", slot))?;
        Ok(free)
    }

    fn set_free(&mut self, slot: usize, free: Free) {
        let free_slots_buf = self.free_slots_buf_mut();
        let start = slot * size_of::<Free>();
        let end = start + size_of::<Free>();
        free.write_to(&mut free_slots_buf[start..end]);
    }

    fn file_position_to_pointer(&self, file_pos: u64) -> Pointer {
        Pointer(file_pos - self.page_buf.len() as u64 + 1)
    }

    fn pointer_to_file_position(&self, pointer: Pointer) -> Option<u64> {
        if pointer != Pointer::NULL {
            Some(pointer.0 + self.page_buf.len() as u64 - 1)
        } else {
            None
        }
    }

    fn seek_to(&mut self, pos: Pointer) -> Result<()> {
        self.file.seek(SeekFrom::Start(
            self.pointer_to_file_position(pos)
                .expect("tried to seek to null pointer"),
        ))?;
        Ok(())
    }

    fn writer(&mut self) -> &mut impl Write {
        &mut self.file
    }

    fn reader(&mut self) -> &mut impl Read {
        &mut self.file
    }

    fn current_position(&mut self) -> Result<Pointer> {
        let stream_position = self.file.stream_position()?;
        Ok(self.file_position_to_pointer(stream_position))
    }
}

pub trait Backend: Read + Write + Seek {
    fn truncate(&mut self, size: u64) -> Result<()>;
    fn init_max_size(&self) -> u64;
    fn init_page_size(&self) -> u16;
}

/// this is for tests
impl<'a, T> Backend for io::Cursor<&'a mut Vec<T>>
where
    io::Cursor<&'a mut Vec<T>>: Read + Write + Seek,
{
    fn truncate(&mut self, len: u64) -> Result<()> {
        self.get_mut().truncate(len as usize);
        Ok(())
    }

    fn init_max_size(&self) -> u64 {
        u64::MAX
    }

    fn init_page_size(&self) -> u16 {
        // smaller numbers make things easier to debug
        128
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
}

pub struct Transaction<'tx, F> {
    pub io: TxIo<'tx, F>,
    slots_by_name: &'tx HashMap<String, Meta>,
    indexers: &'tx mut Vec<Box<dyn RefCellIndexStore>>,
    list_refs: &'tx BTreeSet<ListSlot>,
    used_slots: &'tx BTreeSet<ListSlot>,
    tx_used_slots: BTreeSet<ListSlot>,
    tx_list_refs: BTreeSet<ListSlot>,
    tx_slots_by_name: HashMap<String, Meta>,
}

struct TxIoInner<F> {
    io: Rc<RefCell<Io<F>>>,
    free_space: Rc<RefCell<FreeSpace>>,
    changed_heads: HashMap<ListSlot, Pointer>,
}

impl<'tx, F: Backend> TxIoInner<F> {
    fn curr_head(&self, list_slot: ListSlot) -> Pointer {
        self.changed_heads
            .get(&list_slot)
            .cloned()
            .unwrap_or_else(|| self.io.borrow_mut().get_head(list_slot))
    }

    fn read_at<T: bincode::Decode>(&self, pointer: EntryPointer) -> Result<(EntryHandle, T)> {
        let mut io = self.io.borrow_mut();
        let value_pointer = pointer.value_pointer();
        io.seek_to(value_pointer)?;
        let val = bincode::decode_from_std_read(io.reader(), BINCODE_CONFIG)?;
        let end = io.current_position()?;
        let len = end.0 - value_pointer.0;
        Ok((
            EntryHandle {
                entry_pointer: pointer,
                value_len: len,
            },
            val,
        ))
    }

    fn raw_read_at<T: bincode::Decode>(&self, value_pointer: Pointer) -> Result<T> {
        let mut io = self.io.borrow_mut();
        io.seek_to(value_pointer)?;
        let val = bincode::decode_from_std_read(io.reader(), BINCODE_CONFIG)?;
        Ok(val)
    }
}

pub struct TxIo<'tx, F> {
    inner: Rc<RefCell<TxIoInner<F>>>,
    lifetime: PhantomData<&'tx ()>,
}

impl<F> core::fmt::Debug for TxIo<'_, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxIo").finish_non_exhaustive()
    }
}

impl<'tx, F> Clone for TxIo<'tx, F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            lifetime: PhantomData,
        }
    }
}

impl<'tx, F> AsRef<TxIo<'tx, F>> for TxIo<'tx, F> {
    fn as_ref(&self) -> &TxIo<'tx, F> {
        self
    }
}

impl<'tx, F: crate::Backend> TxIo<'tx, F> {
    fn into_inner(self) -> TxIoInner<F> {
        Rc::into_inner(self.inner)
            .expect("should only be called at end of tx")
            .into_inner()
    }

    pub fn iter(&self, slot: ListSlot) -> EntryIter<'tx, F> {
        let inner = self.inner.borrow();
        EntryIter {
            io: inner.io.clone(),
            curr: inner.curr_head(slot),
            remap: Default::default(),
            reverse_remap: Default::default(),
            lifetime: PhantomData,
        }
    }

    fn _push<T: bincode::Encode>(
        &self,
        list_slot: ListSlot,
        value: &T,
        extra_space: usize,
    ) -> Result<EntryHandle> {
        let curr_head = {
            let inner = self.inner.borrow();
            inner.curr_head(list_slot)
        };
        let handle = self.push_dangling(curr_head, value, extra_space)?;
        self.inner
            .borrow_mut()
            .changed_heads
            .insert(list_slot, handle.entry_pointer.this_entry);
        Ok(handle)
    }

    pub fn push<T: bincode::Encode>(&self, list_slot: ListSlot, value: &T) -> Result<EntryHandle> {
        self._push(list_slot, value, 0)
    }

    pub fn push_kv<K: bincode::Encode, V: bincode::Encode>(
        &self,
        list_slot: ListSlot,
        key: &K,
        value: &V,
    ) -> Result<EntryHandle> {
        let mut value_buf = vec![];
        let value_len = bincode::encode_into_std_write(value, &mut value_buf, BINCODE_CONFIG)?;
        let key_handle = self._push(list_slot, key, value_len)?;
        let inner = self.inner.borrow();
        let mut io = inner.io.borrow_mut();
        io.writer().write_all(&value_buf)?;
        Ok(key_handle)
    }

    pub(crate) fn encode_entry<T: bincode::Encode>(
        value: T,
        prev: Pointer,
    ) -> Result<(Vec<u8>, usize)> {
        let mut buf = vec![];
        let rev_pointer_len = bincode::encode_into_std_write(prev, &mut buf, BINCODE_CONFIG)?;
        debug_assert_eq!(rev_pointer_len as u64, prev.encoded_len());
        let value_len = bincode::encode_into_std_write(value, &mut buf, BINCODE_CONFIG)?;
        Ok((buf, value_len))
    }

    fn push_dangling<T: bincode::Encode>(
        &self,
        prev: Pointer,
        value: &T,
        extra_space: usize,
    ) -> Result<EntryHandle> {
        let (entry_bytes, value_len) = Self::encode_entry(value, prev)?;

        let inner = self.inner.borrow_mut();

        let location = inner
            .free_space
            .borrow_mut()
            .take_for_size(entry_bytes.len() as u64 + extra_space as u64)
            .ok_or(anyhow!("no more space in file"))?;

        let mut io = inner.io.borrow_mut();
        io.seek_to(location)?;
        io.writer().write_all(&entry_bytes)?;

        Ok(EntryHandle {
            entry_pointer: EntryPointer {
                this_entry: location,
                next_entry_possibly_stale: prev,
            },
            value_len: value_len as u64,
        })
    }

    pub fn pop<T: bincode::Encode + bincode::Decode>(
        &self,
        list_slot: ListSlot,
    ) -> Result<Option<T>> {
        let mut iter = self.iter(list_slot);
        Ok(
            if let Some((handle, value)) = iter.next_with_handle::<T>().transpose()? {
                let mut inner = self.inner.borrow_mut();
                let entry_pointer = handle.entry_pointer;
                inner.free_space.borrow_mut().free(Free::from_start_pointer(
                    entry_pointer.this_entry,
                    handle.entry_len(),
                ));
                inner
                    .changed_heads
                    .insert(list_slot, entry_pointer.next_entry_possibly_stale);
                Some(value)
            } else {
                None
            },
        )
    }

    pub fn free(&self, handle: EntryHandle) {
        self.inner
            .borrow()
            .free_space
            .borrow_mut()
            .free(Free::from_start_pointer(
                handle.entry_pointer.this_entry,
                handle.entry_len(),
            ));
    }

    pub fn read_at<T: bincode::Decode>(&self, pointer: EntryPointer) -> Result<(EntryHandle, T)> {
        self.inner.borrow().read_at(pointer)
    }

    pub fn raw_read_at<T: bincode::Decode>(&self, pointer: Pointer) -> Result<T> {
        self.inner.borrow().raw_read_at(pointer)
    }

    pub fn curr_head(&self, slot: ListSlot) -> Pointer {
        self.inner.borrow().curr_head(slot)
    }
}

impl<'tx, F: Backend> Transaction<'tx, F> {
    pub fn take_index<'i, I>(&'i self, index_handle: IndexHandle<I>) -> I::Api<'i, F>
    where
        I: IndexStore,
    {
        let dyn_store = &self.indexers[index_handle.id];
        let as_any = dyn_store.as_any();
        let store = as_any
            .downcast_ref::<RefCell<I>>()
            .expect("invalid index_handle passed in");

        let store = store
            .try_borrow_mut()
            .expect("index can only be taken once");

        let io: TxIo<'i, F> = self.io.clone();

        let api = I::create_api(store, io);

        api
    }

    pub fn store_index<I>(&mut self, index: I) -> IndexHandle<I>
    where
        I: IndexStore,
    {
        let index = RefCell::new(index);
        self.indexers.push(Box::new(index));
        IndexHandle {
            id: self.indexers.len() - 1,
            index_ty: PhantomData,
        }
    }

    pub fn store_and_take_index<'i, I>(&'i mut self, index: I) -> (IndexHandle<I>, I::Api<'i, F>)
    where
        I: IndexStore,
    {
        let handle = self.store_index(index);
        let api = self.take_index(handle);
        (handle, api)
    }

    pub fn take_list<T>(&mut self, list_name: &str) -> Result<LinkedList<T>> {
        let lookup_slot = self
            .slots_by_name
            .get(list_name)
            .or_else(|| self.tx_slots_by_name.get(list_name));
        let slot = match lookup_slot {
            Some(meta) => meta.slot,
            None => {
                if let Some(new_slot) = self.reserve_next_slot() {
                    let meta = Meta {
                        name: list_name.into(),
                        slot: new_slot,
                    };
                    self.io.push(META_LIST.slot(), &meta)?;
                    self.tx_slots_by_name.insert(list_name.into(), meta);
                    new_slot
                } else {
                    return Err(anyhow!("no more slots available"));
                }
            }
        };

        if self.list_refs.contains(&slot) || !self.tx_list_refs.insert(slot) {
            return Err(anyhow!(
                "attempt to take a second reference to list {}",
                list_name
            ));
        }

        Ok(LinkedList::new(slot))
    }

    fn reserve_next_slot(&mut self) -> Option<ListSlot> {
        let inner = self.io.inner.borrow();
        let n_list_slots = inner.io.borrow().n_list_slots;
        for slot in 0..n_list_slots {
            if self.used_slots.contains(&slot) || !self.tx_used_slots.insert(slot) {
                continue;
            }

            return Some(slot);
        }
        None
    }
}

impl<'tx, F> AsRef<TxIo<'tx, F>> for Transaction<'tx, F> {
    fn as_ref(&self) -> &TxIo<'tx, F> {
        &self.io
    }
}

pub struct EntryIter<'tx, F> {
    io: Rc<RefCell<Io<F>>>,
    remap: HashMap<Pointer, Pointer>,
    reverse_remap: HashMap<Pointer, Pointer>,
    curr: Pointer,
    lifetime: PhantomData<&'tx ()>,
}

impl<'tx, F: Backend> EntryIter<'tx, F> {
    pub fn into_pointer_iter(mut self) -> impl Iterator<Item = Result<EntryPointer>> + 'tx
    where
        F: 'tx,
    {
        core::iter::from_fn(move || self.next_pointer())
    }

    pub fn next<T: bincode::Encode + bincode::Decode>(&mut self) -> Option<Result<T>> {
        self.next_with_handle()
            .map(|res| res.map(|(_, value)| value))
    }

    fn map_to_current(&self, entry_pointer: Pointer) -> Pointer {
        self.remap
            .get(&entry_pointer)
            .cloned()
            .unwrap_or(entry_pointer)
    }

    /// Pointer to the next value
    pub fn next_pointer(&mut self) -> Option<Result<EntryPointer>> {
        (|| {
            let mut io = self.io.borrow_mut();
            if self.curr == Pointer::NULL {
                return Ok(None);
            }
            let this_entry = self.curr;
            io.seek_to(this_entry)?;
            let next_entry_possibly_stale: Pointer =
                bincode::decode_from_std_read(io.reader(), BINCODE_CONFIG)?;
            drop(io);
            self.curr = self.map_to_current(next_entry_possibly_stale);
            Ok(Some(EntryPointer {
                this_entry,
                next_entry_possibly_stale,
            }))
        })()
        .transpose()
    }

    pub(crate) fn next_with_handle<T: bincode::Encode + bincode::Decode>(
        &mut self,
    ) -> Option<Result<(EntryHandle, T)>> {
        (|| {
            let mut io = self.io.borrow_mut();
            if self.curr == Pointer::NULL {
                return Ok(None);
            }
            let this_entry = self.curr;
            io.seek_to(self.curr)?;
            let next_entry_possibly_stale: Pointer =
                bincode::decode_from_std_read(io.reader(), BINCODE_CONFIG)?;
            self.curr = self.map_to_current(next_entry_possibly_stale);
            let value_start = io.current_position()?;
            let value: T = bincode::decode_from_std_read(io.reader(), BINCODE_CONFIG)?;
            let value_end = io.current_position()?;
            let len = value_end.0 - value_start.0;
            Ok(Some((
                EntryHandle {
                    entry_pointer: EntryPointer {
                        this_entry,
                        next_entry_possibly_stale,
                    },
                    value_len: len,
                },
                value,
            )))
        })()
        .transpose()
    }

    pub fn remap(&mut self, Remap { from, to }: Remap) {
        // the thing we are remapping to may have already been remapped
        let to = self.map_to_current(to);

        // anything pointing to `from` must now point to `to`
        if let Some(prev_from) = self.reverse_remap.remove(&from) {
            self.remap.insert(prev_from, to);
            self.reverse_remap.insert(to, prev_from);
        }

        self.remap.insert(from, to);
        self.reverse_remap.insert(to, from);
    }
}

#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
pub struct Meta {
    pub name: String,
    pub slot: ListSlot,
}

#[derive(Debug, PartialEq)]
pub struct IndexHandle<I> {
    id: usize,
    index_ty: PhantomData<I>,
}

impl<I> Clone for IndexHandle<I> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            index_ty: self.index_ty.clone(),
        }
    }
}

impl<I> Copy for IndexHandle<I> {}
