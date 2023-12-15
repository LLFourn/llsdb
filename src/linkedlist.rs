use crate::{
    index::IndexStore, Backend, EntryHandle, EntryIter, EntryPointer, ListSlot, Pointer, Remap,
    TxIo,
};
use anyhow::Result;
use core::marker::PhantomData;
use std::cell::RefMut;

#[derive(Debug)]
pub struct LinkedList<T> {
    value_type: PhantomData<T>,
    slot: ListSlot,
}

impl<T> Clone for LinkedList<T> {
    fn clone(&self) -> Self {
        Self {
            value_type: self.value_type.clone(),
            slot: self.slot.clone(),
        }
    }
}

impl<T> LinkedList<T> {
    pub const fn new(slot: ListSlot) -> Self {
        Self {
            slot,
            value_type: PhantomData,
        }
    }

    pub const fn slot(&self) -> ListSlot {
        self.slot
    }

    pub fn api<'a, 'tx: 'a, F>(&'a self, io: impl AsRef<TxIo<'tx, F>>) -> LinkedListApi<'a, F, T> {
        LinkedListApi {
            io: io.as_ref().clone(),
            slot: self.slot,
            value_type: PhantomData,
        }
    }
}

impl<T: Send + 'static> IndexStore for LinkedList<T> {
    type Api<'i, F> = LinkedListApi<'i, F, T>;

    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot> {
        vec![self.slot]
    }

    fn create_api<'s, F>(store: std::cell::RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized,
    {
        LinkedListApi {
            io,
            slot: store.slot,
            value_type: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct LinkedListApi<'i, F, T> {
    io: TxIo<'i, F>,
    slot: ListSlot,
    value_type: PhantomData<T>,
}

impl<'i, F, T> LinkedListApi<'i, F, T>
where
    F: Backend,
{
    pub fn iter_pointers(&self) -> impl Iterator<Item = Result<EntryPointer>> + '_ {
        let mut it = self.io.iter(self.slot);
        core::iter::from_fn(move || it.next_pointer())
    }
}

impl<'i, F, T> LinkedListApi<'i, F, T>
where
    F: Backend,
    T: bincode::Encode + bincode::Decode,
{
    pub fn head_pointer(&self) -> Pointer {
        self.io.curr_head(self.slot)
    }

    pub fn head(&self) -> Result<Option<T>> {
        self.io.iter(self.slot).next::<T>().transpose()
    }

    pub fn push(&self, value: &T) -> Result<EntryHandle> {
        self.io.push(self.slot, value)
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<T>> + '_ {
        let mut it = self.io.iter(self.slot);
        core::iter::from_fn(move || it.next::<T>())
    }

    pub fn pop(&self) -> Result<Option<T>> {
        self.io.pop(self.slot)
    }

    pub fn entry_iter(&self) -> EntryIter<'i, F> {
        self.io.iter(self.slot)
    }

    pub fn clear(&self) -> Result<()> {
        loop {
            if self.pop()?.is_none() {
                break;
            }
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.head_pointer() == Pointer::NULL
    }
}

impl<'i, F, K, V> LinkedListApi<'i, F, (K, V)>
where
    F: Backend,
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    pub fn push_kv(&self, key: &K, value: &V) -> Result<EntryHandle> {
        self.io.push_kv(self.slot, key, value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, bincode::Encode, bincode::Decode)]
pub enum Mut<T> {
    Add(T),
    Remap(Remap),
}

impl<T> Mut<T> {
    pub fn into_value(self) -> Option<T> {
        match self {
            Mut::Add(value) => Some(value),
            _ => None,
        }
    }

    pub fn unwrap_value(self) -> T {
        self.into_value().expect("must not point to remap")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, bincode::Encode, bincode::Decode)]
/// Read the `Mut` but not read the value
pub enum MutNoValue {
    Add,
    Remove(Remap),
}

#[derive(Debug)]
pub struct LinkedListMut<T>(pub LinkedList<Mut<T>>);

impl<T> LinkedListMut<T> {
    pub fn api<'a, 'tx: 'a, F>(
        &'a self,
        io: impl AsRef<TxIo<'tx, F>>,
    ) -> LinkedListMutApi<'a, F, T> {
        LinkedListMutApi(self.0.api(io))
    }
}
#[derive(Debug)]
pub struct LinkedListMutApi<'i, F, T>(LinkedListApi<'i, F, Mut<T>>);

impl<T: Send + 'static> IndexStore for LinkedListMut<T> {
    type Api<'i, F> = LinkedListMutApi<'i, F, T>;

    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot> {
        self.0.owned_lists()
    }

    fn create_api<'s, F>(list: std::cell::RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized,
    {
        let list = RefMut::map(list, |list| &mut list.0);
        LinkedListMutApi(LinkedList::create_api(list, io))
    }
}

impl<'i, F, T> LinkedListMutApi<'i, F, T>
where
    F: Backend,
    T: bincode::Encode + bincode::Decode,
{
    pub fn unlink(&self, handle: EntryHandle) -> Result<()> {
        let io = &self.0.io;
        let end_of_list = io.curr_head(self.0.slot);
        let entry_pointer = handle.entry_pointer;
        if end_of_list == entry_pointer.this_entry {
            self.0.pop()?;
        } else {
            io.push(
                self.0.slot,
                &Mut::<T>::Remap(Remap {
                    from: entry_pointer.this_entry,
                    to: entry_pointer.next_entry_possibly_stale,
                }),
            )?;
            io.free(handle);
        }
        Ok(())
    }

    pub fn push(&self, value: T) -> Result<EntryHandle> {
        self.0.io.push(self.0.slot, &Mut::Add(value))
    }

    pub fn iter_handles(&self) -> impl Iterator<Item = Result<(EntryHandle, T)>> + '_ {
        let mut it = self.0.io.iter(self.0.slot);
        core::iter::from_fn(move || loop {
            match it.next_with_handle::<Mut<T>>()? {
                Ok((handle, value)) => match value {
                    Mut::Remap(remap) => it.remap(remap),
                    Mut::Add(entry) => break Some(Ok((handle, entry))),
                },
                Err(e) => break Some(Err(e)),
            }
        })
    }

    pub fn iter_pointers(&self) -> impl Iterator<Item = Result<EntryPointer>> + '_ {
        let mut it = self.0.io.iter(self.0.slot);
        core::iter::from_fn(move || loop {
            match it.next_with_handle::<MutNoValue>()? {
                Ok((handle, value)) => match value {
                    MutNoValue::Remove(remap) => it.remap(remap),
                    MutNoValue::Add => break Some(Ok(handle.entry_pointer)),
                },
                Err(e) => break Some(Err(e)),
            }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<T>> + '_ {
        self.iter_handles().map(|res| res.map(|(_, value)| value))
    }

    pub fn pop(&self) -> Result<Option<T>> {
        if let Some((handle, value)) = self.iter_handles().next().transpose()? {
            self.unlink(handle)?;
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub fn clear(&self) -> Result<()> {
        self.0.clear()
    }
}
