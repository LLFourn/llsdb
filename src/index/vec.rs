use crate::{
    Backend, EntryHandle, EntryPointer, LinkedList, LinkedListApi, LinkedListMut, LinkedListMutApi,
    Mut, Pointer, Transaction, TxIo,
};
use anyhow::Result;
use std::{cell::RefMut, collections::VecDeque, vec::Vec as StdVec};

use super::IndexStore;

#[derive(Debug)]
pub struct Vec<T> {
    list: crate::LinkedList<T>,
    store: VecStore,
}

#[derive(Debug)]
struct VecStore {
    index: VecDeque<Pointer>,
    tx_changes: StdVec<Change>,
}

#[derive(Debug)]
enum Change {
    Push,
    Pop(Pointer),
}

impl<T> Vec<T>
where
    T: bincode::Encode + bincode::Decode,
{
    pub fn new<'tx, F: Backend>(
        list: crate::LinkedList<T>,
        tx: &Transaction<'tx, F>,
    ) -> Result<Self> {
        let mut it = tx.io.iter(list.slot());
        let mut index = VecDeque::new();
        while let Some(next_pointer) = it.next_pointer() {
            match next_pointer {
                Ok(next_pointer) => {
                    index.push_front(next_pointer.value_pointer());
                }
                Err(e) => {
                    index.clear();
                    return Err(e);
                }
            }
        }

        index.make_contiguous();

        let store = Vec {
            list,
            store: VecStore {
                index,
                tx_changes: Default::default(),
            },
        };

        Ok(store)
    }
}

impl<T: 'static + Send> IndexStore for Vec<T> {
    type Api<'i, F> = VecApi<'i, F, T>;
    fn tx_fail_rollback(&mut self) {
        let VecStore {
            tx_changes, index, ..
        } = &mut self.store;
        for change in tx_changes.drain(..).rev() {
            match change {
                Change::Push => assert!(index.pop_back().is_some()),
                Change::Pop(pointer) => index.push_back(pointer),
            }
        }
    }

    fn tx_success(&mut self) {
        self.store.tx_changes.clear();
    }

    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot> {
        vec![self.list.slot()]
    }

    fn create_api<'s, F>(vec: RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized,
    {
        let (list, store) = RefMut::map_split(vec, |vec| (&mut vec.list, &mut vec.store));
        let list = LinkedList::create_api(list, io.clone());
        VecApi { io, list, store }
    }
}

#[derive(Debug)]
pub struct VecApi<'i, F, T> {
    io: TxIo<'i, F>,
    store: RefMut<'i, VecStore>,
    list: LinkedListApi<'i, F, T>,
}

impl<'i, F, T> VecApi<'i, F, T>
where
    T: bincode::Encode + bincode::Decode,
    F: Backend,
{
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Result<T>> + ExactSizeIterator + '_ {
        let io = self.io.clone();

        self.store
            .index
            .iter()
            .map(move |pointer| io.raw_read_at(*pointer))
    }

    pub fn get(&self, index: usize) -> Result<Option<T>> {
        let pointer = match self.store.index.get(index) {
            Some(pointer) => pointer,
            _ => return Ok(None),
        };

        Ok(Some(self.io.raw_read_at(*pointer)?))
    }

    pub fn push(&mut self, value: &T) -> Result<()> {
        let handle = self.list.push(value)?;
        self.store.tx_changes.push(Change::Push);
        self.store.index.push_back(handle.value_pointer());
        Ok(())
    }

    pub fn pop(&mut self) -> Result<Option<T>> {
        match self.list.pop()? {
            Some(value) => {
                let pointer = self.store.index.pop_back().expect("must exist");
                self.store.tx_changes.push(Change::Pop(pointer));
                Ok(Some(value))
            }
            None => {
                assert_eq!(self.store.index.len(), 0);
                Ok(None)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.store.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.index.is_empty()
    }
}

#[derive(Debug)]
pub struct VecRemove<T> {
    list: crate::LinkedListMut<T>,
    store: VecRemoveStore,
}

#[derive(Debug)]
struct VecRemoveStore {
    index: VecDeque<EntryPointer>,
    tx_changes: StdVec<ChangeMut>,
}

#[derive(Debug)]
pub struct VecRemoveApi<'i, F, T> {
    io: TxIo<'i, F>,
    list: LinkedListMutApi<'i, F, T>,
    store: RefMut<'i, VecRemoveStore>,
}

#[derive(Debug)]
pub enum ChangeMut {
    Push,
    Pop(EntryPointer),
    Remove(usize, EntryPointer),
}

impl<T> VecRemove<T>
where
    T: bincode::Encode + bincode::Decode + Send,
{
    pub fn new<'tx, F: Backend>(
        list: crate::LinkedList<Mut<T>>,
        tx: &Transaction<'tx, F>,
    ) -> Result<Self> {
        let list = LinkedListMut(list);
        let list_api = list.api(&tx.io);
        let mut it = list_api.iter_pointers();
        let mut index = VecDeque::new();
        while let Some(next_pointer) = it.next() {
            match next_pointer {
                Ok(next_pointer) => {
                    index.push_front(next_pointer);
                }
                Err(e) => {
                    index.clear();
                    return Err(e);
                }
            }
        }

        drop(it);
        drop(list_api);

        index.make_contiguous();

        let store = Self {
            list,
            store: VecRemoveStore {
                index,
                tx_changes: Default::default(),
            },
        };

        Ok(store)
    }
}

impl<T: 'static + Send> IndexStore for VecRemove<T> {
    type Api<'i, F> = VecRemoveApi<'i, F, T>;

    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot> {
        vec![self.list.0.slot()]
    }

    fn tx_fail_rollback(&mut self) {
        let VecRemoveStore {
            tx_changes, index, ..
        } = &mut self.store;
        for change in tx_changes.drain(..).rev() {
            match change {
                ChangeMut::Push => assert!(index.pop_back().is_some()),
                ChangeMut::Pop(pointer) => index.push_back(pointer),
                ChangeMut::Remove(i, pointer) => index.insert(i, pointer),
            }
        }
    }

    fn tx_success(&mut self) {
        self.store.tx_changes.clear();
    }

    fn create_api<'s, F>(vec: RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized,
    {
        let (list, store) = RefMut::map_split(vec, |vec| (&mut vec.list, &mut vec.store));
        let list = LinkedListMut::create_api(list, io.clone());
        VecRemoveApi { list, store, io }
    }
}

impl<'i, F, T> VecRemoveApi<'i, F, T>
where
    T: bincode::Encode + bincode::Decode + core::fmt::Debug,
    F: Backend + 'i,
{
    pub fn get(&self, index: usize) -> Result<Option<T>> {
        let pointer = match self.store.index.get(index) {
            Some(pointer) => pointer,
            _ => return Ok(None),
        };

        let (_, value) = self.io.read_at::<Mut<T>>(*pointer)?;
        Ok(Some(
            value.into_value().expect("VecMut references values only"),
        ))
    }

    pub fn push(&mut self, value: T) -> Result<()> {
        let handle = self.list.push(value)?;
        self.store.index.push_back(handle.entry_pointer);
        self.store.tx_changes.push(ChangeMut::Push);
        Ok(())
    }

    pub fn pop(&mut self) -> Result<Option<T>> {
        let value = self.list.pop()?;
        if let Some(popped) = self.store.index.pop_back() {
            self.store.tx_changes.push(ChangeMut::Pop(popped));
        }
        Ok(value)
    }

    pub fn retain(&mut self, mut f: impl FnMut(T) -> bool) -> Result<()> {
        let mut to_remove = vec![];
        for (i, res) in self._iter().enumerate() {
            let (handle, value) = res?;
            if !f(value) {
                to_remove.push((i, handle));
            }
        }

        // it's important to remove from the back since this means we don't need to adjust i but
        // also because removing the last element in the list can be optimized to a pop.
        for (i, handle) in to_remove.into_iter().rev() {
            self.list.unlink(handle)?;
            let removed = self.store.index.remove(i).expect("must exist");
            self.store.tx_changes.push(ChangeMut::Remove(i, removed));
        }

        Ok(())
    }

    pub fn remove(&mut self, index: usize) -> Result<T> {
        let pointer = self.store.index[index];
        let (handle, value) = self.io.read_at::<Mut<T>>(pointer)?;
        let value = value.into_value().expect("VecMut only points to values");
        self.list.unlink(handle)?;
        let removed = self.store.index.remove(index).expect("must exist");
        self.store
            .tx_changes
            .push(ChangeMut::Remove(index, removed));
        Ok(value)
    }

    pub fn len(&self) -> usize {
        self.store.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.index.is_empty()
    }

    fn _iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = Result<(EntryHandle, T)>> + ExactSizeIterator + '_ {
        let io = self.io.clone();
        self.store.index.iter().map(move |pointer| {
            let (value_handle, value) = io.read_at::<Mut<T>>(*pointer)?;
            Ok((value_handle, value.unwrap_value()))
        })
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Result<T>> + ExactSizeIterator + '_ {
        self._iter().map(|res| res.map(|(_, value)| value))
    }

    pub fn clear(&mut self) -> Result<()> {
        self.list.clear()?;
        let mut index = core::mem::take(&mut self.store.index);
        self.store.tx_changes.extend(
            index
                .drain(..)
                .enumerate()
                .map(|(i, entry_pointer)| ChangeMut::Remove(i, entry_pointer)),
        );
        Ok(())
    }
}
