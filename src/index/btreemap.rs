use crate::Backend;
use crate::EntryHandle;
use crate::LinkedList;
use crate::LinkedListApi;
use crate::TxIo;
use anyhow::Result;
use std::cell::RefMut;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap as StdBTreeMap;
use std::marker::PhantomData;
use std::ops::RangeBounds;

use super::IndexStore;

#[derive(Debug)]
pub struct BTreeMap<K, V> {
    list: LinkedList<(K, V)>,
    store: Store<K>,
}

#[derive(Debug)]
struct Store<K> {
    index: StdBTreeMap<K, EntryHandle>,
    tx_changes: Vec<Change<K>>,
}

#[derive(Debug)]
enum Change<K> {
    Insert {
        key: K,
        prev_value: Option<EntryHandle>,
    },
}

impl<K, V> BTreeMap<K, V>
where
    K: Ord + bincode::Encode + bincode::Decode + Clone,
    V: bincode::Encode + bincode::Decode,
{
    pub fn new<'tx, F: Backend>(
        list: LinkedList<(K, V)>,
        tx: impl AsRef<TxIo<'tx, F>>,
    ) -> Result<Self> {
        let api = list.api(&tx);
        let mut it = api.entry_iter();
        let mut index = StdBTreeMap::default();
        while let Some((key_handle, key)) = it.next_with_handle::<K>().transpose()? {
            if let Entry::Vacant(vacant) = index.entry(key) {
                vacant.insert(key_handle);
            }
        }
        let store = Store {
            index,
            tx_changes: Default::default(),
        };

        Ok(Self { list, store })
    }
}

impl<K: Send + 'static + Ord, V: Send + 'static> IndexStore for BTreeMap<K, V> {
    type Api<'i, F> = BTreeMapApi<'i, F, K, V>;

    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot> {
        self.list.owned_lists()
    }

    fn create_api<'s, F>(btree: RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized,
    {
        let (list, store) = RefMut::map_split(btree, |btree| (&mut btree.list, &mut btree.store));
        let list = LinkedList::create_api(list, io.clone());
        BTreeMapApi { io, list, store }
    }

    fn tx_fail_rollback(&mut self) {
        let Store { tx_changes, index } = &mut self.store;

        for change in tx_changes.drain(..).rev() {
            match change {
                Change::Insert {
                    key,
                    prev_value: prev_key_handle,
                } => {
                    match prev_key_handle {
                        Some(prev_key_handle) => index.insert(key, prev_key_handle),
                        None => index.remove(&key),
                    };
                }
            }
        }
    }

    fn tx_success(&mut self) {
        self.store.tx_changes.clear()
    }
}

pub struct BTreeMapApi<'tx, F, K, V> {
    io: TxIo<'tx, F>,
    list: LinkedListApi<'tx, F, (K, V)>,
    store: RefMut<'tx, Store<K>>,
}

impl<'tx, F, K, V> BTreeMapApi<'tx, F, K, V>
where
    K: Ord + bincode::Encode + bincode::Decode + Clone,
    V: bincode::Encode + bincode::Decode + PartialEq,
    F: Backend,
{
    pub fn insert(&mut self, key: K, value: &V) -> Result<Option<V>> {
        let Store { index, tx_changes } = &mut *self.store;
        let prev_value = match index.entry(key.clone()) {
            Entry::Occupied(mut occupied) => {
                let existing_key_handle = occupied.get_mut();
                let existing_value = self.io.raw_read_at(existing_key_handle.pointer_to_end())?;
                if &existing_value != value {
                    let new_key_handle = self.list.push_kv(&key, value)?;
                    tx_changes.push(Change::Insert {
                        key,
                        prev_value: Some(*existing_key_handle),
                    });
                    *existing_key_handle = new_key_handle;
                }
                Some(existing_value)
            }
            Entry::Vacant(vacant) => {
                let new_key_handle = self.list.push_kv(&key, value)?;
                vacant.insert(new_key_handle);
                self.store.tx_changes.push(Change::Insert {
                    key,
                    prev_value: None,
                });
                None
            }
        };

        Ok(prev_value)
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.store
            .index
            .get(key)
            .map(|key_handle| self.io.raw_read_at(key_handle.pointer_to_end()))
            .transpose()
    }

    pub fn range<R>(&self, range: R) -> Range<'_, F, K, V>
    where
        R: RangeBounds<K>,
    {
        Range {
            io: self.io.clone(),
            inner: self.store.index.range(range),
            value_ty: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.store.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.index.is_empty()
    }

    pub fn keys(&self) -> std::collections::btree_map::Keys<'_, K, EntryHandle> {
        self.store.index.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = Result<V>> + DoubleEndedIterator + '_ {
        self.range(..).map(|res| res.map(|(_, v)| v))
    }

    // TODO: make ExactSizeIterator version
    pub fn iter(&self) -> impl Iterator<Item = Result<(K, V)>> + DoubleEndedIterator + '_ {
        self.range(..)
    }
}

pub struct Range<'a, F, K, V> {
    inner: std::collections::btree_map::Range<'a, K, EntryHandle>,
    io: TxIo<'a, F>,
    value_ty: PhantomData<V>,
}

impl<'a, F, K, V> std::iter::Iterator for Range<'a, F, K, V>
where
    K: bincode::Decode + Clone,
    V: bincode::Decode,
    F: Backend,
{
    type Item = Result<(K, V)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(key, key_handle)| {
            Ok((
                key.clone(),
                self.io.raw_read_at(key_handle.pointer_to_end())?,
            ))
        })
    }
}

impl<'a, F, K, V> DoubleEndedIterator for Range<'a, F, K, V>
where
    K: bincode::Decode + Clone,
    V: bincode::Decode,
    F: Backend,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(key, key_handle)| {
            Ok((
                key.clone(),
                self.io.raw_read_at(key_handle.pointer_to_end())?,
            ))
        })
    }
}
