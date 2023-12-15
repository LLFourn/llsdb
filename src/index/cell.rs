use super::IndexStore;
use crate::{Backend, LinkedList, LinkedListApi, Pointer, Transaction, TxIo};
use anyhow::{anyhow, Result};
use core::cell::RefMut;

#[derive(Debug)]
pub struct Cell<T> {
    list: LinkedList<T>,
}

#[derive(Debug)]
pub struct CellApi<'i, F, T> {
    list: LinkedListApi<'i, F, T>,
}

impl<T> Cell<T>
where
    T: bincode::Encode + bincode::Decode,
{
    pub fn new<'a, F: crate::Backend>(
        list: crate::LinkedList<T>,
        tx: &Transaction<'a, F>,
    ) -> crate::Result<Self> {
        let mut iter = tx.io.iter(list.slot());
        match iter.next_pointer() {
            Some(_) => {
                if iter.next_pointer().is_some() {
                    return Err(anyhow!("Cell can only index a list with one item"));
                }

                Ok(Self { list })
            }
            None => Err(anyhow!(
                "Cell cannot index a list with no items. Consider using a CellOption instead."
            )),
        }
    }

    pub fn new_with_initial_value<'a, F: crate::Backend>(
        list: crate::LinkedList<T>,
        value: &T,
        tx: &Transaction<'a, F>,
    ) -> crate::Result<Self> {
        let api = list.api(tx);
        let mut iter = api.iter_pointers();

        match iter.next().transpose()? {
            Some(_) => {
                if iter.next().transpose()?.is_some() {
                    return Err(anyhow!("Cell can only index a list with one item"));
                }
            }
            None => {
                api.push(value)?;
            }
        }
        drop(iter);
        Ok(Self { list })
    }

    pub fn new_with_default<'a, F: crate::Backend>(
        list: crate::LinkedList<T>,
        tx: &mut Transaction<'a, F>,
    ) -> crate::Result<Self>
    where
        T: Default,
    {
        Self::new_with_initial_value(list, &T::default(), tx)
    }
}

impl<'i, F, T> CellApi<'i, F, T>
where
    T: bincode::Encode + bincode::Decode,
    F: crate::Backend,
{
    pub fn get(&self) -> crate::Result<T> {
        match self.list.head()? {
            Some(val) => Ok(val),
            None => Err(anyhow!("Call has list its item")),
        }
    }

    pub fn replace(&self, value: &T) -> crate::Result<T> {
        match self.list.pop()? {
            Some(old_value) => {
                self.list.push(value)?;
                Ok(old_value)
            }
            None => Err(anyhow!("Cell has lost its item")),
        }
    }
}

impl<T: Send + 'static> IndexStore for Cell<T> {
    type Api<'i, F> = CellApi<'i, F, T>;

    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot> {
        vec![self.list.slot()]
    }

    fn create_api<'s, F>(cell: RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized,
    {
        let refmut_list = RefMut::map(cell, |cell| &mut cell.list);
        CellApi {
            list: LinkedList::create_api(refmut_list, io),
        }
    }
}

/// Sepcialized cell representing an `Option<T>` on disk.
#[derive(Debug)]
pub struct CellOption<T> {
    list: LinkedList<T>,
}

impl<T> CellOption<T> {
    pub fn new<'tx, F: Backend>(
        list: crate::LinkedList<T>,
        tx: &Transaction<'tx, F>,
    ) -> Result<Self> {
        let api = list.api(tx);
        let mut iter = api.iter_pointers();
        if let Some(_) = iter.next().transpose()? {
            if iter.next().transpose()?.is_some() {
                return Err(anyhow!("CellOption can only index one item"));
            }
        }

        drop(iter);
        Ok(Self { list })
    }
}

#[derive(Debug)]
pub struct CellOptionApi<'i, F, T> {
    list: LinkedListApi<'i, F, T>,
}

impl<'i, F, T> CellOptionApi<'i, F, T>
where
    T: bincode::Encode + bincode::Decode,
    F: Backend,
{
    pub fn get(&self) -> Result<Option<T>> {
        self.list.head()
    }

    pub fn is_none(&self) -> bool {
        self.list.head_pointer() == Pointer::NULL
    }

    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    pub fn replace(&self, value: Option<&T>) -> Result<Option<T>> {
        let res = self.list.pop()?;
        if let Some(value) = value {
            self.list.push(value)?;
        }
        Ok(res)
    }

    pub fn clear(&self) -> Result<()> {
        self.list.clear()
    }

    pub fn take(&self) -> Result<Option<T>> {
        self.list.pop()
    }
}

impl<T: Send + 'static> IndexStore for CellOption<T> {
    type Api<'i, F> = CellOptionApi<'i, F, T>;

    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot> {
        vec![self.list.slot()]
    }

    fn create_api<'s, F>(cell: RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized,
    {
        let list = LinkedList::create_api(RefMut::map(cell, |cell| &mut cell.list), io);
        CellOptionApi { list }
    }
}
