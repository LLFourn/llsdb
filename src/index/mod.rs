mod btreemap;
pub use btreemap::*;
mod vec;
pub use vec::*;
mod cell;
pub use cell::*;

use crate::TxIo;
use std::cell::RefMut;

pub trait IndexStore: 'static + Send {
    type Api<'i, F>;
    fn tx_fail_rollback(&mut self) {}
    fn tx_success(&mut self) {}
    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot>;
    fn create_api<'s, F>(store: RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized;
}

/// plumbing trait for doing dynamic dispatch on a RefCell<T> where T: IndexStore
pub trait RefCellIndexStore: 'static + Send {
    fn tx_fail_rollback(&self);
    fn tx_success(&self);
    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot>;
    fn as_any(&self) -> &dyn core::any::Any;
}

impl<T: IndexStore> RefCellIndexStore for core::cell::RefCell<T> {
    fn tx_fail_rollback(&self) {
        self.borrow_mut().tx_fail_rollback()
    }

    fn tx_success(&self) {
        self.borrow_mut().tx_success()
    }

    fn owned_lists(&self) -> std::vec::Vec<crate::ListSlot> {
        self.borrow().owned_lists()
    }

    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
}
