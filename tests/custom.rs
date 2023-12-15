use std::cell::RefMut;

use llsdb::{
    index::{IndexStore, Vec},
    Backend, Result, Transaction, TxIo,
};

#[derive(Debug)]
pub struct Custom {
    foos: Vec<String>,
    bars: Vec<u32>,
}

impl Custom {
    pub fn new(tx: &mut Transaction<'_, impl Backend>) -> Result<Self> {
        let foos_list = tx.take_list("foos")?;
        let foos = Vec::new(foos_list, tx)?;
        let bars_list = tx.take_list("bars")?;
        let bars = Vec::new(bars_list, tx)?;
        Ok(Self { foos, bars })
    }
}

#[derive(Debug)]
pub struct CustomApi<'i, F> {
    foos: <Vec<String> as IndexStore>::Api<'i, F>,
    bars: <Vec<u32> as IndexStore>::Api<'i, F>,
}

impl IndexStore for Custom {
    type Api<'i, F> = CustomApi<'i, F>;

    fn owned_lists(&self) -> std::vec::Vec<llsdb::ListSlot> {
        self.bars
            .owned_lists()
            .into_iter()
            .chain(self.foos.owned_lists())
            .collect()
    }

    fn create_api<'s, F>(store: RefMut<'s, Self>, io: TxIo<'s, F>) -> Self::Api<'s, F>
    where
        Self: Sized,
    {
        let (foos, bars) = RefMut::map_split(store, |custom| (&mut custom.foos, &mut custom.bars));
        let foos = Vec::create_api(foos, io.clone());
        let bars = Vec::create_api(bars, io.clone());
        CustomApi { foos, bars }
    }

    fn tx_fail_rollback(&mut self) {
        self.foos.tx_fail_rollback();
        self.bars.tx_fail_rollback()
    }

    fn tx_success(&mut self) {
        self.foos.tx_success();
        self.bars.tx_success();
    }
}

impl<'i, F: Backend> CustomApi<'i, F> {
    pub fn push_string(&mut self, string: String) -> Result<()> {
        self.bars.push(&(string.len() as u32))?;
        self.foos.push(&string)?;
        Ok(())
    }
}
