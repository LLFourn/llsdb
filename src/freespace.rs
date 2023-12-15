use core::mem::size_of;
use std::collections::{BTreeMap, BTreeSet};

type Pointer = u64;

#[derive(Clone, Debug, PartialEq)]
pub struct FreeSpace {
    end_to_start: BTreeMap<Pointer, Pointer>,
    sizes: BTreeSet<Free>,
    tx_changes: Vec<Change>,
    pending_frees: Vec<Free>,
    persist: PersistFreeSpace,
}

#[derive(Debug, Clone, Copy, bincode::Encode, bincode::Decode, PartialEq, Eq, PartialOrd, Ord)]
pub struct Free {
    size: u64,
    end_pointer: Pointer,
}

impl Free {
    pub fn from_start_pointer(start_pointer: crate::Pointer, size: u64) -> Self {
        Self {
            size,
            end_pointer: start_pointer.0 + size,
        }
    }

    pub const NULL: Self = Free {
        size: 0,
        end_pointer: 0,
    };
    pub fn write_to(&self, buf: &mut [u8]) {
        buf[..size_of::<u64>()].copy_from_slice(self.size.to_le_bytes().as_ref());
        buf[size_of::<u64>()..].copy_from_slice(self.end_pointer.to_le_bytes().as_ref());
    }

    pub fn read_from(buf: &[u8]) -> Option<Free> {
        assert_eq!(buf.len(), size_of::<Free>());
        let mut size_buf = [0u8; size_of::<u64>()];
        let mut end_pointer_buf = [0u8; size_of::<u64>()];
        size_buf.copy_from_slice(&buf[..size_of::<u64>()]);
        end_pointer_buf.copy_from_slice(&buf[size_of::<u64>()..]);
        let free = Self {
            size: u64::from_le_bytes(size_buf),
            end_pointer: u64::from_le_bytes(end_pointer_buf),
        };
        if free.size > free.end_pointer {
            return None;
        }

        Some(free)
    }

    pub fn start_pointer(&self) -> Pointer {
        self.end_pointer - self.size
    }
}

impl Default for Free {
    fn default() -> Self {
        Self::NULL
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Change {
    Remove(Free),
    Add(Free),
}

impl FreeSpace {
    pub fn new(n_persist: usize) -> Self {
        Self {
            end_to_start: Default::default(),
            sizes: Default::default(),
            tx_changes: Default::default(),
            pending_frees: Default::default(),
            persist: PersistFreeSpace::new(n_persist),
        }
    }

    pub fn new_from_persist_state(state: Vec<Free>) -> Self {
        let persist = PersistFreeSpace::restore(state);
        let mut end_to_start = BTreeMap::default();
        let mut sizes = BTreeSet::default();
        for free in persist.state() {
            end_to_start.insert(free.end_pointer, free.start_pointer());
            sizes.insert(*free);
        }
        Self {
            end_to_start,
            sizes,
            persist,
            ..Self::new(0)
        }
    }

    pub fn persist_state(&self) -> &[Free] {
        self.persist.state()
    }

    fn insert(
        &mut self,
        Free {
            mut end_pointer,
            size,
        }: Free,
    ) {
        if size == 0 {
            return;
        }
        let mut start_pointer = end_pointer - size;
        let (start, end) = loop {
            let suffix_check = self.end_to_start.range(..end_pointer).last();
            let prefix_check = self.end_to_start.range(end_pointer..).next();

            match (suffix_check, prefix_check) {
                // the new space suffixes an existing space
                (Some((&existing_end, &existing_start)), _) if existing_end == start_pointer => {
                    let _size = self.remove(existing_end);
                    debug_assert_eq!(_size, Some(existing_end - existing_start));
                    start_pointer = existing_start;
                }
                // the new space prefixes an existing space
                (_, Some((&existing_end, &existing_start))) if existing_start == end_pointer => {
                    let _size = self.remove(existing_end);
                    debug_assert_eq!(_size, Some(existing_end - existing_start));
                    end_pointer = existing_end;
                }
                _ => break (start_pointer, end_pointer),
            };
        };

        let space_size = end - start;
        let free = Free {
            end_pointer: end,
            size: space_size,
        };
        self.tx_changes.push(Change::Add(free));
        assert!(self.end_to_start.insert(end, start).is_none());
        assert!(self.sizes.insert(free));
        self.persist.add(free);
    }

    fn remove(&mut self, end_pointer: Pointer) -> Option<u64> {
        self.resize(end_pointer, 0)
    }

    pub fn free(&mut self, space: Free) {
        self.pending_frees.push(space);
    }

    fn resize(&mut self, end_pointer: Pointer, new_size: u64) -> Option<u64> {
        if let Some(start_pointer) = self.end_to_start.remove(&end_pointer) {
            let current_size = end_pointer - start_pointer;
            let mut free = Free {
                end_pointer,
                size: current_size,
            };
            assert!(self.sizes.remove(&free));
            self.persist.remove(free);
            self.tx_changes.push(Change::Remove(free));
            if new_size != 0 {
                free.size = new_size;
                self.insert(free);
            }
            return Some(current_size);
        }

        None
    }

    pub fn where_to_trim(&self) -> Option<crate::Pointer> {
        self.end_to_start
            .last_key_value()
            .map(|(_, &start)| crate::Pointer(start))
    }

    pub fn tx_fail_rollback(&mut self) {
        while let Some(change) = self.tx_changes.pop() {
            match change {
                Change::Add(free) => {
                    assert_eq!(
                        self.end_to_start.remove(&free.end_pointer),
                        Some(free.end_pointer - free.size)
                    );
                    assert!(self.sizes.remove(&free));
                    self.persist.remove(free);
                }
                Change::Remove(free) => {
                    assert!(self
                        .end_to_start
                        .insert(free.end_pointer, free.start_pointer())
                        .is_none());
                    assert!(self.sizes.insert(free));
                    self.persist.add(free);
                }
            }
        }
        let _ = self.persist.take_changed_slots();
        self.pending_frees.clear();
    }

    #[must_use]
    pub fn apply_pending_frees(&mut self) -> BTreeSet<usize> {
        let pending_frees = core::mem::take(&mut self.pending_frees);
        for free in pending_frees {
            self.insert(free);
        }
        self.persist.take_changed_slots()
    }

    pub fn tx_success(&mut self) {
        self.tx_changes.clear();
    }

    pub fn take_for_size(&mut self, size: u64) -> Option<crate::Pointer> {
        let free = self
            .sizes
            .range(
                &Free {
                    size,
                    end_pointer: Pointer::MIN,
                }..,
            )
            .next()?
            .clone();

        let remaining_size = free.size - size;
        self.resize(free.end_pointer, remaining_size);

        Some(crate::Pointer(free.start_pointer()))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PersistFreeSpace {
    state: Vec<Free>,
    reverse_by_size: BTreeMap<Free, usize>,
    unused_slots: Vec<usize>,
    unplaced_queue: BTreeSet<Free>,
    changed_slots: BTreeSet<usize>,
}

impl PersistFreeSpace {
    pub fn new(n_persist: usize) -> Self {
        Self {
            state: vec![Free::NULL; n_persist],
            reverse_by_size: Default::default(),
            // rev so we pop the lower indices first
            unused_slots: (0..n_persist).rev().collect(),
            unplaced_queue: Default::default(),
            changed_slots: Default::default(),
        }
    }

    pub fn restore(state: Vec<Free>) -> Self {
        let mut new = Self {
            state,
            unused_slots: Default::default(),
            ..Self::new(0)
        };
        // rev so the lower indices get popped first
        for (i, free) in new.state.iter().enumerate().rev() {
            if free == &Free::NULL {
                new.unused_slots.push(i);
            } else {
                new.reverse_by_size.insert(*free, i);
            }
        }
        new
    }

    pub fn remove(&mut self, free: Free) {
        if let Some(slot) = self.reverse_by_size.remove(&free) {
            let _removed = core::mem::take(&mut self.state[slot]);
            self.changed_slots.insert(slot);
            debug_assert_eq!(_removed, free);
            self.unused_slots.push(slot);

            if let Some(next_in_queue) = self.unplaced_queue.pop_last() {
                self.add(next_in_queue);
            }
            return;
        }

        if self.unplaced_queue.remove(&free) {
            return;
        }

        panic!("removed something that was neither in a free slot or unplaced");
    }

    pub fn add(&mut self, free: Free) {
        let slot = if let Some(unused_slot) = self.unused_slots.pop() {
            Some(unused_slot)
        } else {
            let (&smallest, &slot) = self
                .reverse_by_size
                .first_key_value()
                .expect("there are no unused slots");
            // NOTE: this doesn't compare .size because add needs to be STRICTLY the inverse of
            // remove so add a free space may displace a free space of the same size!
            if free > smallest {
                self.reverse_by_size.remove(&smallest).expect("invariant");
                self.unplaced_queue.insert(smallest);
                Some(slot)
            } else {
                None
            }
        };

        match slot {
            Some(slot) => {
                self.reverse_by_size.insert(free, slot);
                self.state[slot] = free;
                self.changed_slots.insert(slot);
            }
            None => {
                self.unplaced_queue.insert(free);
            }
        }
    }

    pub fn state(&self) -> &[Free] {
        &self.state
    }

    pub fn take_changed_slots(&mut self) -> BTreeSet<usize> {
        core::mem::take(&mut self.changed_slots)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::*;

    #[derive(Debug, Clone, Copy)]
    enum Action {
        Take(u64),
        Free,
    }

    impl Action {
        pub fn apply(
            self,
            spaces: &mut Vec<Free>,
            free_space: &mut FreeSpace,
            rng: &mut impl RngCore,
        ) {
            match self {
                Action::Take(size) => {
                    let pointer = free_space.take_for_size(size).unwrap();
                    spaces.push(Free::from_start_pointer(pointer, size));
                }
                Action::Free => {
                    if spaces.len() > 1 {
                        let index = rng.gen_range(0..spaces.len());
                        let free = spaces.remove(index);
                        free_space.free(free);
                    }
                }
            }
        }
    }

    fn change_strat() -> impl Strategy<Value = Action> {
        prop_oneof![
            2 => (1..256).prop_map(|size| Action::Take(size as u64)),
            1 => Just(Action::Free),
        ]
    }

    proptest! {

        #[test]
        fn rollbacks_always_restore(
            init in proptest::collection::vec(change_strat(), 0usize..75),
            success in proptest::collection::vec(change_strat(), 0usize..75),
            rollback_actions in proptest::collection::vec(change_strat(), 0usize..75),
            n_persist in 1usize..100,
        ) {
            run_test(init, success, rollback_actions, n_persist)
        }

    }

    fn run_test(
        init: Vec<Action>,
        success: Vec<Action>,
        rollback_actions: Vec<Action>,
        n_persist: usize,
    ) {
        let mut free_space = FreeSpace::new(n_persist);
        // manually insert initial space
        free_space.insert(Free::from_start_pointer(crate::Pointer(0), 256 * 1000));
        let mut spaces = vec![];
        let mut rng = TestRng::deterministic_rng(RngAlgorithm::ChaCha);

        for action in init {
            action.apply(&mut spaces, &mut free_space, &mut rng);
        }

        let _ = free_space.apply_pending_frees();
        free_space.tx_success();

        for action in success {
            action.apply(&mut spaces, &mut free_space, &mut rng);
        }

        let _ = free_space.apply_pending_frees();
        free_space.tx_success();

        let before_rollback = free_space.clone();

        for action in rollback_actions {
            action.apply(&mut spaces, &mut free_space, &mut rng);
        }

        let _ = free_space.apply_pending_frees();
        free_space.tx_fail_rollback();

        assert_eq!(before_rollback, free_space);
    }
}
