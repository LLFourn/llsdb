use llsdb::{LinkedList, LlsDb};
use std::io::Cursor;

#[test]
fn linked_list_head() {
    let mut backend = vec![];

    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

        let ll1 = db
            .execute(|tx| {
                let ll1: LinkedList<u32> = tx.take_list("ll1")?;
                assert_eq!(ll1.api(tx).head()?, None);
                Ok(ll1)
            })
            .unwrap();

        db.execute(|tx| ll1.api(tx).push(&50)).unwrap();
        assert_eq!(db.execute(|tx| ll1.api(tx).head()).unwrap(), Some(50));

        let ll2 = db
            .execute(|tx| {
                let ll2: LinkedList<u32> = tx.take_list("ll2")?;
                assert_eq!(ll2.api(tx).head()?, None);
                Ok(ll2)
            })
            .unwrap();

        db.execute(|tx| ll2.api(tx).push(&60)).unwrap();
        assert_eq!(db.execute(|tx| ll2.api(tx).head()).unwrap(), Some(60));

        assert_eq!(
            db.execute(|tx| {
                let ll1 = ll1.api(&tx);
                let ll2 = ll2.api(&tx);
                ll1.push(&51)?;
                ll2.push(&61)?;
                Ok((ll1.head()?, ll2.head()?))
            })
            .unwrap(),
            (Some(51), Some(61))
        );

        assert_eq!(
            db.execute(|tx| { Ok((ll1.api(&tx).head()?, ll2.api(&tx).head()?)) })
                .unwrap(),
            (Some(51), Some(61))
        );

        {
            let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
            let ll1: LinkedList<u32> = db.get_list("ll1").unwrap();
            let ll2: LinkedList<u32> = db.get_list("ll2").unwrap();

            assert_eq!(
                db.execute(|tx| { Ok((ll1.api(&tx).head()?, ll2.api(&tx).head()?)) })
                    .unwrap(),
                (Some(51), Some(61))
            );
        }
    }
}

#[test]
fn allocating_same_list() {
    let mut backend = vec![];
    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
        db.execute(|tx| {
            let ll1 = tx.take_list::<u32>("ll1")?;
            let api = ll1.api(&tx);
            api.push(&42)?;
            Ok(())
        })
        .unwrap();
    }

    {
        let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
        let ll2 = db
            .execute(|tx| {
                let ll2 = tx.take_list::<u32>("ll1")?;
                assert_eq!(ll2.api(tx).head()?, Some(42));
                Ok(ll2)
            })
            .unwrap();
        assert_eq!(db.execute(|tx| ll2.api(tx).head()).unwrap(), Some(42));
    }
}

#[test]
fn linked_list_push_result_of_head() {
    let mut backend = vec![];
    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

        let ll1 = db
            .execute(|tx| {
                let ll1: LinkedList<u32> = tx.take_list("ll1")?;
                ll1.api(tx).push(&1)?;
                Ok(ll1)
            })
            .unwrap();
        let ll2 = db
            .execute(|tx| {
                let ll2: LinkedList<u32> = tx.take_list("ll2").unwrap();
                let head = ll1.api(&tx).head()?.unwrap();
                ll2.api(&tx).push(&head)?;
                Ok(ll2)
            })
            .unwrap();

        assert_eq!(
            db.execute(|tx| { Ok((ll1.api(&tx).head()?, ll2.api(&tx).head()?)) })
                .unwrap(),
            (Some(1), Some(1))
        );
    }
}

#[test]
fn transaction_rolls_back_changes() {
    let mut backend = vec![];
    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
        db.execute(|tx| {
            tx.take_list::<u32>("ll")?;
            Ok(())
        })
        .unwrap();
    }
    let starting_size = backend.len();

    {
        let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
        let ll: LinkedList<u32> = db.get_list("ll").unwrap();

        assert!(db
            .execute(|tx| {
                let ll = ll.api(tx);
                ll.push(&42)?;
                assert_eq!(ll.head()?, Some(42));
                ll.push(&84)?;
                assert_eq!(ll.head()?, Some(84));
                Err::<(), _>(anyhow::anyhow!("error to roll back"))
            })
            .is_err());

        assert_eq!(db.execute(|tx| ll.api(tx).head()).unwrap(), None);
    }

    assert_eq!(backend.len(), starting_size);
}

#[test]
fn ll_pop_truncates_backend() {
    let mut backend = vec![];
    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
        db.execute(|tx| {
            let _ = tx.take_list::<u32>("ll")?;
            Ok(())
        })
        .unwrap();
    }

    let len_at_start = backend.len();

    {
        let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
        let ll: LinkedList<u32> = db.get_list("ll").unwrap();

        db.execute(|tx| {
            let ll = ll.api(tx);
            ll.push(&1)?;
            ll.push(&2)?;
            ll.push(&3)?;
            ll.push(&4)?;
            assert_eq!(ll.pop()?, Some(4));
            assert_eq!(ll.head()?, Some(3));
            Ok(())
        })
        .unwrap();

        db.execute(|tx| {
            assert_eq!(ll.api(tx).head()?, Some(3));
            Ok(())
        })
        .unwrap();
    }

    assert_eq!(backend.len(), len_at_start + 3 * 2);

    let len_before_pop = backend.len();

    {
        let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
        let ll: LinkedList<u32> = db.get_list("ll").unwrap();

        db.execute(|tx| {
            assert_eq!(ll.api(tx).pop()?, Some(3));
            Ok(())
        })
        .unwrap();
    }
    let len_after_pop = backend.len();
    assert_eq!(len_before_pop - 1 * 2, len_after_pop);

    let len_before_pop = backend.len();

    {
        let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
        let ll: LinkedList<u32> = db.get_list("ll").unwrap();

        db.execute(|tx| {
            let ll = ll.api(tx);
            assert_eq!(ll.pop()?, Some(2));
            assert_eq!(ll.pop()?, Some(1));
            assert_eq!(ll.pop()?, None);
            Ok(())
        })
        .unwrap();
    }

    let len_after_pop = backend.len();
    assert_eq!(len_before_pop - 2 * 2, len_after_pop);
    assert_eq!(len_at_start, len_after_pop);
}

#[test]
fn ll_push_after_pop_reclaims_space() {
    let mut backend = vec![];
    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
        db.execute(|tx| {
            let _: LinkedList<u32> = tx.take_list("ll")?;
            Ok(())
        })
        .unwrap();
    }
    let len_at_start = backend.len();

    let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
    let ll: LinkedList<u32> = db.get_list("ll").unwrap();

    db.execute(|tx| {
        let ll = ll.api(tx);
        ll.push(&1)?;
        ll.pop()?;
        Ok(())
    })
    .unwrap();

    assert_eq!(db.backend().get_ref().len(), len_at_start);

    db.execute(|tx| ll.api(tx).push(&2)).unwrap();

    assert_eq!(len_at_start, backend.len() - 1 * 2);
}

#[test]
fn ll_push_after_pop_reclaims_space_after_load() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
    let (ll1, _ll2) = db
        .execute(|tx| {
            let ll1: LinkedList<u32> = tx.take_list("ll1")?;
            let ll2: LinkedList<u32> = tx.take_list("ll2")?;
            ll1.api(&tx).push(&1)?;
            ll2.api(&tx).push(&1)?;
            Ok((ll1, ll2))
        })
        .unwrap();

    db.execute(|tx| {
        ll1.api(tx).pop()?;
        Ok(())
    })
    .unwrap();

    let len_before_push = db.backend().get_ref().len();
    let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
    db.execute(|tx| ll1.api(tx).push(&2)).unwrap();

    assert_eq!(
        len_before_push,
        backend.len(),
        "should have consumed free space"
    );
}

#[test]
fn load_after_alloc_then_alloc_again() {
    let mut backend = vec![];
    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
        db.execute(|tx| {
            let ll1: LinkedList<u32> = tx.take_list("ll1")?;
            ll1.api(tx).push(&42)
        })
        .unwrap();
    }

    {
        let mut db = LlsDb::load(Cursor::new(&mut backend)).unwrap();
        db.execute(|tx| {
            let ll1 = tx.take_list::<u32>("ll1").unwrap();
            let ll2 = tx.take_list::<u32>("ll2").unwrap();
            let ll1 = ll1.api(&tx);
            let ll2 = ll2.api(&tx);
            ll2.push(&84).unwrap();
            assert_eq!(ll1.head()?, Some(42));
            assert_eq!(ll2.head()?, Some(84));
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn push_result_of_pop() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
    let (ll1, ll2) = db
        .execute(|tx| {
            let ll1 = tx.take_list::<u32>("ll1")?;
            let ll2 = tx.take_list::<u32>("ll2")?;
            ll1.api(tx).push(&42)?;
            Ok((ll1, ll2))
        })
        .unwrap();

    db.execute(|tx| {
        let ll1 = ll1.api(&tx);
        let ll2 = ll2.api(&tx);
        ll2.push(&ll1.pop()?.unwrap())?;
        assert_eq!(ll1.head()?, None);
        assert_eq!(ll2.head()?, Some(42));
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        assert_eq!(ll1.api(&tx).head()?, None);
        assert_eq!(ll2.api(&tx).head()?, Some(42));
        Ok(())
    })
    .unwrap();
}
