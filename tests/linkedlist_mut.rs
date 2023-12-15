use llsdb::{LinkedListMut, LlsDb};
use std::io::Cursor;

#[test]
fn linked_list_mut_remove_in_same_tx_middle() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

    let ll1 = db
        .execute(|tx| {
            let ll1 = LinkedListMut(tx.take_list("ll1").unwrap());
            let api = ll1.api(tx);
            api.push(50)?;
            let remove_handle = api.push(60)?;
            api.push(70)?;
            api.unlink(remove_handle)?;
            assert_eq!(api.iter().collect::<Result<Vec<_>, _>>()?, vec![70, 50]);
            Ok(ll1)
        })
        .unwrap();

    db.execute(|tx| {
        assert_eq!(
            ll1.api(tx).iter().collect::<Result<Vec<_>, _>>()?,
            vec![70, 50]
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn linked_list_mut_remove_middle() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

    let (ll1, handle) = db
        .execute(|tx| {
            let ll1 = LinkedListMut(tx.take_list("ll1").unwrap());
            let api = ll1.api(tx);
            api.push(50)?;
            let remove_handle = api.push(60)?;
            api.push(70)?;
            Ok((ll1, remove_handle))
        })
        .unwrap();

    db.execute(|tx| {
        ll1.api(tx).unlink(handle)?;
        Ok(())
    })
    .unwrap();

    let len_before_push = db.backend().get_ref().len();

    db.execute(|tx| {
        let api = ll1.api(tx);
        assert_eq!(api.iter().collect::<Result<Vec<_>, _>>()?, vec![70, 50]);
        let new_handle = api.push(60)?;
        assert_eq!(
            new_handle.value_pointer(),
            handle.value_pointer(),
            "inserted in the old one's spot"
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(
        backend.len(),
        len_before_push,
        "the new push should have gone into the spot left by the old one"
    );
}

#[test]
fn linked_list_mut_remove_start() {
    let mut backend = vec![];
    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

        let ll1 = db
            .execute(|tx| {
                let ll1: LinkedListMut<u32> = LinkedListMut(tx.take_list("ll1").unwrap());
                let api = ll1.api(tx);
                api.push(50)?;
                api.push(60)?;
                api.push(70)?;
                Ok(ll1)
            })
            .unwrap();

        db.execute(|tx| {
            let api = ll1.api(tx);
            let (handle, _) = api
                .iter_handles()
                .map(Result::unwrap)
                .find(|(_, value)| *value == 50)
                .unwrap();
            api.unlink(handle)?;
            assert_eq!(api.iter().collect::<Result<Vec<_>, _>>()?, vec![70, 60]);
            Ok(())
        })
        .unwrap();
    }
}
