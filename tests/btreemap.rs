use anyhow::{anyhow, Result};
use llsdb::{index::BTreeMap, LlsDb};
use std::io::Cursor;

#[test]
fn btreemap_basic() {
    let mut backend = vec![];

    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

    let map_handle = db
        .execute(|tx| {
            let list = tx.take_list::<(u32, String)>("btree")?;
            let map_handle = tx.store_index(BTreeMap::new(list, &tx)?);
            let mut map = tx.take_index(map_handle);
            map.insert(0, &"zero".into())?;
            map.insert(1, &"one".into())?;
            map.insert(3, &"three".into())?;
            map.insert(4, &"four".into())?;
            Ok(map_handle)
        })
        .unwrap();

    db.execute(|tx| {
        let map = tx.take_index(map_handle);
        assert_eq!(map.get(&0)?, Some("zero".to_string()));
        assert_eq!(
            map.range(1..4).collect::<Result<Vec<_>>>()?,
            vec![(1, "one".to_string()), (3, "three".to_string())]
        );
        Ok(())
    })
    .unwrap();

    let _it_should_fail = db.execute(|tx| {
        let mut map = tx.take_index(map_handle);
        map.insert(2, &"woops".into())?;
        map.insert(2, &"two".into())?;
        assert_eq!(map.get(&2)?, Some("two".into()));
        assert_eq!(
            map.range(1..4).collect::<Result<Vec<_>>>()?,
            vec![
                (1, "one".to_string()),
                (2, "two".to_string()),
                (3, "three".to_string())
            ]
        );
        Err::<(), _>(anyhow!("fail the tx"))
    });

    db.execute(|tx| {
        let mut map = tx.take_index(map_handle);
        assert_eq!(map.get(&0)?, Some("zero".to_string()));
        assert_eq!(
            map.range(1..4).collect::<Result<Vec<_>>>()?,
            vec![(1, "one".to_string()), (3, "three".to_string())]
        );
        map.insert(2, &"woops".into())?;
        map.insert(2, &"two".into())?;
        assert_eq!(
            map.range(1..4).collect::<Result<Vec<_>>>()?,
            vec![
                (1, "one".to_string()),
                (2, "two".to_string()),
                (3, "three".to_string())
            ]
        );
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        let map = tx.take_index(map_handle);
        assert_eq!(
            map.iter().collect::<Result<Vec<_>>>()?,
            vec![
                (0, "zero".to_string()),
                (1, "one".to_string()),
                (2, "two".to_string()),
                (3, "three".to_string()),
                (4, "four".to_string())
            ]
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn btreemap_overwriting_values() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

    let map_handle = db
        .execute(|tx| {
            let list = tx.take_list::<(u32, String)>("btree")?;
            let map_handle = tx.store_index(BTreeMap::new(list, &tx)?);
            let mut map = tx.take_index(map_handle);
            for i in 0..100 {
                map.insert(i, &"foo".into())?;
            }
            Ok(map_handle)
        })
        .unwrap();

    db.execute(|tx| {
        let mut map = tx.take_index(map_handle);
        for i in 0..100 {
            assert_eq!(map.insert(i, &i.to_string())?, Some("foo".to_string()));
        }
        Ok(())
    })
    .unwrap();

    let mut db = LlsDb::load_or_init(Cursor::new(&mut backend)).unwrap();
    db.execute(|tx| {
        let list = tx.take_list::<(u32, String)>("btree")?;
        let map_handle = tx.store_index(BTreeMap::new(list, &tx)?);
        let map = tx.take_index(map_handle);

        for i in 0..100 {
            assert_eq!(map.get(&i)?, Some(i.to_string()))
        }

        Ok(())
    })
    .unwrap();
}

#[test]
fn btreemap_repeated_identical_insert_doesnt_grow() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

    let map_handle = db
        .execute(|tx| {
            let list = tx.take_list::<(u32, String)>("btree")?;
            let map_handle = tx.store_index(BTreeMap::new(list, &tx)?);
            let mut map = tx.take_index(map_handle);
            for i in 0..100 {
                map.insert(i, &i.to_string())?;
            }
            Ok(map_handle)
        })
        .unwrap();

    let size_before_redundant_insert = db.backend().get_ref().len();

    db.execute(|tx| {
        let mut map = tx.take_index(map_handle);
        for i in 0..100 {
            let string = i.to_string();
            assert_eq!(map.insert(i, &string)?, Some(string));
        }
        Ok(())
    })
    .unwrap();

    assert_eq!(db.backend().get_ref().len(), size_before_redundant_insert);
}
