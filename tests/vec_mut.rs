use anyhow::anyhow;
use llsdb::{index::VecRemove, LlsDb, Mut};
use std::io::Cursor;

#[test]
fn vec_mut_basic() {
    let mut backend = vec![];

    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

    let my_vec = db
        .execute(|tx| {
            let list = tx.take_list::<Mut<String>>("vec")?;
            let vec_handle = tx.store_index(VecRemove::new(list, tx)?);
            let mut vec = tx.take_index(vec_handle);
            assert_eq!(vec.get(0)?, None);
            vec.push("hello".into())?;
            assert_eq!(vec.get(0)?, Some("hello".to_string()));
            Ok(vec_handle)
        })
        .unwrap();

    db.execute(|tx| {
        let vec = tx.take_index(my_vec);
        assert_eq!(vec.get(0)?, Some("hello".to_string()));
        Ok(())
    })
    .unwrap();

    let _it_should_fail = db.execute(|tx| {
        let mut vec = tx.take_index(my_vec);
        vec.push("world".into())?;
        assert_eq!(vec.get(0)?, Some("hello".to_string()));
        assert_eq!(vec.get(1)?, Some("world".to_string()));
        assert_eq!(
            vec.iter()
                .collect::<Result<std::vec::Vec<_>, _>>()?
                .join(" ")
                .as_str(),
            "hello world"
        );
        Err::<(), _>(anyhow!("fail the tx"))
    });

    db.execute(|tx| {
        let vec = tx.take_index(my_vec);
        assert_eq!(vec.get(0)?, Some("hello".to_string()));
        assert_eq!(vec.get(1)?, None);
        assert_eq!(
            vec.iter()
                .collect::<Result<std::vec::Vec<_>, _>>()?
                .join(" ")
                .as_str(),
            "hello"
        );
        Ok(())
    })
    .unwrap();

    let _it_should_fail = db.execute(|tx| {
        let mut vec = tx.take_index(my_vec);
        assert_eq!(vec.pop()?, Some("hello".to_string()));
        vec.push("greetings".into())?;
        vec.push("earth".into())?;
        assert_eq!(
            vec.iter()
                .collect::<Result<std::vec::Vec<_>, _>>()?
                .join(" ")
                .as_str(),
            "greetings earth"
        );
        Err::<(), _>(anyhow!("fail the tx"))
    });

    db.execute(|tx| {
        let vec = tx.take_index(my_vec);
        assert_eq!(vec.get(0)?, Some("hello".to_string()));
        assert_eq!(vec.get(1)?, None);
        assert_eq!(
            vec.iter()
                .collect::<Result<std::vec::Vec<_>, _>>()?
                .join(" ")
                .as_str(),
            "hello"
        );
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        let mut vec = tx.take_index(my_vec);
        vec.push("world".into())?;
        assert_eq!(
            vec.iter()
                .collect::<Result<std::vec::Vec<_>, _>>()?
                .join(" ")
                .as_str(),
            "hello world"
        );
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        let vec = tx.take_index(my_vec);
        assert_eq!(
            vec.iter()
                .collect::<Result<std::vec::Vec<_>, _>>()?
                .join(" ")
                .as_str(),
            "hello world"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
/// Removing the last element should converted to a simple pop
fn vec_mut_remove_last_elem() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
    let list = db
        .execute(|tx| {
            let list = tx.take_list::<Mut<u32>>("vec_mut")?;
            Ok(list)
        })
        .unwrap();

    let my_vec = db
        .execute(|tx| {
            let index_handle = tx.store_index(VecRemove::new(list, tx)?);
            let mut vec = tx.take_index(index_handle);
            vec.push(0)?;
            vec.push(1)?;
            vec.push(2)?;
            Ok(index_handle)
        })
        .unwrap();

    let len_before_remove = db.backend().get_ref().len();

    db.execute(|tx| {
        let mut vec = tx.take_index(my_vec);
        vec.remove(2)?;
        Ok(())
    })
    .unwrap();

    assert_eq!(backend.len(), len_before_remove - 3);
}

#[test]
fn vec_mut_retain_should_shrink_backend_if_you_remove_end_elements() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
    let list = db
        .execute(|tx| {
            let list = tx.take_list::<Mut<u32>>("vec_mut")?;
            Ok(list)
        })
        .unwrap();

    let my_vec = db
        .execute(|tx| {
            let index_handle = tx.store_index(VecRemove::new(list, tx)?);
            let mut vec = tx.take_index(index_handle);
            vec.push(0)?;
            vec.push(1)?;
            vec.push(2)?;
            vec.push(3)?;
            Ok(index_handle)
        })
        .unwrap();

    let len_before_retain = db.backend().get_ref().len();

    db.execute(|tx| {
        let mut vec = tx.take_index(my_vec);
        vec.retain(|i| i < 2)?;
        Ok(())
    })
    .unwrap();

    assert_eq!(backend.len(), len_before_retain - 2 * 3);
}

#[test]
fn vec_mut_retain() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
    let my_vec = db
        .execute(|tx| {
            let list = tx.take_list::<Mut<u32>>("vec_mut")?;
            let vec_handle = tx.store_index(VecRemove::new(list, tx)?);
            let mut vec = tx.take_index(vec_handle);
            for i in 0..=(1 << 6) {
                vec.push(i)?;
            }
            for i in vec.iter().map(Result::unwrap) {
                assert_eq!(vec.get(i as usize)?, Some(i));
            }
            assert_eq!(vec.len(), (1 << 6) + 1);
            Ok(vec_handle)
        })
        .unwrap();

    let _let_it_fail = db.execute(|tx| {
        let mut vec = tx.take_index(my_vec);
        vec.retain(|i| !i.is_power_of_two())?;
        assert_eq!(vec.len(), (1 << 6) + 1 - 7);

        for i in vec.iter().map(Result::unwrap) {
            assert!(!i.is_power_of_two());
        }
        Err::<(), _>(anyhow!("fail it"))
    });

    db.execute(|tx| {
        let vec = tx.take_index(my_vec);
        assert_eq!(vec.len(), (1 << 6) + 1);
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        let mut vec = tx.take_index(my_vec);
        vec.retain(|i| !i.is_power_of_two())?;
        assert_eq!(vec.len(), (1 << 6) + 1 - 7);

        for i in vec.iter() {
            assert!(!i.unwrap().is_power_of_two());
        }
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        let vec = tx.take_index(my_vec);
        for i in vec.iter() {
            assert!(!i.unwrap().is_power_of_two());
        }
        assert_eq!(vec.len(), (1 << 6) + 1 - 7);
        assert_eq!(vec.get(7)?, Some(7 + 4));
        assert_eq!(vec.get(8)?, Some(8 + 4));
        assert_eq!(vec.get(12)?, Some(12 + 5));
        Ok(())
    })
    .unwrap();
}
