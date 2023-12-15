use anyhow::anyhow;
use llsdb::{index::Vec, LlsDb};
use std::io::Cursor;

#[test]
fn vec_basic() {
    let mut backend = vec![];

    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

    let my_vec = db
        .execute(|tx| {
            let list = tx.take_list::<String>("vec")?;
            let vec_handle = tx.store_index(Vec::new(list, tx)?);
            let mut vec = tx.take_index(vec_handle);
            assert_eq!(vec.get(0)?, None);
            vec.push(&"hello".into())?;
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
        vec.push(&"world".into())?;
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
        vec.push(&"greetings".into())?;
        vec.push(&"earth".into())?;
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
        vec.push(&"world".into())?;
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
fn vec_pop() {
    let mut backend = vec![];
    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

    let handle = db
        .execute(|tx| {
            let list = tx.take_list::<String>("vec")?;
            let (handle, mut vec) = tx.store_and_take_index(Vec::new(list, tx)?);
            vec.push(&"one".into())?;
            vec.push(&"two".into())?;
            vec.push(&"three".into())?;
            vec.push(&"four".into())?;
            vec.pop()?;
            assert_eq!(
                vec.iter()
                    .collect::<Result<std::vec::Vec<_>, _>>()?
                    .join(" ")
                    .as_str(),
                "one two three"
            );
            Ok(handle)
        })
        .unwrap();

    let _let_it_fail = db.execute(|tx| {
        let mut vec = tx.take_index(handle);
        vec.pop()?;
        assert_eq!(
            vec.iter()
                .collect::<Result<std::vec::Vec<_>, _>>()?
                .join(" ")
                .as_str(),
            "one two"
        );

        Err::<(), _>(anyhow!("fail it"))
    });

    db.execute(|tx| {
        let vec = tx.take_index(handle);
        assert_eq!(
            vec.iter()
                .collect::<Result<std::vec::Vec<_>, _>>()?
                .join(" ")
                .as_str(),
            "one two three"
        );

        Ok(())
    })
    .unwrap();
}

#[test]
fn vec_load_index() {
    let mut backend = vec![];
    {
        let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();

        db.execute(|tx| {
            let list = tx.take_list::<String>("vec")?;
            let vec_handle = tx.store_index(Vec::new(list, tx)?);
            let mut vec = tx.take_index(vec_handle);
            vec.push(&"hello".into())?;
            vec.push(&"world".into())?;
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

    {
        let mut db = LlsDb::load_or_init(Cursor::new(&mut backend)).unwrap();

        db.execute(|tx| {
            let list = tx.take_list::<String>("vec")?;
            let vec_handle = tx.store_index(Vec::new(list, tx)?);
            let vec = tx.take_index(vec_handle);
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
}
