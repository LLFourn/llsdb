use llsdb::{index::Cell, LlsDb};
use std::io::Cursor;

#[test]
fn cell_get_replace() {
    let mut backend = vec![];

    let mut db = LlsDb::init(Cursor::new(&mut backend)).unwrap();
    let cell = db
        .execute(|tx| {
            let list = tx.take_list("cell")?;
            let cell = Cell::new_with_initial_value(list, &42, tx)?;
            Ok(tx.store_index(cell))
        })
        .unwrap();

    db.execute(|tx| {
        let cell = tx.take_index(cell);
        assert_eq!(cell.get()?, 42);
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        let cell = tx.take_index(cell);
        assert_eq!(cell.replace(&43)?, 42);
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        let cell = tx.take_index(cell);
        assert_eq!(cell.replace(&44)?, 43);
        assert_eq!(cell.replace(&84)?, 44);
        Ok(())
    })
    .unwrap();

    db.execute(|tx| {
        let cell = tx.take_index(cell);
        assert_eq!(cell.get()?, 84);
        Ok(())
    })
    .unwrap();
}
