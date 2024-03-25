use rickdb::{db::DB, options::Options};
use std::io;

fn main() -> io::Result<()> {
    let mut db = DB::new("./test", Options::default()).unwrap();
    db.put(b"a".to_vec(), b"b".to_vec()).unwrap();
    drop(db);
    let db = DB::new("./test", Options::default()).unwrap();
    let v = db.get(b"a").unwrap();
    println!("value {:?}", v);
    Ok(())
}

#[test]
fn test() -> io::Result<()> {
    // Clean up any previous runs of this example.
    let path = std::path::Path::new("test");
    if path.exists() {
        std::fs::remove_dir_all("test")?;
    }
    main()
}
