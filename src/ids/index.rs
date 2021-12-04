//! Data structure for mapping string keys to numeric identifiers.
use std::path::{Path};
use std::sync::Arc;
use std::hash::Hash;
use std::borrow::Borrow;
use hashbrown::hash_map::{HashMap, Keys};

use log::*;
use anyhow::Result;
use parquet::record::reader::RowIter;
use parquet::record::RowAccessor;
use parquet::basic::{Type as PhysicalType, LogicalType, IntType, StringType, Repetition};
use parquet::schema::types::Type;
use crate::io::ObjectWriter;
use crate::arrow::*;

#[cfg(test)]
use quickcheck::{Arbitrary,Gen};
#[cfg(test)]
use tempfile::tempdir;

/// The type of index identifiers.
pub type Id = i32;

/// Index identifiers from a data type
pub struct IdIndex<K> {
  map: HashMap<K,Id>,
}

/// Internal struct for ID records.
#[derive(ParquetRecordWriter)]
struct IdRec {
  id: i32,
  key: String,
}

impl <K> IdIndex<K> where K: Eq + Hash {
  /// Create a new index.
  pub fn new() -> IdIndex<K> {
    IdIndex {
      map: HashMap::new()
    }
  }

  /// Get the index length
  pub fn len(&self) -> usize {
    self.map.len()
  }

  /// Get the ID for a key, adding it to the index if needed.
  pub fn intern<Q>(&mut self, key: &Q) -> Id where K: Borrow<Q>, Q: Hash + Eq + ToOwned<Owned=K> + ?Sized {
    let n = self.map.len() as Id;
    // use Hashbrown's raw-entry API to minimize cloning
    let eb = self.map.raw_entry_mut();
    let e = eb.from_key(key);
    let (_, v) = e.or_insert_with(|| {
      (key.to_owned(), n+1)
    });
    *v
  }

  /// Get the ID for a key, adding it to the index if needed and transferring ownership.
  pub fn intern_owned(&mut self, key: K) -> Id {
    let n = self.map.len() as Id;
    // use Hashbrown's raw-entry API to minimize cloning
    *self.map.entry(key).or_insert(n+1)
  }

  /// Look up the ID for a key if it is present.
  #[allow(dead_code)]
  pub fn lookup<Q>(&self, key: &Q) -> Option<Id> where K: Borrow<Q>, Q: Hash + Eq + ?Sized {
    self.map.get(key).map(|i| *i)
  }

  /// Iterate over keys (see [std::collections::HashMap::keys]).
  #[allow(dead_code)]
  pub fn keys(&self) -> Keys<'_, K, Id> {
    self.map.keys()
  }

  /// Get the keys in order.
  pub fn key_vec(&self) -> Vec<&K> {
    let mut vec = Vec::with_capacity(self.len());
    vec.resize(self.len(), None);
    for (k, n) in self.map.iter() {
      let i = (n - 1) as usize;
      assert!(vec[i].is_none());
      vec[i] = Some(k);
    }

    let vec = vec.iter().map(|ro| ro.unwrap()).collect();
    vec
  }
}

impl IdIndex<String> {
  /// Load from a Parquet file, with a standard configuration.
  ///
  /// This assumes the Parquet file has the following columns:
  ///
  /// - `key`, of type `String`, storing the keys
  /// - `id`, of type `u32`, storing the IDs
  pub fn load_standard<P: AsRef<Path>>(path: P) -> Result<IdIndex<String>> {
    IdIndex::load(path, "id", "key")
  }

  /// Load from a Parquet file.
  ///
  /// This loads two columns from a Parquet file.  The ID column is expected to
  /// have type `UInt32` (or a type projectable to it), and the key column should
  /// be `Utf8`.
  pub fn load<P: AsRef<Path>>(path: P, id_col: &str, key_col: &str) -> Result<IdIndex<String>> {
    let path_str = path.as_ref().to_string_lossy();
    info!("reading index from file {}", path_str);
    let read = open_parquet_file(path.as_ref())?;

    let file_schema = read.metadata().file_metadata().schema();
    debug!("file schema: {:?}", file_schema);

    let id_type = LogicalType::INTEGER(IntType::new(32, false));
    let key_type = LogicalType::STRING(StringType::new());
    let mut tgt_fields = vec![
      Arc::new(Type::primitive_type_builder(id_col, PhysicalType::INT32)
        .with_logical_type(Some(id_type))
        .with_repetition(Repetition::REQUIRED)
        .build()?),
      Arc::new(Type::primitive_type_builder(key_col, PhysicalType::BYTE_ARRAY)
        .with_logical_type(Some(key_type))
        .with_repetition(Repetition::REQUIRED)
        .build()?),
    ];
    let tgt_schema = Type::group_type_builder(file_schema.name()).with_fields(&mut tgt_fields).build()?;
    debug!("target schema: {:?}", tgt_schema);

    let read = RowIter::from_file(Some(tgt_schema), &read)?;
    let mut map = HashMap::new();

    debug!("reading file contents");
    for row in read {
      let id = row.get_int(0)?;
      let key = row.get_string(1)?;
      map.insert(key.clone(), id);
    }

    info!("read {} keys from {}", map.len(), path_str);

    Ok(IdIndex {
      map
    })
  }

  /// Save to a Parquet file with the standard configuration.
  pub fn save_standard<P: AsRef<Path>>(&self, path: P) -> Result<()> {
    self.save(path, "id", "key")
  }

  /// Save to a Parquet file with the standard configuration.
  pub fn save<P: AsRef<Path>>(&self, path: P, id_col: &str, key_col: &str) -> Result<()> {
    let mut wb = TableWriterBuilder::new()?;
    wb = wb.rename("id", id_col);
    wb = wb.rename("key", key_col);
    let mut writer = wb.open(path)?;

    for (k, v) in &self.map {
      writer.write_object(IdRec {
        id: *v,
        key: k.clone()
      })?;
    }

    writer.finish()?;

    Ok(())
  }
}


#[test]
fn test_index_empty() {
  let index: IdIndex<String> = IdIndex::new();
  assert_eq!(index.len(), 0);
  assert!(index.lookup("bob").is_none());
}


#[test]
fn test_index_intern_one() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche");
  assert_eq!(id, 1);
  assert_eq!(index.lookup("hackem muche").unwrap(), 1);
}


#[test]
fn test_index_intern_two() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche");
  assert_eq!(id, 1);
  let id2 = index.intern("readme");
  assert_eq!(id2, 2);
  assert_eq!(index.lookup("hackem muche").unwrap(), 1);
}


#[test]
fn test_index_intern_twice() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern("hackem muche");
  assert_eq!(id, 1);
  let id2 = index.intern("hackem muche");
  assert_eq!(id2, 1);
  assert_eq!(index.len(), 1);
}

#[test]
fn test_index_intern_twice_owned() {
  let mut index: IdIndex<String> = IdIndex::new();
  assert!(index.lookup("hackem muche").is_none());
  let id = index.intern_owned("hackem muche".to_owned());
  assert_eq!(id, 1);
  let id2 = index.intern_owned("hackem muche".to_owned());
  assert_eq!(id2, 1);
  assert_eq!(index.len(), 1);
}


#[test]
fn test_index_save() -> Result<()> {
  let mut index: IdIndex<String> = IdIndex::new();
  let mut gen = Gen::new(100);
  for _i in 0..10000 {
    let key = String::arbitrary(&mut gen);
    let prev = index.lookup(&key);
    let id = index.intern(&key);
    match prev {
      Some(i) => assert_eq!(id, i),
      None => assert_eq!(id as usize, index.len())
    };
  }

  let dir = tempdir()?;
  let pq = dir.path().join("index.parquet");
  index.save_standard(&pq).expect("save error");

  let i2 = IdIndex::load_standard(&pq).expect("load error");
  assert_eq!(i2.len(), index.len());
  for (k, v) in &index.map {
    let v2 = i2.lookup(k);
    assert!(v2.is_some());
    assert_eq!(v2.unwrap(), *v);
  }

  Ok(())
}
