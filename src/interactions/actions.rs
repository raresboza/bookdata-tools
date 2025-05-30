use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::take;
use std::path::Path;

use anyhow::Result;
use log::*;
use parquet::record::RecordWriter;
use parquet_derive::ParquetRecordWriter;

use super::{Dedup, Interaction, Key};
use crate::arrow::*;
use crate::io::{file_size, ObjectWriter};
use crate::util::logging::item_progress;
use crate::util::Timer;

/// Record for a single output action.
#[derive(ParquetRecordWriter, Debug)]
pub struct TimestampActionRecord {
    pub user: i32,
    pub item: i32,
    pub first_time: i64,
    pub last_time: i64,
    pub last_rating: Option<f32>,
    pub nactions: i32,
}

/// Record for a single output action without time.
#[derive(ParquetRecordWriter, Debug)]
pub struct TimelessActionRecord {
    pub user: i32,
    pub item: i32,
    pub nactions: i32,
}

#[derive(PartialEq, Clone, Debug)]
pub struct ActionInstance {
    timestamp: i64,
    rating: Option<f32>,
}

/// Collapse a sequence of actions into an action record.
pub trait FromActionSet {
    fn create(user: i32, item: i32, actions: Vec<ActionInstance>) -> Self;
}

impl FromActionSet for TimestampActionRecord {
    fn create(user: i32, item: i32, actions: Vec<ActionInstance>) -> Self {
        let mut vec = actions;
        if vec.len() == 1 {
            // fast path
            let act = &vec[0];
            TimestampActionRecord {
                user,
                item,
                first_time: act.timestamp,
                last_time: act.timestamp,
                last_rating: act.rating,
                nactions: 1,
            }
        } else {
            vec.sort_unstable_by_key(|a| a.timestamp);
            let first = &vec[0];
            let last = &vec[vec.len() - 1];
            let rates = vec.iter().flat_map(|a| a.rating).collect::<Vec<f32>>();
            let last_rating = if rates.len() > 0 {
                Some(rates[rates.len() - 1])
            } else {
                None
            };

            TimestampActionRecord {
                user,
                item,
                first_time: first.timestamp,
                last_time: last.timestamp,
                last_rating,
                nactions: vec.len() as i32,
            }
        }
    }
}

impl FromActionSet for TimelessActionRecord {
    fn create(user: i32, item: i32, actions: Vec<ActionInstance>) -> Self {
        TimelessActionRecord {
            user,
            item,
            nactions: actions.len() as i32,
        }
    }
}

/// Action deduplicator.
pub struct ActionDedup<R>
where
    R: FromActionSet,
    for<'a> &'a [R]: RecordWriter<R>,
{
    _phantom: PhantomData<R>,
    table: HashMap<Key, Vec<ActionInstance>>,
}

impl<R> Default for ActionDedup<R>
where
    R: FromActionSet + 'static,
    for<'a> &'a [R]: RecordWriter<R>,
{
    fn default() -> ActionDedup<R> {
        ActionDedup {
            _phantom: PhantomData,
            table: HashMap::new(),
        }
    }
}

impl<I: Interaction, R> Dedup<I> for ActionDedup<R>
where
    R: FromActionSet + Send + Sync + 'static,
    for<'a> &'a [R]: RecordWriter<R>,
{
    fn add_interaction(&mut self, act: I) -> Result<()> {
        self.record(
            act.get_user(),
            act.get_item(),
            act.get_timestamp(),
            act.get_rating(),
        );
        Ok(())
    }

    fn save(&mut self, path: &Path) -> Result<usize> {
        self.write_actions(path)
    }
}

impl<R> ActionDedup<R>
where
    R: FromActionSet + Send + Sync + 'static,
    for<'a> &'a [R]: RecordWriter<R>,
{
    /// Add an action to the deduplicator.
    pub fn record(&mut self, user: i32, item: i32, timestamp: i64, rating: Option<f32>) {
        let k = Key::new(user, item);
        // get the vector for this user/item pair
        let vec = self.table.entry(k).or_insert_with(|| Vec::with_capacity(1));
        // and insert our records!
        vec.push(ActionInstance { timestamp, rating });
    }

    /// Save the rating table disk.
    pub fn write_actions<P: AsRef<Path>>(&mut self, path: P) -> Result<usize> {
        let path = path.as_ref();
        info!(
            "writing {} deduplicated actions to {}",
            friendly::scalar(self.table.len()),
            path.display()
        );
        let mut writer = TableWriter::open(path)?;
        let timer = Timer::new();
        let n = self.table.len() as u64;
        let pb = item_progress(n, "writing actions");

        // we're going to consume the hashtable.
        let table = take(&mut self.table);
        for (k, vec) in pb.wrap_iter(table.into_iter()) {
            let record = R::create(k.user, k.item, vec);
            writer.write_object(record)?;
        }

        let rv = writer.finish()?;

        info!(
            "wrote {} actions in {}, file is {}",
            friendly::scalar(n),
            timer.human_elapsed(),
            friendly::bytes(file_size(path)?)
        );

        Ok(rv)
    }
}
