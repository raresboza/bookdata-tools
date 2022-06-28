//! Index names from authority records.
use std::collections::{HashSet, HashMap};
use std::path::{PathBuf, Path};
use std::fs::File;
use std::thread::{spawn, JoinHandle};
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};

use indicatif::{ProgressBar, ProgressStyle};
use structopt::StructOpt;
use csv;
use serde::{Serialize};
use flate2::write::GzEncoder;

use rayon::prelude::*;

use crate::marc::flat_fields::FieldRecord;
use crate::prelude::*;
use crate::arrow::*;
use crate::io::background::ThreadWrite;
use crate::io::object::ThreadObjectWriter;
use crate::cleaning::names::*;
use crate::util::logging::{set_progress, item_progress};

#[derive(StructOpt, Debug)]
#[structopt(name="index-names")]
/// Clean and index author names from authority records.
pub struct IndexNames {
  /// MARC authority field file to scan for names.
  #[structopt(long="marc-authorities", name = "FILE", parse(from_os_str))]
  marc_authorities: Option<PathBuf>,

  /// Index output Parquet file.
  #[structopt(name = "OUTFILE", parse(from_os_str))]
  outfile: PathBuf,
}

type NameIndex = HashMap<String,HashSet<u32>>;

#[derive(ArrowField, Serialize, Clone)]
struct IndexEntry {
  rec_id: u32,
  name: String,
}

fn scan_authority_names(path: &Path, send: SyncSender<(String, u32)>) -> Result<JoinHandle<Result<usize>>> {
  info!("reading names from authority fields in {:?}", path);
  let scanner = scan_parquet_file(path)?;

  Ok(spawn(move || {
    let scanner = scanner;
    let pb = item_progress(scanner.remaining() as u64, "fields");
    // pb.set_draw_delta(10000);
    let _lg = set_progress(pb.clone());
    let mut n = 0;
    for rec in pb.wrap_iter(scanner) {
      let rec: FieldRecord = rec?;
      if rec.tag == 700 && rec.sf_code == b'a' {
        send.send((rec.contents, rec.rec_id))?;
        n += 1;
      }
    }
    debug!("finished scanning parquet");
    Ok(n)
  }))
}

fn process_names(recv: Receiver<(String, u32)>) -> Result<NameIndex> {
  let mut index = NameIndex::new();

  // process results and add to list
  for (src, rec_id) in recv {
    for name in name_variants(&src)? {
      index.entry(name).or_default().insert(rec_id);
    }
  }

  info!("index {} names", index.len());
  Ok(index)
}

fn write_index(index: NameIndex, path: &Path) -> Result<()> {
  info!("sorting {} names", index.len());
  debug!("copying names");
  let mut names = Vec::with_capacity(index.len());
  names.extend(index.keys().map(|s| s.as_str()));
  debug!("sorting names");
  names.par_sort_unstable();

  info!("writing deduplicated names to {}", path.to_string_lossy());
  let mut writer = TableWriter::open(&path)?;

  let mut csv_fn = PathBuf::from(path);
  csv_fn.set_extension("csv.gz");
  info!("writing CSV version to {:?}", csv_fn);
  let out = File::create(&csv_fn)?;
  let out = GzEncoder::new(out, flate2::Compression::fast());
  let out = ThreadWrite::new(out)?;
  // let out = Encoder::new(out, 2)?.auto_finish();
  let csvw = csv::Writer::from_writer(out);
  let mut csvout = ThreadObjectWriter::new(csvw);

  let pb = ProgressBar::new(names.len() as u64);
  let pb = pb.with_style(ProgressStyle::default_bar().template("{prefix}: {bar} {pos}/{len} ({percent}%, ETA {eta})")?);
  let pb = pb.with_prefix("names");
  // pb.set_draw_delta(5000);
  let _lg = set_progress(pb.clone());

  for name in pb.wrap_iter(names.into_iter()) {
    let mut ids: Vec<u32> = index.get(name).unwrap().iter().map(|i| *i).collect();
    ids.sort_unstable();
    for rec_id in ids {
      let e = IndexEntry {
        rec_id,
        name: name.to_string()
      };
      csvout.write_object(e.clone())?;
      writer.write_object(e)?;
    }
  }

  writer.finish()?;
  csvout.finish()?;
  Ok(())
}

impl Command for IndexNames {
  fn exec(&self) -> Result<()> {
    let (send, recv) = sync_channel(4096);
    let h = if let Some(ref path) = self.marc_authorities {
      scan_authority_names(path.as_path(), send)?
    } else {
      return Err(anyhow!("no name source specified"))
    };

    let names = process_names(recv)?;
    let nr = h.join().expect("thread join error")?;
    info!("scanned {} name records", nr);

    write_index(names, &self.outfile)?;

    Ok(())
  }
}
