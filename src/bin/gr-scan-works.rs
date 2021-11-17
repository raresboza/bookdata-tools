use std::path::PathBuf;

use serde::Deserialize;
use chrono::NaiveDate;

use bookdata::prelude::*;
use bookdata::arrow::*;
use bookdata::parsing::*;

/// Scan GoodReads book info into Parquet
#[derive(StructOpt)]
#[structopt(name="scan-book-info")]
pub struct ScanInteractions {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf
}

// the records we read from JSON
#[derive(Deserialize)]
struct RawWork {
  work_id: String,
  #[serde(default)]
  original_title: String,
  #[serde(default)]
  original_publication_year: String,
  #[serde(default)]
  original_publication_month: String,
  #[serde(default)]
  original_publication_day: String,
}

// work info records to actually write
#[derive(TableRow)]
struct InfoRecord {
  work_id: i32,
  title: Option<String>,
  pub_year: Option<i16>,
  pub_month: Option<u8>,
  pub_date: Option<NaiveDate>
}

fn main() -> Result<()> {
  let options = ScanInteractions::from_args();
  options.common.init()?;

  info!("reading books from {}", &options.infile.display());
  let proc = LineProcessor::open_gzip(&options.infile)?;

  let mut info_out = TableWriter::open("gr-work-info.parquet")?;

  for rec in proc.json_records() {
    let row: RawWork = rec?;
    let work_id: i32 = row.work_id.parse()?;

    let pub_year = parse_opt(&row.original_publication_year)?;
    let pub_month = parse_opt(&row.original_publication_month)?;
    let pub_day: Option<u32> = parse_opt(&row.original_publication_day)?;
    let pub_date = maybe_date(pub_year, pub_month, pub_day);

    info_out.write_object(InfoRecord {
      work_id,
      title: trim_owned(&row.original_title),
      pub_year, pub_month, pub_date
    })?;
  }

  let nlines = info_out.finish()?;
  info!("wrote {} work info records", nlines);

  Ok(())
}