use clap::Args;

use crate::{
    arrow::{polars::nonnull_schema, writer::open_polars_writer},
    prelude::*,
};
use polars::prelude::*;

static ALL_ISBNS_FILE: &str = "book-links/all-isbns.parquet";

/// Link records to ISBN IDs.
#[derive(Debug, Args)]
#[command(name = "link-isbn-ids")]
pub struct LinkISBNIds {
    /// Read record IDs from RECFLD.
    #[arg(
        short = 'R',
        long = "record-id",
        name = "RECFLD",
        default_value = "rec_id"
    )]
    rec_field: String,

    /// Read ISBNs from FIELD.
    #[arg(
        short = 'I',
        long = "isbn-field",
        name = "FIELD",
        default_value = "isbn"
    )]
    isbn_fields: Vec<String>,

    /// Write output to FILE.
    #[arg(short = 'o', long = "output", name = "FILE")]
    outfile: PathBuf,

    /// Read records from INPUT.
    #[arg(name = "INFILE")]
    infile: PathBuf,
}

impl Command for LinkISBNIds {
    fn exec(&self) -> Result<()> {
        info!("record field: {}", &self.rec_field);
        info!("ISBN fields: {:?}", &self.isbn_fields);

        let isbns = LazyFrame::scan_parquet(ALL_ISBNS_FILE, default())?;
        let records = LazyFrame::scan_parquet(&self.infile, default())?;

        let merged = if self.isbn_fields.len() == 1 {
            // one column, join on it
            records.join(
                isbns,
                &[col(self.isbn_fields[0].as_str())],
                &[col("isbn")],
                JoinType::Inner.into(),
            )
        } else {
            let mut melt = MeltArgs::default();
            melt.id_vars.push((&self.rec_field).into());
            for fld in &self.isbn_fields {
                melt.value_vars.push(fld.into());
            }
            melt.value_name = Some("isbn".into());
            melt.variable_name = Some("field".into());
            let rm = records.melt(melt);
            rm.join(
                isbns,
                &[col("isbn")],
                &[col("isbn")],
                JoinType::Inner.into(),
            )
        };
        let filtered = merged
            .filter(col("isbn").is_not_null())
            .select(&[col(self.rec_field.as_str()), col("isbn_id")])
            .unique(None, UniqueKeepStrategy::First)
            .sort(self.rec_field.as_str(), default());

        info!("collecting results");
        let frame = filtered.collect()?;
        if frame.column(&self.rec_field)?.null_count() > 0 {
            error!("final frame has null record IDs");
            return Err(anyhow!("data check failed"));
        }
        if frame.column("isbn_id")?.null_count() > 0 {
            error!("final frame has null ISBN IDs");
            return Err(anyhow!("data check failed"));
        }

        info!("saving {} links to {:?}", frame.height(), &self.outfile);
        let schema = nonnull_schema(&frame);
        let writer = open_polars_writer(&self.outfile, schema)?;
        writer.write_and_finish(frame.iter_chunks())?;

        Ok(())
    }
}
