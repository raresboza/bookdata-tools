//! Extract author information for book clusters.
use std::path::PathBuf;

use parse_display::{Display, FromStr};

use crate::arrow::dfext::*;
use crate::arrow::polars::nonnull_schema;
use crate::arrow::writer::open_polars_writer;
use crate::io::object::ThreadObjectWriter;
use crate::prelude::*;
use crate::util::logging::data_progress;
use anyhow::Result;
use polars::prelude::*;

#[derive(Display, FromStr, Debug, Clone)]
#[display(style = "lowercase")]
enum Source {
    OpenLib,
    LOC,
}

#[derive(Args, Debug)]
#[command(name = "extract-authors")]
/// Extract cluster author data from extracted book data.
pub struct ClusterAuthors {
    /// Only extract first authors
    #[arg(long = "first-author")]
    first_author: bool,

    /// Specify output file
    #[arg(short = 'o', long = "output")]
    output: PathBuf,

    /// Specify the source
    #[arg(short = 's', long = "source")]
    sources: Vec<Source>,
}

/// Scan the OpenLibrary data for authors.
fn scan_openlib(first_only: bool) -> Result<LazyFrame> {
    info!("scanning OpenLibrary author data");
    info!("reading ISBN clusters");
    let icl = LazyFrame::scan_parquet("book-links/isbn-clusters.parquet", default())?;
    let icl = icl.select(&[col("isbn_id"), col("cluster")]);
    info!("reading edition IDs");
    let edl = LazyFrame::scan_parquet("openlibrary/edition-isbn-ids.parquet", default())?;
    let edl = edl.filter(col("isbn_id").is_not_null());
    info!("reading edition authors");
    let mut eau = LazyFrame::scan_parquet("openlibrary/edition-authors.parquet", default())?;
    if first_only {
        eau = eau.filter(col("pos").eq(0i16));
    }

    info!("reading author names");
    let auth = LazyFrame::scan_parquet("openlibrary/author-names.parquet", default())?;
    let linked = icl.join(
        edl,
        [col("isbn_id")],
        [col("isbn_id")],
        JoinType::Inner.into(),
    );
    let linked = linked.join(
        eau,
        [col("edition")],
        [col("edition")],
        JoinType::Inner.into(),
    );
    let linked = linked.join(auth, [col("author")], [col("id")], JoinType::Inner.into());
    let authors = linked.select(vec![
        col("cluster"),
        col("name")
            .alias("author_name")
            .map(udf_clean_name, GetOutput::from_type(DataType::Utf8)),
    ]);

    Ok(authors)
}

/// Scan the Library of Congress data for first authors.
fn scan_loc(first_only: bool) -> Result<LazyFrame> {
    if !first_only {
        error!("only first-author extraction is currently supported");
        return Err(anyhow!("cannot extract multiple authors"));
    }

    info!("reading ISBN clusters");
    let icl = LazyFrame::scan_parquet("book-links/isbn-clusters.parquet", default())?;
    let icl = icl.select([col("isbn_id"), col("cluster")]);

    info!("reading book records");
    let books = LazyFrame::scan_parquet("loc-mds/book-isbn-ids.parquet", default())?;

    info!("reading book authors");
    let authors = LazyFrame::scan_parquet("loc-mds/book-authors.parquet", default())?;
    let authors = authors.filter(col("author_name").is_not_null());

    let linked = icl.join(
        books,
        [col("isbn_id")],
        [col("isbn_id")],
        JoinType::Inner.into(),
    );
    let linked = linked.join(
        authors,
        [col("rec_id")],
        [col("rec_id")],
        JoinType::Inner.into(),
    );
    let authors = linked.select(vec![
        col("cluster"),
        col("author_name").map(udf_clean_name, GetOutput::from_type(DataType::Utf8)),
    ]);

    Ok(authors)
}

impl Command for ClusterAuthors {
    fn exec(&self) -> Result<()> {
        let mut authors: Option<LazyFrame> = None;
        for source in &self.sources {
            let astr = match source {
                Source::OpenLib => scan_openlib(self.first_author)?,
                Source::LOC => scan_loc(self.first_author)?,
            };
            debug!("author source {} has schema {:?}", source, astr.schema());
            if let Some(adf) = authors {
                authors = Some(concat(
                    [adf, astr],
                    UnionArgs {
                        parallel: true,
                        rechunk: false,
                        to_supertypes: false,
                    },
                )?);
            } else {
                authors = Some(astr);
            }
        }
        let authors = authors.ok_or(anyhow!("no sources specified"))?;
        let authors = authors.filter(
            col("author_name")
                .is_not_null()
                .and(col("author_name").neq("".lit())),
        );

        let authors = authors.unique(None, UniqueKeepStrategy::First);

        debug!("plan: {}", authors.describe_plan());

        info!("collecting results");
        let mut authors = authors.collect()?;
        authors.as_single_chunk_par();
        info!("found {} cluster-author links", authors.height());

        info!("saving to {:?}", &self.output);
        // clean up nullability
        // we do the writing ourself because we have no nulls, but polars doesn't deal with that
        let schema = nonnull_schema(&authors);
        debug!("schema: {:?}", schema);

        let writer = open_polars_writer(&self.output, schema)?;
        let mut writer = ThreadObjectWriter::new(writer);
        let pb = data_progress(authors.n_chunks());
        for chunk in authors.iter_chunks() {
            writer.write_object(chunk)?;
            pb.tick();
        }
        writer.finish()?;
        std::mem::drop(pb);

        info!(
            "output file is {}",
            friendly::bytes(file_size(&self.output)?)
        );

        Ok(())
    }
}
