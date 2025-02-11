//! Summarize author gender information for clusters.
//!
//! This script reads the cluster author information and author gender
//! information, in order to aggregate author genders for each cluster.
//!
//! We use a lot of left joins so that we can compute statistics across
//! the integration pipeline.
use std::path::{Path, PathBuf};

use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

use crate::arrow::*;
use crate::ids::codes::*;
use crate::prelude::*;

mod authors;
mod clusters;

#[derive(Args, Debug)]
#[command(name = "extract-author-languages")]
/// Extract cluster author gender data from extracted book data.
pub struct AuthorLanguage {
    /// Specify output file
    #[arg(short = 'o', long = "output")]
    output: PathBuf,

    /// Specify the cluster-author file.
    #[arg(short = 'A', long = "cluster-authors")]
    author_file: PathBuf,
}

/// Record format for saving gender information.
#[derive(Serialize, Deserialize, Clone, ParquetRecordWriter)]
struct ClusterLanguageInfo {
    cluster: i32,
    language: String,
}

fn save_languages(clusters: Vec<i32>, languages: clusters::ClusterLangTable, outf: &Path) -> Result<()> {
    info!("writing cluster languages to {}", outf.display());
    let mut out = TableWriter::open(outf)?;

    for cluster in clusters {
        let mut language = "no-book-author".to_owned();
        if NS_ISBN.from_code(cluster).is_some() {
            language = "no-book".to_owned();
        }
        if let Some(stats) = languages.get(&cluster) {
            if stats.n_book_authors == 0 {
                assert!(stats.languages.is_empty());
                language = "no-book-author".to_owned() // shouldn't happen but 🤷‍♀️
            } else if stats.n_author_recs == 0 {
                assert!(stats.languages.is_empty());
                language = "no-author-rec".to_owned()
            } else if stats.languages.is_empty() {
                language = "no-language".to_owned()
            } else {
                language = stats.languages.to_language().to_string()
            };
        }
        out.write_object(ClusterLanguageInfo { cluster, language })?;
    }

    out.finish()?;

    Ok(())
}

impl Command for AuthorLanguage {
    fn exec(&self) -> Result<()> {
        let clusters = clusters::all_clusters("book-links/cluster-stats.parquet")?;
        let name_languages = authors::viaf_author_lang_table()?;
        let cluster_languages = clusters::read_resolve(&self.author_file, &name_languages)?;
        save_languages(clusters, cluster_languages, self.output.as_ref())?;

        Ok(())
    }
}
