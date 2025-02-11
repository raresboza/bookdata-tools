//! Summarize author gender information for clusters.
//!
//! This script reads the cluster author information and author gender
//! information, in order to aggregate author genders for each cluster.
//!
//! We use a lot of left joins so that we can compute statistics across
//! the integration pipeline.
use std::path::{Path, PathBuf};
use std::collections::HashMap;

use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

use crate::arrow::*;
use crate::ids::codes::*;
use crate::prelude::*;

mod clusters;

#[derive(Args, Debug)]
#[command(name = "extract-cluster-ol-languages")]
/// Extract cluster ol language data from extracted book data.
pub struct ClusterOLLanguage {
    /// Specify output file
    #[arg(short = 'o', long = "output")]
    output: PathBuf,
    /// Specify the cluster-author file.
    #[arg(short = 'A', long = "cluster-ol-lang")]
    cluster_ol_lang_file: PathBuf,
}

#[derive(Args, Debug)]
#[command(name = "extract-cluster-loc-translation")]
/// Extract cluster loc translation data from extracted book data.
pub struct ClusterLocTranslation {
    /// Specify output file
    #[arg(short = 'o', long = "output")]
    output: PathBuf,
}

#[derive(Args, Debug)]
#[command(name = "extract-cluster-deduce-language")]
pub struct ClusterDeduceLanguage {
    /// Specify output file
    #[arg(short = 'o', long = "output")]
    output: PathBuf,
}


/// Record format for saving cluster ol information.
#[derive(Serialize, Deserialize, Clone, ParquetRecordWriter)]
struct ClusterLanguageInfo {
    cluster: i32,
    language: i32,
}

#[derive(Serialize, Deserialize, Clone, ParquetRecordWriter)]
struct ClusterTranslationInfo {
    cluster: i32,
    loc_original_language: String,
}

fn save_languages(cluster_languages: HashMap<u32, u32>, outf: &Path) -> Result<()> {
    info!("writing cluster languages to {}", outf.display());
    let mut out = TableWriter::open(outf)?;

    for (cluster, language) in cluster_languages {
        out.write_object(ClusterLanguageInfo { cluster: cluster.try_into().unwrap(), language: language.try_into().unwrap() })?;
    }

    out.finish()?;

    Ok(())
}

fn save_translations(cluster_translations: clusters::TranslationClusterTable, outf: &Path) -> Result<()> {
    info!("writing cluster translations to {}", outf.display());
    let mut out = TableWriter::open(outf)?;

    for (cluster, original_language) in cluster_translations {
        out.write_object(ClusterTranslationInfo { cluster: cluster.try_into().unwrap(), loc_original_language: original_language.try_into().unwrap() })?;
    }

    out.finish()?;

    Ok(())
}

#[derive(Serialize, Deserialize, Clone, ParquetRecordWriter)]
struct GRWorkLanguage {
    work_id: u32,
    language_loc: Option<String>,
    language_author: Option<String>,
    language_ol: Option<i32>,
}

fn save_merged_languages(work_merged_languages: HashMap<u32, clusters::WorkLanguageRow>, outf: &Path) -> Result<()> {
    info!("writing merged_languages to {}", outf.display());
    let mut out = TableWriter::open(outf)?;

    for (work_id, languages) in work_merged_languages {
        out.write_object(GRWorkLanguage { work_id: work_id.try_into().unwrap(), language_loc: languages.language_loc.try_into()?,
                                          language_author: languages.language_author.try_into()?, language_ol: languages.language_ol.try_into()?,
                                         })?;
    }
    out.finish()?;

    Ok(())
}

impl Command for ClusterOLLanguage {
    fn exec(&self) -> Result<()> {
        let cluster_languages = clusters::openlib_cluster_language(&self.cluster_ol_lang_file)?;
        save_languages(cluster_languages, self.output.as_ref())?;

        Ok(())
    }
}

impl Command for ClusterLocTranslation {
    fn exec(&self) -> Result<()> {
        let translation_clusters_map = clusters::loc_cluster_translation()?;
        save_translations(translation_clusters_map, self.output.as_ref())?;

        Ok(())
    }
}

impl Command for ClusterDeduceLanguage {
    fn exec(&self) -> Result<()> {
        let work_merged_languages = clusters::cluster_derive_language()?;
        save_merged_languages(work_merged_languages, self.output.as_ref())?;

        Ok(())
    }
}