//! Support for loading author info.
use std::collections::HashMap;

use parquet_derive::ParquetRecordReader;

use crate::arrow::*;
use crate::language::*;
use crate::prelude::*;
use crate::util::logging::item_progress;

#[derive(Debug, Default)]
pub struct AuthorInfoLang {
    pub n_author_recs: u32,
    pub languages: LanguageBag,
}

pub type AuthorLangTable = HashMap<String, AuthorInfoLang>;

#[derive(Debug, ParquetRecordReader)]
struct LanguageRow {
    rec_id: u32,
    language: String,
}

#[derive(Debug, ParquetRecordReader)]
struct NameRow {
    rec_id: u32,
    name: String,
}

/// Load VIAF author names.
fn viaf_load_names() -> Result<HashMap<u32, Vec<String>>> {
    let mut map: HashMap<u32, Vec<String>> = HashMap::new();

    info!("loading VIAF author names");
    let iter = scan_parquet_file("viaf/author-name-index.parquet")?;

    let pb = item_progress(iter.remaining() as u64, "authors");
    let iter = pb.wrap_iter(iter);
    let timer = Timer::new();

    for row in iter {
        let row: NameRow = row?;
        map.entry(row.rec_id).or_default().push(row.name);
    }

    info!(
        "loaded authors for {} records in {}",
        map.len(),
        timer.human_elapsed()
    );

    Ok(map)
}


/// Load VIAF author languages
fn viaf_load_languages() -> Result<HashMap<u32, LanguageBag>> {
    let mut map: HashMap<u32, LanguageBag> = HashMap::new();
    let timer = Timer::new();

    info!("loading VIAF author languages");
    let iter = scan_parquet_file("viaf/author-language.parquet")?;

    let pb = item_progress(iter.remaining(), "authors");
    let iter = pb.wrap_iter(iter);

    for row in iter {
        let row: LanguageRow = row?;
        let language: Language = row.language.into();
        map.entry(row.rec_id).or_default().add(language);
    }

    info!(
        "loaded languages for {} records in {}",
        map.len(),
        timer.human_elapsed()
    );

    Ok(map)
}

/// Load the VIAF author language records.
#[inline(never)]
pub fn viaf_author_lang_table() -> Result<AuthorLangTable> {
    let mut table = AuthorLangTable::new();

    let rec_names = viaf_load_names()?;
    let rec_languages = viaf_load_languages()?;

    info!("merging gender records");
    let pb = item_progress(rec_names.len() as u64, "clusters");
    let timer = Timer::new();
    for (rec_id, names) in pb.wrap_iter(rec_names.into_iter()) {
        let languages = rec_languages.get(&rec_id);
        for name in names {
            let rec = table.entry(name).or_default();
            rec.n_author_recs += 1;
            if let Some(bag) = languages {
                rec.languages.merge_from(bag);
            }
        }
    }

    info!("merged {} gender records in {}", table.len(), timer);

    Ok(table)
}
