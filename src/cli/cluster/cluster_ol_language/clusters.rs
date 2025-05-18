//! Support for loading author info.
use std::collections::HashMap;
use std::collections::HashSet;

use parquet_derive::ParquetRecordReader;
use polars::prelude::*;

use crate::arrow::*;
use crate::prelude::*;
use crate::util::logging::item_progress;

#[derive(Debug, ParquetRecordReader)]
struct ClusterRow {
    language_flag: u8,
    cluster: i32,
}

fn merge_languages(existing: u32, new: u32) -> u32 {
    match (existing, new) {
        (x, 0) | (0, x) => x,
        (2, _) | (_, 2) => 2, 
        (1, 3) | (3, 1) => 2,
        (x,y) if x == y => x,
        _ => existing,                // If no rule matches, keep the existing value
    }
}

/// Load OpenLib Cluster-Language_flag
/// First merge editions.parquet with edition-works.parquet
/// Afterwards merge the result with work-cluster.parquet
#[inline(never)]
pub fn openlib_cluster_language(path: &Path) -> Result<HashMap<u32, u32>> {
    let lf = LazyFrame::scan_parquet(path, Default::default())?;
    info!("schema {:?}", lf.schema()?);
    let df = lf.collect()?;
    let mut map: HashMap<u32, u32> = HashMap::new();
    let rows: Vec<ClusterRow> = df.column("language_flag").unwrap().u8().unwrap().into_iter()
    .zip(df.column("cluster").unwrap().i32().unwrap().into_iter())
    .map(|(language_flag, cluster)| {
        ClusterRow {
            language_flag: language_flag.unwrap(),
            cluster: cluster.unwrap(),
        }
    })
    .collect();
    
    info!("{}", rows.len());
    for row in rows {
        map
            .entry(row.cluster as u32)
            .and_modify(|existing_language| {
                // Merge with existing language
                *existing_language = merge_languages(*existing_language, row.language_flag as u32);
            })
            .or_insert(row.language_flag as u32); // Insert if not already in the map
    }
    info!("merged {} cluster-ol-lan records", map.len());

    Ok(map)
}

#[derive(Debug, Default)]
pub struct TranslationClusterRow {
    pub cluster: u32,
    pub original_language: Option<String>,
    pub translated_language: Option<String>,
}

pub type TranslationClusterTable = HashMap<u32, String>;

// Load Loc Translation Status
/// First merge 
/// Afterwards merge the result with work-cluster.parquet
#[inline(never)]
pub fn loc_cluster_translation() -> Result<TranslationClusterTable> {
    let translation_rec_lf = LazyFrame::scan_parquet("loc-mds/book-languages.parquet", Default::default())?;
    let translation_rec_df = translation_rec_lf.collect()?;

    let rec_ids_lf = LazyFrame::scan_parquet("loc-mds/book-isbn-ids.parquet", Default::default())?;
    let rec_ids_df = rec_ids_lf.collect()?;

    let mut map: TranslationClusterTable = HashMap::new();

    // Perform an inner join on the "rec_id" column
    info!("Perform an inner join on the rec_id column");
    let translation_ids_df = translation_rec_df.join(&rec_ids_df, ["rec_id"], ["rec_id"], JoinType::Inner.into())?;
    let translation_ids_df = translation_ids_df.drop("rec_id")?;

    let isbn_clusters_lf = LazyFrame::scan_parquet("book-links/isbn-clusters.parquet", Default::default())?;
    let isbn_clusters_df = isbn_clusters_lf.collect()?;

    // Perform inner join on the "isbn_id" column
    info!("Perform an inner join on the isbn_id column");
    let translation_clusters_df = translation_ids_df.join(&isbn_clusters_df, ["isbn_id"], ["isbn_id"], JoinType::Inner.into())?;
    let translation_clusters_df = translation_clusters_df.drop("isbn_id")?;
    let translation_clusters_df = translation_clusters_df.drop("isbn")?;

    // Extract columns and map to Vec<TranslationClusterRow>
    let rows: Vec<TranslationClusterRow> = translation_clusters_df.column("cluster")?.i32()?.into_iter()
        .zip(translation_clusters_df.column("original_language")?.str()?.into_iter())
        .zip(translation_clusters_df.column("translated_language")?.str()?.into_iter())
        .map(|((cluster, original_language), translated_language)| {
            TranslationClusterRow {
                cluster: cluster.unwrap() as u32, // Convert i32 to u32
                original_language: original_language.map(|s| s.to_string()), // Convert Option<&str> to Option<String>
                translated_language: translated_language.map(|s| s.to_string()), // Convert Option<&str> to Option<String>
            }
        })
        .collect();
    info!("length: {}", rows.len());
    let mut cluster_set = HashSet::new();
    let mut has_duplicates = false;
    for row in &rows {
        if !cluster_set.insert(row.cluster) {
            println!("Duplicate cluster found: {}", row.cluster);
            has_duplicates = true;
        }

        let mut value = "unknown".to_string();
        if let Some(original_lang) = &row.original_language {
            if original_lang != "eng" {
                value = "other".to_string();
            } else {
                value = "eng".to_string();
            }
        } else if let Some(translated_lang) = &row.translated_language {
            // Split translated_language into groups of 3 characters
            let chunks: Vec<&str> = translated_lang
                .as_bytes()
                .chunks(3)
                .map(|chunk| std::str::from_utf8(chunk).unwrap())
                .collect();

            if chunks[0] == "eng" {
                value = "other".to_string();
            } else if chunks.iter().any(|&chunk| chunk == "eng") {
                value = "eng".to_string();
            } else {
                value = "other".to_string();
            }
        } else {
            value = "unknown".to_string();
        }
        
        map
            .entry(row.cluster)
            .and_modify(|existing_value| {
                if existing_value == "unknown" {
                    // Preserve the existing value if it's "unknown"
                    *existing_value = value.clone();
                } else if *existing_value != value {
                    *existing_value = "ambiguous".to_string();
                }
            })
            .or_insert(value);
    }

    info!("merged {} translation cluster records", map.len());
    Ok(map)
}

#[derive(Debug, Default)]
pub struct WorkLanguageRow {
    pub work_id: u32,
    pub language_loc: Option<String>,
    pub language_author: Option<String>,
    pub language_ol: Option<i32>,

}

impl WorkLanguageRow {
    // function to deduce language
    pub fn deduce_language(&self) -> String {
        let mut result_loc: String = "unknown".to_string();
        let mut result_author: String = "unknown".to_string();
        let mut result_ol: String = "unknown".to_string();
    
        if self.language_loc.as_ref().map(|s| s == "eng").unwrap_or(false) {
            result_loc = "eng".to_string();
        } else if self.language_loc.as_ref().map(|s| s == "eng").unwrap_or(false) {
            result_loc = "other".to_string();
        }

        if self.language_author.as_ref().map(|s| s == "eng").unwrap_or(false) {
            result_author = "eng".to_string();
        } else if self.language_author.as_ref().map(|s| s == "other").unwrap_or(false) {
            result_author = "other".to_string();
        }

        if let Some(ref n) = self.language_ol {
            match n {
                1 => result_ol = "eng".to_string(),
                2 => result_ol = "both".to_string(),
                3 => result_ol = "other".to_string(),
                _ => {}
            };
        }

        match (result_loc.as_str(), result_author.as_str(), result_ol.as_str()) {
            ("eng", _, _) | (_, "eng", _) => "eng-original".to_string(),
            ("other", _, "eng") | ("other", _, "both") => "other-translated".to_string(),
            (_, "other", "eng") | (_, "other", "both") => "other-translated".to_string(),
            ("other", "unknown", "other") | ("unknown", "other", "other") | ("other", "other", "other") => "other-translation-not-found".to_string(),
            (_, _ , "eng") => "eng-original".to_string(),
            (_, _, "other") => "other-translation-not-found".to_string(),
            (_, _, "both") => "ambiguous".to_string(),
            _ => "unknown".to_string(),
        }
    }
    // Merge function to merge two WorkLanguageRow instances
    fn merge(&mut self, other: WorkLanguageRow) {
        // merge language loc
        match (&self.language_loc, other.language_loc) {
            (Some(_cur_loc), None) => {},
            (None, Some(new_loc)) => self.language_loc = Some(new_loc),
            (Some(cur_loc), Some(new_loc)) => {
                if *cur_loc == "ambiguous".to_string() || *cur_loc == "unknown".to_string() {
                    self.language_loc = Some(new_loc); 
                } else if *cur_loc == "other".to_string() &&  new_loc == "eng".to_string() {
                    info!("Work id {} has eng and other!", self.work_id);
                } else if *cur_loc == "eng".to_string() &&  new_loc == "other".to_string() {
                    info!("Work id {} has eng and other!", self.work_id);
                }
            }
            (_, _) => {}
        }

        // merge language author
        match (&self.language_author, other.language_author) {
            (Some(_cur_author), None) => {},
            (None, Some(new_author)) => self.language_author = Some(new_author),
            (Some(cur_author), Some(new_author)) => {
                if *cur_author == "other".to_string() &&  new_author == "eng".to_string() {
                    info!("Work id {} has eng and other!", self.work_id);
                } else if *cur_author == "eng".to_string() &&  new_author == "other".to_string() {
                    info!("Work id {} has eng and other!", self.work_id);
                } else if new_author == "eng".to_string() || new_author == "other".to_string() {
                    self.language_author = Some(new_author);
                }
            },
            (_, _) => {}
        }

        // merge language ol
        match(&self.language_ol, other.language_ol) {
            (None, Some(x)) => self.language_ol = Some(x),
            (Some(_x), Some(0)) => {},
            (Some(0), Some(x)) => self.language_ol = Some(x),
            (Some(2), _) | (_, Some(2)) => self.language_ol = Some(2), 
            (Some(1), Some(3)) | (Some(3), Some(1)) => self.language_ol = Some(2),
            (Some(x),Some(y)) if *x == y => {},
            _ => {},                // If no rule matches, keep the existing value
        }
    }
}

#[inline(never)]
pub fn cluster_derive_language() -> Result<HashMap<u32, WorkLanguageRow>> {
    let author_lf = LazyFrame::scan_parquet("book-links/cluster-languages.parquet", Default::default())?;
    let mut author_df = author_lf.collect()?;
    let author_df = author_df.rename("language", "language_author")?;

    let ol_lf = LazyFrame::scan_parquet("book-links/cluster-ol-work-language.parquet", Default::default())?;
    let mut ol_df = ol_lf.collect()?;
    let ol_df = ol_df.rename("language", "language_ol")?;

    info!("Perform an outer join author-ol");
    let author_ol_df = author_df.join(&ol_df, ["cluster"], ["cluster"], JoinType::Outer{ coalesce: true }.into())?;

    let loc_lf = LazyFrame::scan_parquet("book-links/cluster-loc-translations.parquet", Default::default())?;
    let mut loc_df = loc_lf.collect()?;
    let loc_df = loc_df.rename("loc_original_language", "language_loc")?;

    info!("Perform an outer join author-ol-loc");
    let author_ol_loc_df = author_ol_df.join(&loc_df, ["cluster"], ["cluster"], JoinType::Outer{ coalesce: true }.into())?;

    let book_links_lf = LazyFrame::scan_parquet("goodreads/gr-book-link.parquet", Default::default())?;
    let book_links_df = book_links_lf.collect()?;
 
    info!("Perform an outer join author-ol-loc-gr-booklinks");
    let merged_df = author_ol_loc_df.join(&book_links_df, ["cluster"], ["cluster"], JoinType::Inner.into())?;
    let merged_df = merged_df.filter(&merged_df.column("work_id")?.is_not_null())?;

    let has_none = merged_df.column("work_id")?.is_null().any();
    if has_none {
        println!("The column 'work_id' contains None values.");
    } else {
        println!("The column 'work_id' does not contain None values.");
    }

    info!("Prepare rows for iteration...");
    println!("{:?}", merged_df.schema());
    println!("Number of rows merged: {}", merged_df.height());
    let rows: Vec<WorkLanguageRow> = merged_df
        .column("work_id")?
        .i32()?
        .into_iter()
        .zip(merged_df.column("language_loc")?.str()?.into_iter())
        .zip(merged_df.column("language_author")?.str()?.into_iter())
        .zip(merged_df.column("language_ol")?.i32()?.into_iter())
        .map(|(((work_id, language_loc), language_author), language_ol)| {
            let invalid_authors = ["no-book-author", "no-author-rec", "no-language"];

            // Convert language_author to None if it matches any of the invalid values
            let language_author = language_author.and_then(|s| {
                if invalid_authors.contains(&s) {
                    None
                } else {
                    Some(s.to_string())
                }
            });

            WorkLanguageRow {
                work_id: work_id.unwrap() as u32, // Convert i32 to u32
                language_loc: language_loc.map(|s| s.to_string()), // Convert Option<&str> to Option<String>
                language_author: language_author, // Convert Option<&str> to Option<String>
                language_ol: language_ol, // Already Option<u8>
            }
        })
        .collect();

    
    // let mut frequency: HashMap<u32, u32> = HashMap::new();

    // for row in &rows {
    //     let count = frequency.entry(row.work_id).or_insert(0);
    //     *count += 1;
        
    //     if *count > 2 {
    //         println!("work_id {} appears more than twice!", row.work_id);
    //     }
    // }
    let mut map: HashMap<u32, WorkLanguageRow> = HashMap::new();
    for row in rows {
        if let Some(existing_row) = map.get_mut(&row.work_id) {
            // If it exists, merge the existing row with the new one
            existing_row.merge(row);
        } else {
            // If it doesn't exist, insert the new row into the map
            map.insert(row.work_id, row);
        }
    }

    info!("length: {}", map.len());
    Ok(map)
}

#[derive(Debug, Default)]
pub struct GRItemLanguageRow {
    pub gr_item: u32,
    pub deduced_language: String,

}

impl GRItemLanguageRow {
    fn merge(&mut self, other: GRItemLanguageRow) {
        //other-not-found into other-translation
        if self.deduced_language == "other-translation-not-found".to_string() && other.deduced_language == "other-translated".to_string() {
           self.deduced_language = other.deduced_language;
        } else
        // language ambiguous
        if self.deduced_language == "ambiguous".to_string() && other.deduced_language != "unknown".to_string() {
            self.deduced_language = other.deduced_language;
        } else
        // language unknown
        if self.deduced_language == "unknown".to_string() {
            self.deduced_language = other.deduced_language;
        }
    }
}

#[derive(Debug, Default)]
pub struct GRRatingLanguageRow {
    pub gr_item: u32,
    pub user_item: u32,
    pub rating: u32,
    pub deduced_language: String,

}

#[inline(never)]
pub fn gr_work_to_id() -> Result<Vec<GRRatingLanguageRow>> {
    let gr_item_languages_lf = LazyFrame::scan_parquet("goodreads/gr-item-languages.parquet", Default::default())?;
    let gr_item_languages_df = gr_item_languages_lf.collect()?;

    info!("Prepare rows for iteration...");
    println!("{:?}", gr_item_languages_df.schema());
    println!("Number of rows merged: {}", gr_item_languages_df.height());
    let rows: Vec<GRItemLanguageRow> = gr_item_languages_df
    .column("gr_item")?
    .i32()?
    .into_iter()
    .zip(gr_item_languages_df.column("deduced_language")?.str()?.into_iter())
    .map(|(gr_item, deduced_language)| {
        GRItemLanguageRow {
            gr_item: gr_item.unwrap() as u32, // Convert i32 to u32
            deduced_language: deduced_language.unwrap().to_string(), // Convert Option<&str> to Option<String>
        }
    })
    .collect();

    let mut map: HashMap<u32, GRItemLanguageRow> = HashMap::new();
    for row in rows {
        if let Some(existing_row) = map.get_mut(&row.gr_item) {
            // If it exists, merge the existing row with the new one
            existing_row.merge(row);
        } else {
            // If it doesn't exist, insert the new row into the map
            map.insert(row.gr_item, row);
        }
    }

    info!("length: {}", map.len());

    let gr_ratings_lf = LazyFrame::scan_parquet("goodreads/gr-work-ratings.parquet", Default::default())?;
    let gr_ratings_df = gr_ratings_lf.collect()?;

    info!("Prepare rows for iteration...");
    println!("{:?}", gr_ratings_df.schema());
    println!("Number of rows merged: {}", gr_ratings_df.height());

    let rows: Vec<GRRatingLanguageRow> = gr_ratings_df
    .column("user")?
    .i32()?
    .into_iter()
    .zip(gr_ratings_df.column("item")?.i32()?.into_iter())
    .zip(gr_ratings_df.column("rating")?.f32()?.into_iter())
    .map(|((user, item), rating)| {
        GRRatingLanguageRow {
            gr_item: item.unwrap() as u32, // Convert i32 to u32
            user_item: user.unwrap() as u32,
            rating: rating.unwrap() as u32,
            deduced_language: map.get(&(item.unwrap() as u32)).map(|s| s.deduced_language.clone()).unwrap_or("unknown".to_string()),
            //deduced_language: map.get(&(item.unwrap() as u32)).unrwap().deduced_language.cloned().unwrap_or_else(|| "unknown".to_string()),
        }
    })
    .collect();

    info!("length: {}", rows.len());
    Ok(rows)
}