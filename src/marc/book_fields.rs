//! Code for writing extracted information specific to books.
use parquet_derive::ParquetRecordWriter;
use serde::Serialize;

use crate::arrow::*;
use crate::cleaning::isbns::{parse_isbn_string, ParseResult};
use crate::cleaning::names::clean_name;
use crate::marc::flat_fields::FieldOutput;
use crate::marc::MARCRecord;
use crate::prelude::*;

/// Structure recording book identifiers from a MARC field.
#[derive(ParquetRecordWriter, Debug)]
struct BookIds {
    rec_id: u32,
    marc_cn: String,
    lccn: Option<String>,
    status: u8,
    rec_type: u8,
    bib_level: u8,
}

/// Structure recording an ISBN record from a book.
#[derive(Serialize, ParquetRecordWriter, Debug)]
struct ISBNrec {
    rec_id: u32,
    isbn: String,
    tag: Option<String>,
}

/// Structure recording a Language record of a book
#[derive(Serialize, ParquetRecordWriter, Debug)]
struct LanguageRec{ // Added for Thesis 
    rec_id: u32,
    original_language: Option<String>,
    translated_language: Option<String>,
}

/// Structure recording a record's author field.
#[derive(Serialize, ParquetRecordWriter, Debug)]
struct AuthRec {
    rec_id: u32,
    author_name: String,
}

/// Output that writes books to set of Parquet files.
pub struct BookOutput {
    n_books: u32,
    prefix: String,
    fields: FieldOutput,
    ids: TableWriter<BookIds>,
    isbns: TableWriter<ISBNrec>,
    authors: TableWriter<AuthRec>,
    languages: TableWriter<LanguageRec>,
}

impl BookOutput {
    pub fn open(prefix: &str) -> Result<BookOutput> {
        let ffn = format!("{}-fields.parquet", prefix);
        info!("writing book fields to {}", ffn);
        let fields = TableWriter::open(ffn)?;
        let fields = FieldOutput::new(fields);

        let idfn = format!("{}-ids.parquet", prefix);
        info!("writing book IDs to {}", idfn);
        let ids = TableWriter::open(idfn)?;

        let isbnfn = format!("{}-isbns.parquet", prefix);
        info!("writing book IDs to {}", isbnfn);
        let isbns = TableWriter::open(isbnfn)?;

        let authfn = format!("{}-authors.parquet", prefix);
        info!("writing book authors to {}", authfn);
        let authors = TableWriter::open(authfn)?;

        let langfn = format!("{}-languages.parquet", prefix);
        info!("writing book languages to {}", langfn);
        let languages = TableWriter::open(langfn)?;

        Ok(BookOutput {
            n_books: 0,
            prefix: prefix.to_string(),
            fields,
            ids,
            isbns,
            authors,
            languages,
        })
    }
}

impl DataSink for BookOutput {
    fn output_files(&self) -> Vec<PathBuf> {
        vec![
            format!("{}-fields.parquet", &self.prefix).into(),
            format!("{}-ids.parquet", &self.prefix).into(),
            format!("{}-isbns.parquet", &self.prefix).into(),
        ]
    }
}

impl ObjectWriter<MARCRecord> for BookOutput {
    fn write_object(&mut self, record: MARCRecord) -> Result<()> {
        if !record.is_book() {
            return Ok(());
        }
        self.n_books += 1;
        let rec_id = self.n_books;

        // scan for ISBNs and authors
        for df in &record.fields {
            // ISBNs: tag 20, subfield 'a'
            if df.tag == 20 {
                for sf in &df.subfields {
                    if sf.code == 'a' {
                        match parse_isbn_string(&sf.content) {
                            ParseResult::Valid(isbns, _) => {
                                for isbn in isbns {
                                    if isbn.tags.len() > 0 {
                                        for tag in isbn.tags {
                                            self.isbns.write_object(ISBNrec {
                                                rec_id,
                                                isbn: isbn.text.clone(),
                                                tag: Some(tag),
                                            })?;
                                        }
                                    } else {
                                        self.isbns.write_object(ISBNrec {
                                            rec_id,
                                            isbn: isbn.text,
                                            tag: None,
                                        })?;
                                    }
                                }
                            }
                            ParseResult::Ignored(_) => (),
                            ParseResult::Unmatched(s) => {
                                warn!("unmatched ISBN text {}", s)
                            }
                        }
                    }
                }
            } else if df.tag == 41 {
                // language code
                // first indicator
                // 0 - item not a translation/does not include a translation
                // 1 - item is or includes a translation
                // subfield
                // a - language code of the text
                // h - language code of the original
                let mut original_language: Option<String> = None;
                let mut translated_language: Option<String> = None;
                for sf in &df.subfields {
                    if sf.code == 'a' {
                      let content = sf.content.trim();
                      if df.ind1 == 0 {
                          original_language = Some(content.to_string());
                      } else {
                      // default is code 1
                          translated_language = Some(content.to_string());
                      }
                    } else if sf.code == 'h' {
                      let content = sf.content.trim();
                      original_language = Some(content.to_string());
                    }
                }
                self.languages.write_object(LanguageRec {
                    rec_id,
                    original_language,
                    translated_language,
                })?;
            } else if df.tag == 100 {
                // authors: tag 100, subfield a
                for sf in &df.subfields {
                    if sf.code == 'a' {
                        let content = sf.content.trim();
                        let author_name = clean_name(content);
                        if !author_name.is_empty() {
                            self.authors.write_object(AuthRec {
                                rec_id,
                                author_name,
                            })?;
                        }
                    }
                }
            }
        }

        // emit book IDs
        let ids = BookIds {
            rec_id,
            marc_cn: record
                .marc_control()
                .ok_or_else(|| anyhow!("no MARC control number"))?
                .to_owned(),
            lccn: record.lccn().map(|s| s.to_owned()),
            status: record.rec_status().unwrap_or(0),
            rec_type: record.rec_type().unwrap_or(0),
            bib_level: record.rec_bib_level().unwrap_or(0),
        };
        self.ids.write_object(ids)?;

        self.fields.write_object(record)?;
        Ok(())
    }

    fn finish(self) -> Result<usize> {
        self.fields.finish()?;
        self.ids.finish()?;
        self.isbns.finish()?;
        self.authors.finish()?;
        self.languages.finish()?;
        Ok(self.n_books as usize)
    }
}
