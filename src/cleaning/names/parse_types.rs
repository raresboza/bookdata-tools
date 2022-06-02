use thiserror::Error;

#[derive(Error, Debug)]
pub enum NameError {
  #[error("Error parsing name entry: {0}")]
  ParseError(nom::error::Error<String>),
  #[error("Error parsing name entry: {0}")]
  PEGError(#[from] peg::error::ParseError<peg::str::LineCol>),
}

pub enum NameFmt {
  Single(String),
  TwoPart(String, String),
  Empty
}

pub struct NameEntry {
  pub name: NameFmt,
  pub year: Option<String>
}

impl From<(NameFmt, String)> for NameEntry {
  fn from(t: (NameFmt, String)) -> NameEntry {
    let (name, year) = t;
    NameEntry {
      name, year: Some(year)
    }
  }
}

impl From<NameFmt> for NameEntry {
  fn from(name: NameFmt) -> NameEntry {
    NameEntry {
      name, year: None
    }
  }
}
