//! Code for working with languages.
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum LanguageError {
    #[error("could not parse language from string")]
    #[allow(dead_code)]
    ParseError,
}

/// A language representation.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Language {
    Unknown,
    English,
    Other,
    Open(String),
}

/// A collection of languages.
#[derive(Default, Debug)]
pub struct LanguageBag {
    size: usize,
    mask: u32,
    resolved: Language,
}

impl Default for Language {
    fn default() -> Self {
        Language::Unknown
    }
}

impl FromStr for Language {
    type Err = LanguageError;

    fn from_str(s: &str) -> Result<Language, LanguageError> {
        Ok(s.into())
    }
}

impl From<&str> for Language {
    fn from(s: &str) -> Language {
        let sg = s.trim().to_lowercase();
        
        if sg.contains("eng") || sg.contains("ang") || sg.contains("ingl") {
            return Language::English;
        }

        if sg == "xxx" || sg.contains("fem") || sg.contains("male") {
            return Language::Unknown;
        }

        Language::Other
    }
}

impl From<String> for Language {
    fn from(s: String) -> Language {
        s.as_str().into()
    }
}

impl<T> From<&T> for Language
where
    T: Into<Language> + Clone,
{
    fn from(obj: &T) -> Language {
        obj.clone().into()
    }
}

impl fmt::Display for Language {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Language::Unknown => f.write_str("unknown"),
            Language::English => f.write_str("english"),
            Language::Other => f.write_str("other"),
            Language::Open(s) => f.write_str(s.as_str()),
        }
    }
}

impl Language {
    /// Merge a Language record with another.  If the two records
    /// disagree, the result is [Gender::Ambiguous].
    pub fn merge(&self, other: &Language) -> Language {
        match (self, other) {
            (Language::Unknown, g) => g.clone(),
            (g, Language::Unknown) => g.clone(),
            (g1, g2) if g1 == g2 => g2.clone(),
            _ => Language::Unknown,
        }
    }

    /// Get an integer usable as a mask for this Language.
    fn mask_val(&self) -> u32 {
        match self {
            Language::Unknown => 1,
            Language::English => 2,
            Language::Other => 4,
            Language::Open(_) => 0x8000_0000,
        }
    }
}

impl LanguageBag {
    /// Add a language to this bag.
    pub fn add(&mut self, language: Language) {
        self.size += 1;
        self.mask |= language.mask_val();
        self.resolved = self.resolved.merge(&language);
    }

    /// Merge another language bag into this one.
    pub fn merge_from(&mut self, bag: &LanguageBag) {
        self.size += bag.size;
        self.mask |= bag.mask;
        self.resolved = self.resolved.merge(&bag.resolved);
    }

    /// Get the number of language entries recorded to make this language record.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.size
    }

    /// Check whether the language bag is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Get the language, returning [None] if no languages are seen.
    #[allow(dead_code)]
    pub fn maybe_language(&self) -> Option<&Language> {
        if self.is_empty() {
            None
        } else {
            Some(&self.resolved)
        }
    }

    /// Get the language, returning [Language::Unknown] if no languages are seen.
    pub fn to_language(&self) -> &Language {
        &self.resolved
    }
}
