//! Scan the nodes in an RDF ntriples file.
use bookdata::prelude::*;
use bookdata::io::LineProcessor;
use bookdata::rdf::model::*;
use bookdata::rdf::nodeindex::NodeIndex;
use bookdata::rdf::nsmap::NSMap;

#[derive(StructOpt, Debug)]
#[structopt(name="rdf-scan-nodes")]
/// Scan the nodes in one or more RDF triples files.
pub struct ScanNodes {
  #[structopt(flatten)]
  common: CommonOpts,

  /// Use a namespace map file.
  #[structopt(short="m", long="ns-map")]
  nsmap: Option<PathBuf>,

  /// Write named node IDs to an output file.
  #[structopt(short="w", long="save-index")]
  outfile: Option<PathBuf>,

  /// Input file
  #[structopt(name = "FILE", parse(from_os_str))]
  infiles: Vec<PathBuf>,
}

fn scan_file(idx: &mut NodeIndex, path: &Path) -> Result<()> {
  let proc = LineProcessor::open_solo_zip(path)?;
  for triple in proc.records() {
    let triple: Triple = triple?;
    if !triple.is_empty() {
      scan_triple(idx, triple)?;
    }
  }

  Ok(())
}

fn scan_triple(idx: &mut NodeIndex, triple: Triple) -> Result<()> {
  idx.node_id(&triple.subject)?;
  idx.iri_id(&triple.predicate)?;
  term_id(idx, &triple.object)?;
  Ok(())
}

/// Generate an ID for an RDF term.
pub fn term_id(idx: &mut NodeIndex, obj: &Term) -> Result<Option<i32>> {
  match obj {
    Term::Node(node) => idx.node_id(node).map(Some),
    Term::Literal(_) => Ok(None)
  }
}

pub fn main() -> Result<()> {
  let opts = ScanNodes::from_args();
  opts.common.init()?;

  let mut idx = if let Some(path) = &opts.outfile {
    NodeIndex::new_with_file(path)?
  } else {
    NodeIndex::new_in_memory()
  };
  if let Some(path) = &opts.nsmap {
    let map = NSMap::load(path)?;
    idx.set_nsmap(&map);
  }

  for file in opts.infiles {
    scan_file(&mut idx, file.as_ref())?;
  }

  idx.finish()?;

  Ok(())
}