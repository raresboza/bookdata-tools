//! Command line interface for book data.
//!
//! The book data tools are implemented as a single monolithic executable (to reduce
//! compilation time and disk space in common configurations, with different tools
//! implemented as subcommands.  Each subcommand implements the [Command] trait, which
//! exposes the command line arguments and invocation.
mod support;
pub use support::{CmdEntry, Command};

pub mod fusion;
pub mod index_names;
pub mod rdf_scan_nodes;
pub mod rdf_scan_triples;


pub fn commands() -> Vec<Box<dyn CmdEntry>> {
  vec![
    fusion::Fusion::entry(),
    index_names::IndexNames::entry(),
    rdf_scan_nodes::ScanNodes::entry(),
    rdf_scan_triples::ScanTriples::entry(),
  ]
}


use structopt::StructOpt;
use happylog::args::LogOpts;
use anyhow::Result;

#[derive(StructOpt, Debug)]
pub struct CommonOpts {
  #[structopt(flatten)]
  logging: LogOpts
}

impl CommonOpts {
  pub fn init(&self) -> Result<()> {
    self.logging.init()?;
    Ok(())
  }
}
