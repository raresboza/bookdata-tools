//! Command line interface for book data.
//!
//! The book data tools are implemented as a single monolithic executable (to reduce
//! compilation time and disk space in common configurations, with different tools
//! implemented as subcommands.  Each subcommand implements the [Command] trait, which
//! exposes the command line arguments and invocation.
pub mod fusion;
pub mod index_names;
pub mod extract_graph;
pub mod cluster_books;
pub mod rdf_scan_nodes;
pub mod rdf_scan_triples;

use structopt::StructOpt;
use tokio::runtime::Runtime;
use enum_dispatch::enum_dispatch;
use async_trait::async_trait;

/// Trait implemented by book data commands.
#[enum_dispatch]
pub trait Command {
  /// Run the command with options
  fn exec(&self) -> Result<()>;
}

/// Enum to collect and dispatch CLI commands.
#[enum_dispatch(Command)]
#[derive(StructOpt, Debug)]
pub enum BDCommand {
  Fusion(fusion::Fusion),
  ClusterBooks(cluster_books::ClusterBooks),
  IndexNames(index_names::IndexNames),
  ExtractGraph(extract_graph::ExtractGraph),
  RDF(RDFWrapper)
}

/// Tools for processing RDF.
#[derive(StructOpt, Debug)]
pub struct RDFWrapper {
  #[structopt(subcommand)]
  command: RDFCommand
}

impl Command for RDFWrapper {
  fn exec(&self) -> Result<()> {
    self.command.exec()
  }
}

#[enum_dispatch(Command)]
#[derive(StructOpt, Debug)]
pub enum RDFCommand {
  ScanNodes(rdf_scan_nodes::ScanNodes),
  ScanTriples(rdf_scan_triples::ScanTriples),
}

/// Trait for implementing commands asynchronously.
#[async_trait]
pub trait AsyncCommand {
  async fn exec_future(&self) -> Result<()>;
}

#[async_trait]
impl <T: AsyncCommand> Command for T {
  fn exec(&self) -> Result<()> {
    let runtime = Runtime::new()?;
    let task = self.exec_future();
    runtime.block_on(task)
  }
}

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
