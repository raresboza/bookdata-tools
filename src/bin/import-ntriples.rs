extern crate structopt;
#[macro_use]
extern crate log;
extern crate flate2;
extern crate indicatif;
extern crate bookdata;
extern crate zip;
extern crate postgres;
extern crate ntriple;
extern crate snap;
extern crate uuid;
extern crate crossbeam_channel;

use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::collections::HashSet;
use std::thread;

use structopt::StructOpt;
use std::fs;
use std::path::{Path, PathBuf};
use zip::read::ZipArchive;
use indicatif::{ProgressBar, ProgressStyle};
use postgres::Connection;
use uuid::Uuid;

use ntriple::parser::triple_line;
use ntriple::{Subject, Predicate, Object};

use crossbeam_channel::{Sender, Receiver, bounded};

use bookdata::cleaning::{write_pgencoded};
use bookdata::{log_init, Result};
use bookdata::db;

/// Import n-triples RDF (e.g. from LOC) into a database.
#[derive(StructOpt, Debug)]
#[structopt(name="import-ntriples")]
struct Opt {
  /// Verbose mode (-v, -vv, -vvv, etc.)
  #[structopt(short="v", long="verbose", parse(from_occurrences))]
  verbose: usize,
  /// Silence output
  #[structopt(short="q", long="quiet")]
  quiet: bool,
  /// Database URL to connect to
  #[structopt(long="db-url")]
  db_url: Option<String>,
  /// Database schema
  #[structopt(long="db-schema")]
  db_schema: Option<String>,
  /// Database table
  #[structopt(short="t", long="table")]
  table: String,
  /// Input file
  #[structopt(name = "INPUT", parse(from_os_str))]
  infile: PathBuf,
  /// Output directory
  #[structopt(name = "OUTPUT", parse(from_os_str))]
  outdir: PathBuf
}

/// Message for saving nodes in the other thread
#[derive(Debug)]
enum NodeMsg {
  SaveNode(Uuid, String),
  Close
}

/// Sink for saving nodes
struct NodeSink {
  thread: Option<thread::JoinHandle<u64>>,
  send: Sender<NodeMsg>
}

/// Node sink worker code
struct NodePlumber {
  seen: HashSet<Uuid>,
  query: String,
  source: Receiver<NodeMsg>,
}

impl NodePlumber {
  fn create(src: Receiver<NodeMsg>, schema: &str) -> NodePlumber {
    NodePlumber {
      seen: HashSet::new(),
      query: format!("INSERT INTO {}.nodes (node_id, node_iri) VALUES ($1, $2) ON CONFLICT DO NOTHING", schema),
      source: src
    }
  }

  /// Listen for messages and store in the database
  fn run(&mut self, url: &Option<String>) -> Result<u64> {
    let db = db::db_open(url)?;
    let mut added = 0;
    let mut done = false;
    while !done {
      let txn = db.transaction()?;
      let (n, eos) = self.run_batch(&txn)?;
      debug!("committing {} new additions", n);
      txn.commit()?;
      added += n;
      done = eos;
    }
    Ok(added)
  }

  /// Run a single batch of inserts
  fn run_batch(&mut self, db: &postgres::GenericConnection) -> Result<(u64, bool)> {
    let stmt = db.prepare_cached(&self.query)?;
    let mut n = 0;
    while n < 5000 {
      match self.source.recv()? {
        NodeMsg::SaveNode(id, iri) => {
          let idstr = id.to_simple_ref().to_string();
          if self.seen.insert(id) {
            n += stmt.execute(&[&idstr, &iri])?;
          }
        },
        NodeMsg::Close => {
          return Ok((n, true));
        }
      }
    };
    Ok((n, false))
  }
}

impl NodeSink {
  fn create(url: &Option<String>, ns: &str) -> NodeSink {
    let url = url.as_ref().map(|s| s.clone());
    let ns = ns.to_string();
    let (tx, rx) = bounded(1000);

    let jh = thread::spawn(move || {
      let mut plumber = NodePlumber::create(rx, &ns);
      plumber.run(&url).unwrap()
    });

    NodeSink {
      thread: Some(jh),
      send: tx
    }
  }

  fn save(&self, id: &Uuid, iri: &str) -> Result<()> {
    match self.send.send(NodeMsg::SaveNode(*id, iri.to_string())) {
      Ok(_) => Ok(()),
      Err(_) => Err(bookdata::err("node channel disconnected"))
    }
  }
}

impl Drop for NodeSink {
  fn drop(&mut self) {
    self.send.send(NodeMsg::Close).unwrap();
    if let Some(thread) = self.thread.take() {
      let saved = thread.join().unwrap();
      info!("saved {} nodes", saved);
    }
  }
}

struct IdGenerator<W: Write> {
  blank_ns: Uuid,
  node_sink: NodeSink,
  lit_file: W
}

impl<W: Write> IdGenerator<W> {
  fn create(nodes: NodeSink, lit_out: W, name: &str) -> IdGenerator<W> {
    let ns_ns = Uuid::new_v5(&Uuid::NAMESPACE_URL, "https://boisestate.github.io/bookdata/ns/blank".as_bytes());
    let blank_ns = Uuid::new_v5(&ns_ns, name.as_bytes());
    IdGenerator {
      blank_ns: blank_ns,
      node_sink: nodes,
      lit_file: lit_out
    }
  }

  fn node_id(&mut self, iri: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, iri.as_bytes());
    self.node_sink.save(&uuid, iri)?;
    Ok(uuid)
  }

  fn blank_id(&self, key: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v5(&self.blank_ns, key.as_bytes());
    Ok(uuid)
  }

  fn lit_id(&mut self, lit: &str) -> Result<Uuid> {
    let uuid = Uuid::new_v4();
    write!(&mut self.lit_file, "{}\t", uuid)?;
    write_pgencoded(&mut self.lit_file, lit.as_bytes())?;
    self.lit_file.write_all(b"\n")?;
    Ok(uuid)
  }

  fn subj_id(&mut self, sub: &Subject) -> Result<Uuid> {
    match sub {
      Subject::IriRef(iri) => self.node_id(iri),
      Subject::BNode(key) => self.blank_id(key)
    }
  }

  fn pred_id(&mut self, pred: &Predicate) -> Result<Uuid> {
    match pred {
      Predicate::IriRef(iri) => self.node_id(iri)
    }
  }

  fn obj_id(&mut self, obj: &Object) -> Result<Uuid> {
    match obj {
      Object::IriRef(iri) => self.node_id(iri),
      Object::BNode(key) => self.blank_id(key),
      Object::Lit(l) => self.lit_id(&l.data)
    }
  }
}

fn main() -> Result<()> {
  let opt = Opt::from_args();
  log_init(opt.quiet, opt.verbose)?;

  let inf = opt.infile.as_path();
  let fs = fs::File::open(inf)?;
  let fs = BufReader::new(fs);
  let mut zf = ZipArchive::new(fs)?;
  if zf.len() > 1 {
    error!("{:?}: more than one member file", inf);
    return Err(bookdata::err("too many input files"))
  } else if zf.len() == 0 {
    error!("{:?}: empty input archive", inf);
    return Err(bookdata::err("empty input archive"));
  }
  let member = zf.by_index(0)?;
  info!("processing member {:?} with {} bytes", member.name(), member.size());
  
  let outp = opt.outdir.as_path();
  if !outp.is_dir() {
    fs::create_dir_all(&outp)?;
  }

  let schema = opt.db_schema.unwrap_or("public".to_string());

  let node_sink = NodeSink::create(&opt.db_url, &schema);
  let lit_query = format!("COPY {}.literals FROM STDIN", schema);
  let lit_out = db::copy_target(&opt.db_url, &lit_query)?;
  let lit_out = BufWriter::new(lit_out);
  let triple_query = format!("COPY {}.{} FROM STDIN", schema, opt.table);
  let triples_out = db::copy_target(&opt.db_url, &triple_query)?;
  let mut triples_out = BufWriter::new(triples_out);

  let mut idg = IdGenerator::create(node_sink, lit_out, member.name());

  let pb = ProgressBar::new(member.size());
  pb.set_style(ProgressStyle::default_bar().template("{elapsed_precise} {bar} {percent}% {bytes}/{total_bytes} (eta: {eta})"));
  let pbr = pb.wrap_read(member);
  let pbr = BufReader::new(pbr);
  let mut lno = 0;
  for line in pbr.lines() {
    let line = line?;
    lno += 1;
    match triple_line(&line) {
      Ok(Some(tr)) => {
        let s_id = idg.subj_id(&tr.subject)?;
        let p_id = idg.pred_id(&tr.predicate)?;
        let o_id = idg.obj_id(&tr.object)?;
        write!(&mut triples_out, "{}\t{}\t{}\n", s_id, p_id, o_id)?
      },
      Ok(None) => (),
      Err(ref e) if pb.is_hidden() => {
        error!("error on line {}: {:?}", lno, e);
        error!("invalid line contained: {}", line);
      },
      Err(ref e) => {
        pb.println(format!("error on line {}: {:?}", lno, e));
      }
    };
  }

  Ok(())
}
