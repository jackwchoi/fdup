use std::path::PathBuf;
use structopt::StructOpt;

/// Find duplicate files recursively and in parallel.
///
/// fdup finds duplicate files quickly by checking file sizes and content checksums.
#[derive(StructOpt, Debug)]
#[structopt(name = "fdup")]
pub struct Opt {
    /// Sort each group of duplicate files lexicographically.
    #[structopt(long = "sort")]
    pub sort: bool,

    /// Number of threads to use. 0 indicates
    #[structopt(long = "threads", default_value = "0")]
    pub num_threads: usize,

    /// Root directory from which to start the search.
    #[structopt(parse(from_os_str))]
    pub root: PathBuf,
}
