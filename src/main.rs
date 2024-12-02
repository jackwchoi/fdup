mod clargs;
mod fdup;

use clargs::*;
use fdup::*;
use rayon::prelude::*;
use std::io::prelude::*;
use structopt::StructOpt;

fn main() {
    let Opt {
        num_threads,
        root,
        sort,
    } = Opt::from_args();

    if num_threads != 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .unwrap();
    }

    group_duplicate_files(sort, &root).for_each(|vec| {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{:?}", vec).unwrap();
    });
}
