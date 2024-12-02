# fdup

[![fdup-crate](https://img.shields.io/crates/v/fdup.svg)](https://crates.io/crates/fdup)

- [crates.io](https://crates.io/crates/fdup)

## Table of Contents

1. [fdup](#fdup)
    1. [Overview](#overview)

## Overview

```bash
$ fdup --help
fdup 3.0.0
Find duplicate files.

fdup finds duplicate files quickly by checking file sizes and content checksums.

USAGE:
    fdup [FLAGS] [OPTIONS] <root>

FLAGS:
    -h, --help
            Prints help information

        --sort
            Sort each group of duplicate files lexicographically

    -V, --version
            Prints version information


OPTIONS:
        --threads <num-threads>
            Number of threads to use. 0 indicates [default: 0]


ARGS:
    <root>
            Root directory from which to start the search
```
