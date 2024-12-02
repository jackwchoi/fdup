use rayon::prelude::*;
use sha2::{Digest, Sha512};
use std::{
    borrow::Borrow,
    collections::HashMap,
    fs::File,
    hash::Hash,
    io::Read,
    path::{Path, PathBuf},
};
use walkdir::{DirEntry, WalkDir};

/// # Returns
///
/// SHA512 checksum of the contents of the file whose filepath is `path`.
fn get_sha512_hash(path: &Path) -> impl AsRef<[u8; 64]> + Eq + Hash + Send + Sync {
    debug_assert!(path.exists());
    debug_assert!(path.is_file());

    let mut buffer = [0u8; 4096];
    let mut file = File::open(path).unwrap();
    let mut hasher = Sha512::new();

    // Read `BUFFER_SIZE` bytes from `file` at a time and feed them to `hasher`.
    loop {
        match file.read(&mut buffer).unwrap() {
            0 => break hasher.finalize(),
            size => hasher.update(&buffer[..size]),
        }
    }
}

/// # Returns
///
/// Size of the file described by `entry`, in bytes.
fn get_file_size(entry: &DirEntry) -> usize {
    debug_assert!(entry.path().exists());
    debug_assert!(entry.file_type().is_file());
    debug_assert!(!entry.file_type().is_symlink());

    entry.metadata().unwrap().len() as usize
}

/// # Returns
///
/// Union `U` of `lhs` and `rhs`, such that `U[k] = lhs[k]` if `k` only exists in `lhs`,
/// `U[k] = rhs[k]` if `k` only exists in `rhs`, otherwise `U[k] = lhs[k] + rhs[k]`. The order of
/// elements in the vectors are undefined.
fn union_multimap<Key, Item>(
    mut lhs: HashMap<Key, Vec<Item>>,
    mut rhs: HashMap<Key, Vec<Item>>,
) -> HashMap<Key, Vec<Item>>
where
    Key: Eq + Hash,
{
    let drain_into = |source: &mut HashMap<Key, Vec<Item>>,
                      target: &mut HashMap<Key, Vec<Item>>| {
        for (key, mut source_values) in source.drain() {
            match target.get_mut(&key) {
                Some(target_values) => target_values.append(&mut source_values),
                None => {
                    target.insert(key, source_values);
                }
            }
        }
    };

    // Drain the smaller map into the larger map.
    if lhs.len() <= rhs.len() {
        drain_into(&mut lhs, &mut rhs);
        rhs
    } else {
        drain_into(&mut rhs, &mut lhs);
        lhs
    }
}

/// Partition the sequence `items` into subsequences such that two items `a, b` will be in the same
/// subsequence if and only if `get_key(a) == get_key(b)`.
fn partition_by_key<Key, T, TBorrowed>(
    get_key: impl Fn(&TBorrowed) -> Key + Send + Sync,
    items: impl ParallelIterator<Item = T>,
) -> impl ParallelIterator<Item = Vec<T>>
where
    Key: Eq + Hash + Send + Sync,
    T: Borrow<TBorrowed> + Send + Sync,
    TBorrowed: ?Sized,
{
    let id = || HashMap::<Key, Vec<T>>::new();

    // First, use parallel-fold to generate hashmaps that map `k: Key` to `ts: Vec<T>` such that
    // for all `t` in `ts`, `get_key(t) == k`. Then, use parallel-reduce to merge these hashmaps
    // into one hashmap.
    let union: HashMap<Key, Vec<T>> = items
        .map(|item| (get_key(item.borrow()), item))
        .fold(id, |mut acc, (key, item)| {
            match acc.get_mut(&key) {
                Some(items_with_same_key) => items_with_same_key.push(item),
                None => {
                    acc.insert(key, vec![item]);
                }
            };
            acc
        })
        .reduce(id, |lhs, rhs| union_multimap(lhs, rhs));

    // Keys can be discarded.
    union.into_par_iter().map(|(_, v)| v)
}

/// # Returns
///
/// Sequence of `Vec<PathBuf>` where each vector contains paths to files with the same size and
/// SHA512 checksum.
pub fn group_duplicate_files(
    sort: bool,
    root: &Path,
) -> impl ParallelIterator<Item = Vec<PathBuf>> {
    // Grab all files under `root`, crashing if we encounter any IO errors.
    let files = WalkDir::new(&root)
        .follow_links(false)
        .into_iter()
        .par_bridge()
        .map(walkdir::Result::unwrap)
        .filter(|dir_entry| dir_entry.file_type().is_file());

    // 1. Group `items` by size, using `get_file_size`. Discard groups with size less than 2.
    // 2. Within each group, subgroup its items by checksum, using `get_sha512_hash`. Discard
    //    subgroups with size less than 2.
    // 3. Flatten, to return an iterator of subgroups.
    partition_by_key(&get_file_size, files)
        .filter(|p| 1 < p.len())
        .map(|files_with_same_size| {
            files_with_same_size
                .into_par_iter()
                .map(DirEntry::into_path)
        })
        .flat_map(|files_with_same_size| {
            partition_by_key(&get_sha512_hash, files_with_same_size).filter(|p| 1 < p.len())
        })
        .map(move |mut files_with_same_hash| {
            if sort {
                files_with_same_hash.sort();
            }
            files_with_same_hash
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashSet,
        env,
        fmt::Display,
        fs::{create_dir_all, read_to_string, remove_dir_all, remove_file, File},
        io::Write,
        path::PathBuf,
    };

    // Wrapper around `std::fs::File` that deletes itself when it's dropped.
    struct TempFile {
        file: Option<File>,
        is_file: bool,
        path: PathBuf,
    }
    impl TempFile {
        pub fn new(is_file: bool, path: impl Borrow<Path>) -> TempFile {
            let mut file = None;
            let path_borrowed = path.borrow();
            // Delete the file/dir if it already exists, before creating a new one.
            if is_file {
                if path_borrowed.exists() {
                    remove_file(path_borrowed).unwrap();
                }
                file = Some(File::create(path_borrowed).unwrap());
            } else {
                if path_borrowed.exists() {
                    remove_dir_all(path_borrowed).unwrap();
                }
                create_dir_all(path_borrowed).unwrap();
            }

            TempFile {
                file,
                is_file,
                path: path_borrowed.to_path_buf(),
            }
        }
        pub fn path<'a>(&'a self) -> &'a Path {
            &self.path
        }
        pub fn file<'a>(&'a self) -> &'a File {
            &self.file.as_ref().unwrap()
        }
    }
    impl Drop for TempFile {
        fn drop(&mut self) {
            if self.is_file {
                remove_file(&self.path()).unwrap();
            } else {
                remove_dir_all(&self.path()).unwrap();
            }
        }
    }

    // # Returns
    //
    // Path to some newly created tempfile in some OS-managed tempdir. The basename of this file
    // will be prefixed with `prefix` and its content equal to `content`.
    //
    // Whether or not the resulting path is unique depends on the prefix.
    fn mktemp(prefix: impl Borrow<str>, content: impl Display) -> TempFile {
        // construct a path to a temporary file
        let path = env::temp_dir().as_path().join(prefix.borrow());
        let tempfile = TempFile::new(true /* is_file */, path);

        // write `content` to the file
        write!(tempfile.file(), "{}", content).unwrap();
        tempfile
    }

    fn test_data() -> Vec<String> {
        vec![
            " ",
            " 12p oka0sd k\n rn12w\r\r\n \t asof AWSDJO !@# @$ ",
            "",
            "12asdopjkzx",
            "2",
            "QxmPzHlMLisNDJm3LKT5LRoTyU9Z06ze",
            "\n",
            "\r",
            "\r\n",
            "\t",
            "g",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    // Check that `get_sha512_hash` returns a unique checksum for each test data.
    #[test]
    fn test_get_sha512_hash() {
        let sums: HashSet<Vec<u8>> = test_data()
            .into_iter()
            .enumerate()
            .map(|(index, content)| {
                let prefix = format!("{}_{}_{}_{}", module_path!(), line!(), column!(), index);
                let tempfile = mktemp(prefix, &content);
                assert_eq!(content, read_to_string(&tempfile.path()).unwrap());

                // pseudo check that the function is deterministic
                let sums: HashSet<Vec<u8>> = (0..4)
                    .map(|_| get_sha512_hash(&tempfile.path()).as_ref().to_vec())
                    .collect();
                assert_eq!(1, sums.len());
                sums.into_iter().nth(0).unwrap()
            })
            .collect();

        // # of Checksum512s == # of input data
        assert_eq!(test_data().len(), sums.len());
    }

    // Check that `get_file_size` returns the correct file size in bytes.
    #[test]
    fn test_get_file_size() {
        test_data()
            .into_iter()
            .enumerate()
            .for_each(|(index, content)| {
                let prefix = format!("{}_{}_{}_{}", module_path!(), line!(), column!(), index);
                let tempfile = mktemp(prefix, &content);
                assert_eq!(content, read_to_string(&tempfile.path()).unwrap());
                let temp_as_entry = WalkDir::new(&tempfile.path())
                    .into_iter()
                    .filter_map(Result::ok)
                    .nth(0)
                    .unwrap();
                let result = get_file_size(&temp_as_entry);
                let expected = content.len();
                assert_eq!(expected, result);
            });
    }

    // E2E test.
    #[test]
    fn fdup() {
        let prefix = format!("{}_{}_{}", module_path!(), line!(), column!());
        let test_dir = TempFile::new(false /* is_file */, std::env::temp_dir().join(prefix));

        create_dir_all(test_dir.path().join("d1/d2/d3/d4")).unwrap();

        // create and populate each file
        let _tempfiles: Vec<_> = vec![
            ("d1/f1", ""),
            ("d1/f2", ""),
            ("d1/f3", "\n"),
            ("d1/d2/f4", "a\nbc2"),
            ("d1/d2/d3/f4", "abcde"),
            ("d1/d2/d3/d4/f6", "a\nbc2"),
            ("d1/d2/d3/d4/f7", "\n"),
            ("d1/d2/d3/d4/f8", "\n"),
        ]
        .into_iter()
        .map(|(path, content)| (test_dir.path().join(path), content))
        .map(|(path_buf, content)| {
            let file = mktemp(path_buf.to_str().unwrap(), content);
            assert_eq!(read_to_string(&path_buf).unwrap(), content);
            file
        })
        .collect();

        let results: HashSet<Vec<PathBuf>> = group_duplicate_files(false, &test_dir.path())
            .map(|mut v| {
                v.sort();
                v
            })
            .collect();
        let expected = HashSet::from([
            vec![test_dir.path().join("d1/f1"), test_dir.path().join("d1/f2")],
            vec![
                test_dir.path().join("d1/d2/d3/d4/f6"),
                test_dir.path().join("d1/d2/f4"),
            ],
            vec![
                test_dir.path().join("d1/d2/d3/d4/f7"),
                test_dir.path().join("d1/d2/d3/d4/f8"),
                test_dir.path().join("d1/f3"),
            ],
        ]);
        assert_eq!(expected, results);
    }
}
