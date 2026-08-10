/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::error::Error;
use std::fmt;
use std::path::Path;

use crate::RegistryPath;

/// The result of looking up a file in a local Bazel registry.
///
/// `NotFound` is reserved for raw `ENOENT` while resolving the configured root
/// or one of its registry-path descendants. Callers decide whether that absence
/// permits trying another registry for the requested registry file kind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileRegistryRead {
    Found(Box<[u8]>),
    NotFound,
}

/// A terminal failure while reading a local Bazel registry.
///
/// The variants intentionally do not retain paths or operating-system error
/// values. Registry roots can be machine-local and must not appear in public
/// errors or DICE values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileRegistryReadError {
    UnsupportedPlatform,
    InvalidRoot,
    InvalidIntermediateEntry,
    InvalidFinalEntry,
    SpecialFinalEntry,
    FileTooLarge,
    ReadFailed,
    AllocationFailed,
}

impl fmt::Display for FileRegistryReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnsupportedPlatform => {
                "secure local registry reads are unsupported on this platform"
            }
            Self::InvalidRoot => "the local registry root is not a safe directory",
            Self::InvalidIntermediateEntry => {
                "a local registry path component is not a safe directory"
            }
            Self::InvalidFinalEntry => "the local registry file could not be opened safely",
            Self::SpecialFinalEntry => "the local registry entry is not a regular file",
            Self::FileTooLarge => "the local registry file exceeds the configured size limit",
            Self::ReadFailed => "the local registry file could not be read",
            Self::AllocationFailed => "memory for the local registry file could not be allocated",
        })
    }
}

impl Error for FileRegistryReadError {}

/// Read a canonical registry-relative path beneath an already resolved root.
///
/// The runtime-resolved `root` must be absolute. On Linux, every component of
/// the root and registry path is traversed relative to an open directory
/// descriptor without following symlinks. The final entry is first opened with
/// `O_PATH` and verified as a regular file before a readable descriptor for the
/// same inode is acquired. Other platforms fail closed until they can provide
/// equivalent traversal and type-before-open semantics.
pub fn read_file_registry(
    root: &Path,
    path: &RegistryPath,
    max_bytes: usize,
) -> Result<FileRegistryRead, FileRegistryReadError> {
    read_file_registry_impl(root, path, max_bytes, || {}, |_| {})
}

#[cfg(target_os = "linux")]
fn read_file_registry_impl(
    root: &Path,
    path: &RegistryPath,
    max_bytes: usize,
    after_stat: impl FnOnce(),
    mut capacity_growth: impl FnMut(usize),
) -> Result<FileRegistryRead, FileRegistryReadError> {
    use std::io::Read;

    use nix::errno::Errno;
    use nix::fcntl::OFlag;
    use nix::sys::stat::Mode;

    let directory = match open_registry_root(root)? {
        OpenRegistryRoot::Found(directory) => directory,
        OpenRegistryRoot::NotFound => return Ok(FileRegistryRead::NotFound),
    };

    let mut components = path.as_str().split('/').peekable();
    let mut directory = directory;
    let file = loop {
        let component = components
            .next()
            .ok_or(FileRegistryReadError::InvalidFinalEntry)?;

        if components.peek().is_some() {
            directory = match nix::fcntl::openat(
                &directory,
                component,
                OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                Mode::empty(),
            ) {
                Ok(directory) => directory,
                Err(Errno::ENOENT) => return Ok(FileRegistryRead::NotFound),
                Err(_) => return Err(FileRegistryReadError::InvalidIntermediateEntry),
            };
            continue;
        }

        break match nix::fcntl::openat(
            &directory,
            component,
            OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        ) {
            Ok(file) => file,
            Err(Errno::ENOENT) => return Ok(FileRegistryRead::NotFound),
            Err(_) => return Err(FileRegistryReadError::InvalidFinalEntry),
        };
    };

    let stat = nix::sys::stat::fstat(&file).map_err(|_| FileRegistryReadError::ReadFailed)?;
    let file_type = nix::sys::stat::SFlag::from_bits_truncate(stat.st_mode);
    if file_type == nix::sys::stat::SFlag::S_IFLNK {
        return Err(FileRegistryReadError::InvalidFinalEntry);
    }
    if file_type != nix::sys::stat::SFlag::S_IFREG {
        return Err(FileRegistryReadError::SpecialFinalEntry);
    }
    let metadata_len =
        usize::try_from(stat.st_size).map_err(|_| FileRegistryReadError::FileTooLarge)?;
    if metadata_len > max_bytes {
        return Err(FileRegistryReadError::FileTooLarge);
    }

    // Tests use this boundary to prove both growth detection and descriptor
    // stability after a pathname replacement. Production calls a no-op.
    after_stat();

    // O_PATH performs no device open and cannot block on a FIFO. Only after
    // fstat proves the pinned object is regular do we obtain a readable handle
    // for that exact inode through procfs, then validate the identity again.
    let mut file = reopen_pinned_regular_file(&file, &stat)?;
    let mut bytes = Vec::new();
    reserve_geometrically(&mut bytes, metadata_len, max_bytes, &mut capacity_growth)?;
    let mut chunk = [0; 8192];
    loop {
        let read = file
            .read(&mut chunk)
            .map_err(|_| FileRegistryReadError::ReadFailed)?;
        if read == 0 {
            break;
        }
        if read > max_bytes.saturating_sub(bytes.len()) {
            return Err(FileRegistryReadError::FileTooLarge);
        }
        let needed = bytes.len() + read;
        reserve_geometrically(&mut bytes, needed, max_bytes, &mut capacity_growth)?;
        bytes.extend_from_slice(&chunk[..read]);
    }

    Ok(FileRegistryRead::Found(bytes.into_boxed_slice()))
}

#[cfg(target_os = "linux")]
enum OpenRegistryRoot {
    Found(std::os::fd::OwnedFd),
    NotFound,
}

#[cfg(target_os = "linux")]
fn open_registry_root(root: &Path) -> Result<OpenRegistryRoot, FileRegistryReadError> {
    use std::path::Component;

    use nix::errno::Errno;
    use nix::fcntl::OFlag;
    use nix::sys::stat::Mode;

    if !root.is_absolute() {
        return Err(FileRegistryReadError::InvalidRoot);
    }

    let flags = OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
    let mut directory = nix::fcntl::open("/", flags, Mode::empty())
        .map_err(|_| FileRegistryReadError::InvalidRoot)?;
    for component in root.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => {
                directory = match nix::fcntl::openat(&directory, component, flags, Mode::empty()) {
                    Ok(directory) => directory,
                    Err(Errno::ENOENT) => return Ok(OpenRegistryRoot::NotFound),
                    Err(_) => return Err(FileRegistryReadError::InvalidRoot),
                };
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(FileRegistryReadError::InvalidRoot);
            }
        }
    }
    Ok(OpenRegistryRoot::Found(directory))
}

#[cfg(target_os = "linux")]
fn reopen_pinned_regular_file(
    path_file: &std::os::fd::OwnedFd,
    expected: &nix::libc::stat,
) -> Result<std::fs::File, FileRegistryReadError> {
    use std::os::fd::AsRawFd;

    use nix::fcntl::OFlag;
    use nix::sys::stat::Mode;
    use nix::sys::stat::SFlag;

    let proc_path = format!("/proc/self/fd/{}", path_file.as_raw_fd());
    let file = nix::fcntl::open(
        proc_path.as_str(),
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| FileRegistryReadError::InvalidFinalEntry)?;
    let actual =
        nix::sys::stat::fstat(&file).map_err(|_| FileRegistryReadError::InvalidFinalEntry)?;
    if actual.st_dev != expected.st_dev
        || actual.st_ino != expected.st_ino
        || SFlag::from_bits_truncate(actual.st_mode) != SFlag::S_IFREG
    {
        return Err(FileRegistryReadError::InvalidFinalEntry);
    }
    Ok(std::fs::File::from(file))
}

#[cfg(target_os = "linux")]
fn reserve_geometrically(
    bytes: &mut Vec<u8>,
    needed: usize,
    max_bytes: usize,
    capacity_growth: &mut impl FnMut(usize),
) -> Result<(), FileRegistryReadError> {
    if needed > max_bytes {
        return Err(FileRegistryReadError::FileTooLarge);
    }
    if needed <= bytes.capacity() {
        return Ok(());
    }

    let target = if bytes.capacity() == 0 {
        needed
    } else {
        needed.max(bytes.capacity().saturating_mul(2).min(max_bytes))
    };
    bytes
        .try_reserve_exact(target.saturating_sub(bytes.len()))
        .map_err(|_| FileRegistryReadError::AllocationFailed)?;
    if bytes.capacity() > max_bytes {
        bytes.shrink_to(max_bytes);
        if bytes.capacity() > max_bytes {
            return Err(FileRegistryReadError::AllocationFailed);
        }
    }
    capacity_growth(bytes.capacity());
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn read_file_registry_impl(
    _root: &Path,
    _path: &RegistryPath,
    _max_bytes: usize,
    _after_stat: impl FnOnce(),
    _capacity_growth: impl FnMut(usize),
) -> Result<FileRegistryRead, FileRegistryReadError> {
    Err(FileRegistryReadError::UnsupportedPlatform)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;

    use nix::errno::Errno;
    use nix::sys::stat::Mode;
    use nix::sys::stat::SFlag;
    use tempfile::TempDir;

    use super::*;

    fn module_path() -> RegistryPath {
        use buck2_bzlmod::ModuleKey;
        use buck2_bzlmod::ModuleName;
        use buck2_bzlmod::Version;

        RegistryPath::module_file(
            &ModuleKey::registry(
                ModuleName::parse("rules_go").unwrap(),
                Version::parse("0.50.1").unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn create_parent(root: &Path, path: &RegistryPath) {
        fs::create_dir_all(
            root.join(
                Path::new(path.as_str())
                    .parent()
                    .expect("registry path has a parent"),
            ),
        )
        .unwrap();
    }

    fn final_path(root: &Path, path: &RegistryPath) -> std::path::PathBuf {
        root.join(path.as_str())
    }

    #[test]
    fn reads_exact_file_bytes() {
        let root = TempDir::new().unwrap();
        let path = module_path();
        create_parent(root.path(), &path);
        let expected = b"module(name = \"rules_go\")\n\0raw";
        fs::write(final_path(root.path(), &path), expected).unwrap();

        assert_eq!(
            read_file_registry(root.path(), &path, expected.len()).unwrap(),
            FileRegistryRead::Found(Box::from(expected.as_slice()))
        );
    }

    #[test]
    fn missing_descendants_are_not_found() {
        let root = TempDir::new().unwrap();
        let path = module_path();
        create_parent(root.path(), &path);
        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap(),
            FileRegistryRead::NotFound
        );

        let root = TempDir::new().unwrap();
        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap(),
            FileRegistryRead::NotFound
        );
    }

    #[test]
    fn missing_or_deleted_registry_root_is_not_found() {
        let parent = TempDir::new().unwrap();
        let missing = parent.path().join("missing-registry");
        assert_eq!(
            read_file_registry(&missing, &RegistryPath::bazel_registry_json(), 1024).unwrap(),
            FileRegistryRead::NotFound
        );

        let deleted = TempDir::new().unwrap();
        let deleted_path = deleted.path().to_owned();
        deleted.close().unwrap();
        assert_eq!(
            read_file_registry(&deleted_path, &RegistryPath::bazel_registry_json(), 1024,).unwrap(),
            FileRegistryRead::NotFound
        );
    }

    #[test]
    fn non_directory_intermediate_entry_is_terminal() {
        let root = TempDir::new().unwrap();
        let path = module_path();
        fs::write(root.path().join("modules"), b"not a directory").unwrap();

        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap_err(),
            FileRegistryReadError::InvalidIntermediateEntry
        );
    }

    #[test]
    fn rejects_root_symlink() {
        assert_eq!(
            read_file_registry(
                Path::new("relative-registry-root"),
                &RegistryPath::bazel_registry_json(),
                1024,
            )
            .unwrap_err(),
            FileRegistryReadError::InvalidRoot
        );

        let parent = TempDir::new().unwrap();
        let actual = parent.path().join("actual");
        fs::create_dir(&actual).unwrap();
        let alias = parent.path().join("secret-root-alias");
        symlink(&actual, &alias).unwrap();

        assert_eq!(
            read_file_registry(&alias, &RegistryPath::bazel_registry_json(), 1024).unwrap_err(),
            FileRegistryReadError::InvalidRoot
        );

        let actual_registry = actual.join("registry");
        fs::create_dir(&actual_registry).unwrap();
        assert_eq!(
            read_file_registry(
                &alias.join("registry"),
                &RegistryPath::bazel_registry_json(),
                1024,
            )
            .unwrap_err(),
            FileRegistryReadError::InvalidRoot
        );
    }

    #[test]
    fn rejects_intermediate_and_final_symlinks() {
        let path = module_path();

        let root = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        fs::create_dir(root.path().join("modules")).unwrap();
        symlink(outside.path(), root.path().join("modules/rules_go")).unwrap();
        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap_err(),
            FileRegistryReadError::InvalidIntermediateEntry
        );

        let root = TempDir::new().unwrap();
        create_parent(root.path(), &path);
        let outside_file = outside.path().join("secret-file");
        fs::write(&outside_file, b"secret").unwrap();
        symlink(&outside_file, final_path(root.path(), &path)).unwrap();
        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap_err(),
            FileRegistryReadError::InvalidFinalEntry
        );
    }

    #[test]
    fn rejects_directory_fifo_and_socket_without_blocking() {
        let path = module_path();

        let root = TempDir::new().unwrap();
        create_parent(root.path(), &path);
        fs::create_dir(final_path(root.path(), &path)).unwrap();
        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap_err(),
            FileRegistryReadError::SpecialFinalEntry
        );

        let root = TempDir::new().unwrap();
        create_parent(root.path(), &path);
        nix::unistd::mkfifo(final_path(root.path(), &path).as_path(), Mode::S_IRUSR).unwrap();
        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap_err(),
            FileRegistryReadError::SpecialFinalEntry
        );

        let root = TempDir::new().unwrap();
        create_parent(root.path(), &path);
        let _listener = UnixListener::bind(final_path(root.path(), &path)).unwrap();
        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap_err(),
            FileRegistryReadError::SpecialFinalEntry
        );
    }

    #[test]
    fn rejects_character_device_when_creation_is_permitted() {
        let root = TempDir::new().unwrap();
        let path = module_path();
        create_parent(root.path(), &path);
        let device = final_path(root.path(), &path);
        match nix::sys::stat::mknod(
            &device,
            SFlag::S_IFCHR,
            Mode::S_IRUSR,
            nix::sys::stat::makedev(1, 3),
        ) {
            Ok(()) => {}
            Err(Errno::EPERM | Errno::EACCES | Errno::ENOTSUP) => return,
            Err(error) => panic!("unexpected mknod failure: {error}"),
        }

        assert_eq!(
            read_file_registry(root.path(), &path, 1024).unwrap_err(),
            FileRegistryReadError::SpecialFinalEntry
        );
    }

    #[test]
    fn rejects_size_from_metadata_and_growth_while_reading() {
        let root = TempDir::new().unwrap();
        let path = module_path();
        create_parent(root.path(), &path);
        let file_path = final_path(root.path(), &path);
        fs::write(&file_path, b"12345").unwrap();
        assert_eq!(
            read_file_registry(root.path(), &path, 4).unwrap_err(),
            FileRegistryReadError::FileTooLarge
        );

        fs::write(&file_path, b"1234").unwrap();
        assert_eq!(
            read_file_registry_impl(
                root.path(),
                &path,
                4,
                || {
                    let mut file = OpenOptions::new().append(true).open(&file_path).unwrap();
                    file.write_all(b"5").unwrap();
                },
                |_| {},
            )
            .unwrap_err(),
            FileRegistryReadError::FileTooLarge
        );
    }

    #[test]
    fn post_stat_growth_uses_geometric_capacity() {
        use std::cell::RefCell;

        const FILE_SIZE: usize = 512 * 1024;

        let root = TempDir::new().unwrap();
        let path = module_path();
        create_parent(root.path(), &path);
        let file_path = final_path(root.path(), &path);
        fs::write(&file_path, b"x").unwrap();
        let capacities = RefCell::new(Vec::new());

        let result = read_file_registry_impl(
            root.path(),
            &path,
            FILE_SIZE,
            || {
                let mut file = OpenOptions::new().append(true).open(&file_path).unwrap();
                file.write_all(&vec![b'x'; FILE_SIZE - 1]).unwrap();
            },
            |capacity| capacities.borrow_mut().push(capacity),
        )
        .unwrap();

        let FileRegistryRead::Found(bytes) = result else {
            panic!("grown file was not found");
        };
        assert_eq!(bytes.len(), FILE_SIZE);
        let capacities = capacities.into_inner();
        assert!(capacities.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(capacities.iter().all(|capacity| *capacity <= FILE_SIZE));
        assert!(capacities.len() <= 8, "capacity growths: {capacities:?}");
        assert!(capacities.len() < FILE_SIZE / 8192);
    }

    #[test]
    fn reads_open_descriptor_after_path_replacement() {
        let root = TempDir::new().unwrap();
        let path = module_path();
        create_parent(root.path(), &path);
        let file_path = final_path(root.path(), &path);
        let replacement = root.path().join("replacement");
        fs::write(&file_path, b"opened inode").unwrap();
        fs::write(&replacement, b"replacement inode").unwrap();

        assert_eq!(
            read_file_registry_impl(
                root.path(),
                &path,
                1024,
                || {
                    fs::rename(&replacement, &file_path).unwrap();
                },
                |_| {},
            )
            .unwrap(),
            FileRegistryRead::Found(Box::from(b"opened inode".as_slice()))
        );
    }

    #[test]
    fn public_errors_never_echo_roots_paths_or_os_errors() {
        let forbidden = [
            "secret-registry-root",
            "rules_go",
            "MODULE.bazel",
            "Permission denied",
            "No such file",
        ];
        let errors = [
            FileRegistryReadError::UnsupportedPlatform,
            FileRegistryReadError::InvalidRoot,
            FileRegistryReadError::InvalidIntermediateEntry,
            FileRegistryReadError::InvalidFinalEntry,
            FileRegistryReadError::SpecialFinalEntry,
            FileRegistryReadError::FileTooLarge,
            FileRegistryReadError::ReadFailed,
            FileRegistryReadError::AllocationFailed,
        ];
        for error in errors {
            for formatted in [error.to_string(), format!("{error:?}")] {
                for forbidden in forbidden {
                    assert!(!formatted.contains(forbidden), "leaked {forbidden:?}");
                }
            }
        }

        let secret_root = Path::new("relative-secret-registry-root");
        let error = read_file_registry(secret_root, &module_path(), 1024).unwrap_err();
        assert!(!error.to_string().contains("secret-registry-root"));
        assert!(!format!("{error:?}").contains("secret-registry-root"));
    }
}
