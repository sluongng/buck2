/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use buck2_core::soft_error;
use buck2_data::VersionControlRevision;
use buck2_events::dispatch::EventDispatcher;
use buck2_fs::async_fs_util;
use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
use buck2_fs::paths::forward_rel_path::ForwardRelativePath;
use buck2_util::properly_reaped_child::reap_on_drop_command;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio::sync::OnceCell;
use tokio_stream::StreamExt;

/// Spawn tasks to collect version control information
/// and return a droppable handle that will cancel them on drop.
pub(crate) fn spawn_version_control_collector(
    dispatch: EventDispatcher,
    repo_root: AbsNormPathBuf,
) -> AbortOnDropHandle {
    let handle = tokio::spawn(async move {
        let mut tasks = FuturesUnordered::<BoxFuture<VersionControlRevision>>::new();

        tasks.push(Box::pin(create_revision_data(
            &repo_root,
            RevisionDataType::CurrentRevision,
        )));
        tasks.push(Box::pin(create_revision_data(
            &repo_root,
            RevisionDataType::Status,
        )));

        while let Some(event) = tasks.next().await {
            if let Some(error) = &event.command_error {
                soft_error!(
                    "spawn_version_control_collector_failed",
                    buck2_error::buck2_error!(buck2_error::ErrorTag::Input, "{}", error),
                    quiet: true
                )
                .ok();
            }

            dispatch.instant_event(event);
        }
    });

    AbortOnDropHandle { handle }
}

/// Abort the underlying task on drop.
pub(crate) struct AbortOnDropHandle {
    pub handle: tokio::task::JoinHandle<()>,
}

impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RepoVcs {
    Hg,
    Git,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
enum RevisionDataType {
    CurrentRevision,
    Status,
}

async fn create_revision_data(
    repo_root: &AbsNormPathBuf,
    revision_type: RevisionDataType,
) -> buck2_data::VersionControlRevision {
    let mut revision = buck2_data::VersionControlRevision::default();
    match repo_type(repo_root).await {
        Ok(repo_vcs) => match repo_vcs {
            RepoVcs::Hg => create_hg_data(&mut revision, revision_type, repo_root).await,
            RepoVcs::Git => create_git_data(&mut revision, revision_type, repo_root).await,
            RepoVcs::Unknown => {
                revision.command_error = Some("Unknown repository type".to_owned());
            }
        },
        Err(e) => {
            revision.command_error = Some(format!("Failed to get repository type: {e:#}"));
        }
    }
    revision
}

async fn create_git_data(
    revision: &mut buck2_data::VersionControlRevision,
    revision_type: RevisionDataType,
    repo_root: &AbsNormPathBuf,
) {
    match revision_type {
        RevisionDataType::CurrentRevision => get_git_revision(revision, repo_root).await,
        RevisionDataType::Status => get_git_status(revision, repo_root).await,
    }
}

async fn get_git_revision(
    revision: &mut buck2_data::VersionControlRevision,
    repo_root: &AbsNormPathBuf,
) {
    let stdout = match run_git(repo_root, &["rev-parse", "HEAD"], "git rev-parse HEAD").await {
        Ok(stdout) => stdout,
        Err(e) => {
            revision.command_error = Some(e);
            return;
        }
    };
    let hash = stdout.trim();
    if hash.is_empty() {
        revision.command_error =
            Some("Command 'git rev-parse HEAD' returned empty output".to_owned());
        return;
    }
    // The proto field name is historical. BuildBuddy consumes this as the
    // source-control commit hash regardless of whether the repo is Hg or Git.
    revision.hg_revision = Some(hash.to_owned());
}

async fn get_git_status(
    revision: &mut buck2_data::VersionControlRevision,
    repo_root: &AbsNormPathBuf,
) {
    let stdout = match run_git(
        repo_root,
        &["status", "--porcelain"],
        "git status --porcelain",
    )
    .await
    {
        Ok(stdout) => stdout,
        Err(e) => {
            revision.command_error = Some(e);
            return;
        }
    };
    revision.has_local_changes = Some(!stdout.trim().is_empty());
}

async fn run_git(
    repo_root: &AbsNormPathBuf,
    args: &[&str],
    command_description: &str,
) -> Result<String, String> {
    let Some(repo_root) = repo_root.as_path().to_str() else {
        return Err(format!(
            "Cannot run '{command_description}' because the repository path is not valid UTF-8"
        ));
    };

    let mut git_args = Vec::with_capacity(args.len() + 2);
    git_args.push("-C");
    git_args.push(repo_root);
    git_args.extend_from_slice(args);

    let output = match reap_on_drop_command("git", &git_args, None) {
        Ok(command) => command.output().await,
        Err(e) => {
            return Err(format!(
                "reap_on_drop_command for '{command_description}' failed: {e}"
            ));
        }
    };

    match output {
        Ok(result) => {
            if !result.status.success() {
                let stderr = std::str::from_utf8(&result.stderr)
                    .map_err(|e| format!("{command_description} stderr is not utf8: {e}"))?;
                return Err(format!(
                    "Command '{command_description}' failed with error code {}; stderr: {}",
                    result.status, stderr
                ));
            }

            std::str::from_utf8(&result.stdout)
                .map(|stdout| stdout.trim().to_owned())
                .map_err(|e| format!("{command_description} stdout is not utf8: {e}"))
        }
        Err(e) => Err(format!(
            "Command '{command_description}' failed with error: {e:?}"
        )),
    }
}

async fn create_hg_data(
    revision: &mut buck2_data::VersionControlRevision,
    revision_type: RevisionDataType,
    repo_root: &AbsNormPathBuf,
) {
    match revision_type {
        RevisionDataType::CurrentRevision => get_hg_revision(revision, repo_root).await,
        RevisionDataType::Status => get_hg_status(revision).await,
    }
}

async fn get_hg_revision(
    revision: &mut buck2_data::VersionControlRevision,
    repo_root: &AbsNormPathBuf,
) {
    // The contents of dirstate may be arbitrarily large, but the id is always
    // in the first 20 bytes, so we only need to read the first 20 bytes
    let mut buffer = [0; 20];
    let dirstate = repo_root
        .join(ForwardRelativePath::new(".hg").unwrap())
        .join(ForwardRelativePath::new("dirstate").unwrap());

    if let Err(e) = async_fs_util::read(&dirstate, &mut buffer).await {
        revision.command_error = Some(format!(
            "Failed to read the first 20 bytes of {}: {:#}",
            dirstate.display(),
            e.categorize_internal(),
        ));
        return;
    }

    let curr_revision = buffer.iter().map(|b| format!("{b:02x}")).collect();
    revision.hg_revision = Some(curr_revision);
}

async fn get_hg_status(revision: &mut buck2_data::VersionControlRevision) {
    // `hg status` returns if there are any local changes
    let status_output = match reap_on_drop_command("hg", &["status"], Some(&[("HGPLAIN", "1")])) {
        Ok(command) => command.output().await,
        Err(e) => {
            revision.command_error =
                Some(format!("reap_on_drop_command for `hg status` failed: {e}"));
            return;
        }
    };

    match status_output {
        Ok(result) => {
            if !result.status.success() {
                let stderr = match std::str::from_utf8(&result.stderr) {
                    Ok(s) => s,
                    Err(e) => {
                        revision.command_error = Some(format!("hg status stderr is not utf8: {e}"));
                        return;
                    }
                };
                revision.command_error = Some(format!(
                    "Command `hg status` failed with error code {}; stderr: {}",
                    result.status, stderr
                ));
                return;
            }

            let stdout = match std::str::from_utf8(&result.stdout) {
                Ok(s) => s.trim(),
                Err(e) => {
                    revision.command_error = Some(format!("hg status stdout is not utf8: {e}"));
                    return;
                }
            };
            revision.has_local_changes = Some(!stdout.is_empty());
        }
        Err(e) => {
            revision.command_error = Some(format!("Command `hg status` failed with error: {e:?}"));
        }
    };
}

async fn detect_repo_type(repo_root: &AbsNormPathBuf) -> buck2_error::Result<RepoVcs> {
    let (hg_metadata, git_metadata) = tokio::join!(
        async_fs_util::metadata(repo_root.join(ForwardRelativePath::new(".hg").unwrap())),
        async_fs_util::metadata(repo_root.join(ForwardRelativePath::new(".git").unwrap()))
    );

    let is_hg = hg_metadata.is_ok_and(|output| output.is_dir());
    let is_git = git_metadata.is_ok_and(|output| output.is_dir() || output.is_file());

    if is_hg {
        Ok(RepoVcs::Hg)
    } else if is_git {
        Ok(RepoVcs::Git)
    } else {
        Ok(RepoVcs::Unknown)
    }
}

async fn repo_type(repo_root: &AbsNormPathBuf) -> buck2_error::Result<&'static RepoVcs> {
    static REPO_TYPE: OnceCell<buck2_error::Result<RepoVcs>> = OnceCell::const_new();
    REPO_TYPE
        .get_or_init(|| detect_repo_type(repo_root))
        .await
        .as_ref()
        .map_err(|e| e.clone())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;
    use std::process::Command;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use super::*;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "buck2-git-revision-test-{}-{timestamp}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn abs_norm_path(&self) -> AbsNormPathBuf {
            AbsNormPathBuf::new(self.path.clone()).unwrap()
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ignored = fs::remove_dir_all(&self.path);
        }
    }

    fn git_available() -> bool {
        Command::new("git")
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success())
    }

    fn run_git(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed with status {}; stderr: {}",
            args,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn git_revision_data_reports_head_and_dirty_status() {
        if !git_available() {
            return;
        }

        let repo = TestDir::new();
        run_git(repo.path(), &["init", "-q"]);
        run_git(repo.path(), &["config", "user.email", "buck2@example.com"]);
        run_git(repo.path(), &["config", "user.name", "Buck2 Test"]);
        fs::write(repo.path().join("BUCK"), "").unwrap();
        run_git(repo.path(), &["add", "BUCK"]);
        run_git(repo.path(), &["commit", "-qm", "initial"]);
        let expected_revision = run_git(repo.path(), &["rev-parse", "HEAD"]);

        let repo_root = repo.abs_norm_path();
        let mut revision = buck2_data::VersionControlRevision::default();
        create_git_data(&mut revision, RevisionDataType::CurrentRevision, &repo_root).await;
        assert_eq!(revision.command_error, None);
        assert_eq!(
            revision.hg_revision.as_deref(),
            Some(expected_revision.as_str())
        );

        let mut status = buck2_data::VersionControlRevision::default();
        create_git_data(&mut status, RevisionDataType::Status, &repo_root).await;
        assert_eq!(status.command_error, None);
        assert_eq!(status.has_local_changes, Some(false));

        fs::write(repo.path().join("BUCK"), "# changed").unwrap();
        let mut status = buck2_data::VersionControlRevision::default();
        create_git_data(&mut status, RevisionDataType::Status, &repo_root).await;
        assert_eq!(status.command_error, None);
        assert_eq!(status.has_local_changes, Some(true));
    }

    #[tokio::test]
    async fn detect_repo_type_accepts_git_file() {
        let repo = TestDir::new();
        fs::write(repo.path().join(".git"), "gitdir: ../real-git-dir").unwrap();

        assert_eq!(
            detect_repo_type(&repo.abs_norm_path()).await.unwrap(),
            RepoVcs::Git
        );
    }
}
