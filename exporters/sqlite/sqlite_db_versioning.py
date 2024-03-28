import os
import shutil
import getpass
from contextlib import contextmanager
from dvc.repo import Repo
from pathlib import Path
import git
import logging
import warnings
from dataclasses import dataclass
from textwrap import indent


@dataclass(order=True)
class MetadataVersionID:
    major: int
    minor: int
    patch: int
    info: str
    is_dirty: bool

    def get_next_version(self, bump_major=False, bump_minor=False, info=None):
        new_major = self.major
        new_minor = self.minor
        new_patch = self.patch
        if bump_major:
            new_major += 1
            new_minor = 0
            new_patch = 0
        elif bump_minor:
            new_minor += 1
            new_patch = 0
        else:
            new_patch += 1
        new_info = info or ""

        # A new version should never be created as "dirty"
        new_version = self.__class__(new_major, new_minor, new_patch, new_info, False)
        return new_version

    def set_info(self, info):
        self.info = info

    @classmethod
    def from_string(cls, version_string):
        if version_string[0] == "v":
            version_string = version_string[1:]
        major, minor, patch = (
            int(i) if i.isdigit() else i for i in version_string.split(".")
        )
        is_dirty = False
        info = ""
        if isinstance(patch, str):
            if patch.endswith("-dirty"):
                is_dirty = True
                patch = patch.replace("-dirty", "")

            if "-" in patch:
                patch, *info = patch.split("-")
                info = "-".join(info)
            patch = int(patch)

        return cls(major, minor, patch, info, is_dirty)

    def __str__(self):
        tag = f"v{self.major}.{self.minor}.{self.patch}"
        if self.info:
            tag += f"-{self.info}"
        return tag

    @property
    def tag(self):
        return str(self)


class VersioningMetadataDatabase:
    def __init__(self, db_file, use_scratch=False):
        self.use_scratch = use_scratch
        self.db_file = Path(db_file).resolve().absolute()
        self.dvc_file = self.db_file.with_suffix(".db.dvc")
        self.path = self.db_file.parent
        # Make sure there is a .dvc and .git folder under the path
        self.qualify_environment()
        self.repo = git.Repo(self.path)
        self.dvc = Repo(self.path)
        self.__warnings = set()
        if use_scratch:
            self.scratch_path = Path("/scratch", *self.path.parts[1:])
            self.__set_up_scratch_environment()
        # Before doing anything, make sure we are all up to date.
        self.pull()
        # Fix shared /tmp dir because sometimes it's permissions are wrong
        if self.use_scratch:
            fix_tmp_dir = Path(self.original_dvc.root_dir, ".dvc/tmp")
        else:
            fix_tmp_dir = Path(self.dvc.root_dir, ".dvc/tmp")
        fix_dvc_dir_perms(fix_tmp_dir)

    def __set_up_scratch_environment(self):
        if self.dvc_file_is_dirty():
            # If the standard location dvc file is dirty, then we can't use scratch.
            self.use_scratch = False
            return
        try:
            if not self.scratch_path.exists():
                self.scratch_path.parent.mkdir(parents=True, exist_ok=True)
                self.repo.clone_from(
                    next(self.repo.remotes.origin.urls), self.scratch_path
                )
            self.scratch_repo = git.Repo(self.scratch_path)
            self.scratch_dvc = Repo(self.scratch_path)
            self.scratch_db_file = self.scratch_path / self.db_file.name
            self.scratch_dvc_file = self.scratch_path / self.dvc_file.name
            self.scratch_repo.remotes.origin.pull()
            self.scratch_dvc.pull(force=True)
            fix_dvc_dir_perms(Path(self.scratch_dvc.dvc_dir))
            print(f"Using local /scratch disk as metadata database target.")
            self.original_repo = self.repo
            self.original_dvc = self.dvc
            self.original_db_file = self.db_file
            self.original_dvc_file = self.dvc_file
            self.repo = self.scratch_repo
            self.dvc = self.scratch_dvc
            self.db_file = self.scratch_db_file
            self.dvc_file = self.scratch_dvc_file
        except Exception:
            # If anything goes weird with using scratch, just fall back to not using it
            # this doesn't need to prevent work
            self.warn("Couldn't use /scratch disk. Proceeding as normal.")
            self.use_scratch = False

    def warn(self, warning, **kwargs):
        """Emit unique warning messages, caching to prevent duplicate warnings.

        Args:
            warning (str): Warning message to emit if not already emitted.
            **kwargs: Additional keyword arguments to pass to warnings.warn.
        """
        if not warning in self.__warnings:
            warnings.warn(warning, **kwargs)
            self.__warnings.add(warning)

    def clear_warnings(self):
        """Clear the warning cache."""
        self.__warnings = set()

    @property
    def current_version(self) -> MetadataVersionID:
        return self.get_current_version()

    @property
    def next_version(self) -> MetadataVersionID:
        return self.current_version.get_next_version(
            self.bump_major_version(), self.bump_minor_version()
        )

    def qualify_environment(self):
        """Ensure all needed folders are present in the repo.

        Raises:
            FileNotFoundError: If the database file or .dvc or .git folders are missing.
        """
        if not self.path.exists():
            raise FileNotFoundError(f"{self.path} does not exist")
        if not self.path.joinpath(".dvc").exists():
            raise FileNotFoundError(f"{self.path} is not a dvc repo")
        if not self.path.joinpath(".git").exists():
            raise FileNotFoundError(f"{self.path} is not a git repo")

    def get_current_version(self) -> MetadataVersionID:
        """Retrieve the current version of the metadata database.

        Args:
            exact_match (bool, optional): Require the current commit to exactly match a version tag.
              Defaults to True.

        Returns:
            MetadataVersion: Current version of the metadata database.
        """
        version_string = self.query_version_tag()
        current_version = MetadataVersionID.from_string(version_string)
        if current_version.is_dirty:
            self.warn(
                f"Metadata Database tracking is in a dirty state:\n{self.repo_diff()}",
                stacklevel=4,
            )
        elif self.dvc_file_is_dirty():
            current_version.is_dirty = True
        if current_version.info:
            self.warn(
                f"This is an un-versioned commit. {current_version}",
                stacklevel=4,
            )
        return current_version

    def query_version_tag(self):
        """Retrieve the current version of the metadata database.

        Args:
            exact_match (bool, optional): Require the current commit to exactly match a version tag.
              Defaults to True.

        Returns:
            str: Current version of the metadata database.
        """
        if self.repo.tags:
            return self.repo.git.describe(tags=True, dirty=True)
        else:
            # No tags have been created yet. Gotta start somewhere!
            return "v0.0.0"

    def bump_major_version(self):
        # XXX Check if there is a sentinel file in the repo indicating that
        # a live deploy has been done. If so then we should return true, indicating
        # that we should bump the major version, and then delete the sentinel file.
        # Also check if there is a minor version sentinel file in case both a live and
        # internal deploy have been done since the last export.
        return False

    def bump_minor_version(self):
        # XXX Check if there is a sentinel file in the repo indicating that
        # an internal deploy has been done. If so then we should return true, indicating
        # that we should bump the minor version, and then delete the sentinel file.
        return False

    def dvc_file_is_dirty(self):
        """Check if the dvc file is dirty.

        A dirty DVC file indicates that there are uncommitted changes to the
        metadata database.

        Returns:
            bool: True if the dvc file is dirty.
        """
        with cwd(self.dvc.root_dir):
            # This was previously checking `self.dvc.status()` to check the
            # state of the git-committed version against the local version
            # (even if that local version was dvc-committed). This ended up
            # causing trouble with PyGit2 being mad about shared git repos.
            # See: https://trello.com/c/LEPEbuFy
            # Needed to add a git repo `is_dirty` check to account for situations
            # where a dvc add has been run, but not the accompanying git-commit.
            if self.dvc.status() or self.repo.is_dirty():
                return True
            else:
                return False

    def reset_to_current_version(self):
        """Undo any changes to the metadata database since the last export."""
        if self.repo.is_dirty():
            self.repo.git.reset(hard=True)
        self.dvc.checkout(str(self.db_file), force=True)

    def repo_diff(self):
        """Summary of changes to files in the tracking repo since the last commit.

        Returns:
            str: formatted list of files changed.
        """
        diff = self.repo.git.status(short=True)
        return indent(diff, ">   ", lambda _: True)

    def reset_to_version(self, version, force=False):
        """Reset to the given version the metadata database.

        This will grab the dvc tracking file for the given version and stage it
        to be committed, but this method WILL NOT commit the changes.

        Args:
            version (str | MetadataVersion): Version to reset to.
            force (bool, optional): Reset even if tracking file is in a dirty state.
              Defaults to False.

        Raises:
            RuntimeError: Metadata tracking file is in a dirty state.
        """
        if isinstance(version, MetadataVersionID):
            version = str(version)
        if self.repo.is_dirty():
            if force:
                self.repo.git.reset(hard=True)
            else:
                raise RuntimeError(
                    f"Metadata Tracking file is in a dirty state.\n{self.repo_diff()}\n"
                    "Rerun with --force to blow away any uncommitted changes."
                )
        self.repo.git.checkout(version, f"{self.dvc_file}")
        self.dvc.pull()
        self.dvc.checkout(str(self.db_file), version)

    def pull(self):
        """Pull the latest changes from the remote repo."""
        self.repo.remotes.origin.pull()
        if not self.dvc_file_is_dirty():
            self.dvc.pull()

    def commit(self, message, info=None, tag_version=True):
        """Commit the metadata database to the tracking repo.

        If there are no changes to be committed, this method will do nothing.

        The warnings cache will be cleared after the commit.

        Args:
            message (str): Commit message.
            info (str, optional): Extra info to append to tag. Defaults to None.
            tag_version (bool, optional): Tag the commit with a new version.

        Raises:
            git.GitCommandError: An error occurred while committing the new version.
        """
        if self.dvc_file_is_dirty() or self.current_version.info:
            if tag_version:
                new_commit = self.__commit_new_version(message, info)
            else:
                new_commit = self.__commit_without_version(message)

            self.repo.remotes.origin.push().raise_if_error()
            self.dvc.push()

            # Clear the cached warnings here since we are starting new work
            self.clear_warnings()

            # Regenerate the current_version object
            self.get_current_version()

        else:
            print(
                f"Current version is the same as {self.current_version}\n"
                "No commit needed."
            )
            new_commit = None
        if self.repo.is_dirty(str(self.dvc_file)):
            self.warn(
                f"Metadata Database tracking is in a dirty state:\n{self.repo_diff()}",
                stacklevel=3,
            )
        if self.use_scratch:
            self.__pull_to_orig(new_commit)

    def __commit_without_version(self, message):
        """Commit the metadata database without tagging it as a version.

        This is for situations where the metadata database is being updated
        but we don't want the "version" it creates to be usable, such as when
        the export process hits an error. This will commit the changes that
        were made up to that point, but will not declare a version.

        Args:
            message (str): Commit message.
        """

        # Prep the next version tag based on current state
        self.dvc.add(str(self.db_file), quiet=True, desc="UNVERSIONED")
        new_commit = self.repo.index.commit(message)
        self.repo.remotes["origin"].push().raise_if_error()
        self.warn(f"Committed un-versioned changes to the metadata database.")

    def __commit_new_version(self, message, info=None):
        """Commit the new version of the metadata database.

        If there are no changes to be committed, this method will do nothing.

        The warnings cache will be cleared after the commit.

        Args:
            message (str): Commit message.
            info (str, optional): Extra info to append to tag. Defaults to None.

        Raises:
            git.GitCommandError: An error occurred while committing the new version.
        """
        # Prep the next version tag based on current state
        new_tag = self.next_version.tag
        self.dvc.add(str(self.db_file), quiet=True, desc=new_tag)
        new_commit = self.repo.index.commit(message)
        if info:
            new_tag += f"-{info}"
        git_tag = self.repo.create_tag(new_tag)
        try:
            assert new_tag == self.current_version.tag
        except AssertionError:
            raise git.GitCommandError(
                f"New tag {new_tag} was not properly applied."
                " Please contact IT for assistance."
            )
        self.repo.remotes["origin"].push(git_tag).raise_if_error()
        print(f"Committed new Metadata Version {self.current_version}")
        return new_commit

    def __pull_to_orig(self, new_commit):
        """Pull latest changes into the original repo.

        This method only runs if the use_scratch is True.

        Args:
            new_commit (git.Commit or None): New commit to pull or None if
                no commit was made.
        """

        if not self.use_scratch or not new_commit:
            return
        self.original_repo.remotes.origin.pull()
        self.original_dvc.pull(force=True)


@contextmanager
def cwd(path):
    old_pwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_pwd)


def fix_dvc_dir_perms(directory):
    if not directory.exists():
        raise FileNotFoundError(f"Directory {directory} does not exist")
    if not directory.is_dir():
        raise NotADirectoryError(f"{directory} is not a directory")
    if getpass.getuser() == directory.owner():
        logging.info(f"fixing permissions in {directory}.")
        shutil.chown(directory, group="mpcipums")
        os.chmod(directory, 0o2775)
        for f in Path(directory).iterdir():
            if f.suffix == ".db":
                os.chmod(f, 0o0664)
            elif f.is_dir():
                fix_dvc_dir_perms(f)
