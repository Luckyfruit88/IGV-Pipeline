from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any


_GIT_OBJECT = re.compile(r"^[a-f0-9]{40}$")
_RELEASE_TAG = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+$")


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "git command failed"
        raise ValueError(f"release tag verification failed: {detail}")
    return completed.stdout.strip()


def verify_release_tag(
    repository: str | Path,
    expected_commit: str | None = None,
    *,
    tag: str = "v3.0.0",
) -> dict[str, Any]:
    """Resolve a standard release tag using only Git source identity.

    ``expected_commit`` is an optional CI race guard.  It is source identity, not
    a runtime gate, and callers may omit it when verifying an existing release.
    """

    repo_value = Path(repository).expanduser()
    if repo_value.is_symlink() or not repo_value.resolve(strict=True).is_dir():
        raise ValueError("release repository must be a regular non-symlink directory")
    if not _RELEASE_TAG.fullmatch(tag):
        raise ValueError("release tag must use vMAJOR.MINOR.PATCH")
    if expected_commit is not None and not _GIT_OBJECT.fullmatch(expected_commit):
        raise ValueError("expected release commit must be a full lowercase Git object")

    repo = repo_value.resolve(strict=True)
    tag_ref = f"refs/tags/{tag}"
    tagged_commit = _git(repo, "rev-parse", "--verify", f"{tag_ref}^{{commit}}")
    tagged_tree = _git(repo, "rev-parse", "--verify", f"{tagged_commit}^{{tree}}")
    if not _GIT_OBJECT.fullmatch(tagged_commit) or not _GIT_OBJECT.fullmatch(tagged_tree):
        raise ValueError("release tag resolved to an invalid Git object")
    if expected_commit is not None and tagged_commit != expected_commit:
        raise ValueError("release tag does not point to the expected source commit")
    return {
        "schema_version": "3.0-release-tag-verification",
        "status": "PASS",
        "tag": tag,
        "tagged_commit": tagged_commit,
        "tagged_tree": tagged_tree,
    }
