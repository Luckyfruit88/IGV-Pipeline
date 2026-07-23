from __future__ import annotations

from pathlib import Path

import pytest

from ssqtl_igv import release_v3
from ssqtl_igv.release_v3 import verify_release_tag


def test_release_tag_resolves_without_signature_or_qualification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commit, tree = "a" * 40, "b" * 40
    calls: list[tuple[str, ...]] = []

    def git(_repo: Path, *arguments: str) -> str:
        calls.append(arguments)
        if arguments[-1] == "refs/tags/v3.0.0^{commit}":
            return commit
        if arguments[-1] == f"{commit}^{{tree}}":
            return tree
        raise AssertionError(arguments)

    monkeypatch.setattr(release_v3, "_git", git)

    result = verify_release_tag(tmp_path, commit)

    assert result == {
        "schema_version": "3.0-release-tag-verification",
        "status": "PASS",
        "tag": "v3.0.0",
        "tagged_commit": commit,
        "tagged_tree": tree,
    }
    assert not any(call[:2] == ("tag", "-v") for call in calls)


def test_release_tag_rejects_unexpected_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected, tagged, tree = "a" * 40, "c" * 40, "b" * 40

    def git(_repo: Path, *arguments: str) -> str:
        if arguments[-1] == "refs/tags/v3.0.0^{commit}":
            return tagged
        if arguments[-1] == f"{tagged}^{{tree}}":
            return tree
        raise AssertionError(arguments)

    monkeypatch.setattr(release_v3, "_git", git)

    with pytest.raises(ValueError, match="expected source commit"):
        verify_release_tag(tmp_path, expected)


@pytest.mark.parametrize("tag", ["3.0.0", "latest", "v3"])
def test_release_tag_requires_semver_name(tmp_path: Path, tag: str) -> None:
    with pytest.raises(ValueError, match="vMAJOR.MINOR.PATCH"):
        verify_release_tag(tmp_path, tag=tag)
