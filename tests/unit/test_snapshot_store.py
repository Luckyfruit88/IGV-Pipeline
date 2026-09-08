from __future__ import annotations

import multiprocessing
import os
from pathlib import Path

import pytest

from ssqtl_igv import snapshot_store
from ssqtl_igv.project_admission_v3 import _validate_snapshot_product, merge_snapshot_outputs
from ssqtl_igv.snapshot_store import snapshot_view
from test_pull_run_v3 import _snapshot_product


def _crash_publication(root, incoming, after_pointer):
    original = snapshot_store._set_current
    def interrupted(store, generation):
        if after_pointer:
            original(store, generation)
        os._exit(77)
    snapshot_store._set_current = interrupted
    merge_snapshot_outputs(root, incoming)


def _crash_migration(root, incoming):
    original = snapshot_store.os.replace
    def interrupted(source, target):
        original(source, target)
        if Path(source) == root / "snapshots":
            os._exit(77)
    snapshot_store.os.replace = interrupted
    merge_snapshot_outputs(root, incoming)


def _run_crash(target, *args):
    child = multiprocessing.get_context("fork").Process(target=target, args=args)
    child.start()
    child.join(10)
    if child.is_alive():
        child.kill()
        child.join()
        pytest.fail("publication probe hung")
    assert child.exitcode == 77


@pytest.mark.parametrize("after_pointer", [False, True])
def test_hard_exit_exposes_complete_old_or_new_generation(tmp_path, after_pointer):
    initial, incoming, root = tmp_path / "first", tmp_path / "next", tmp_path / "output"
    _snapshot_product(initial, [(1, "case_1", "chr1", b"one")])
    _snapshot_product(incoming, [(2, "case_2", "chr1", b"two")])
    merge_snapshot_outputs(root, initial)
    pinned = snapshot_view(root)
    _run_crash(_crash_publication, root, incoming, after_pointer)
    rows, _ = _validate_snapshot_product(root)
    assert len(rows) == (2 if after_pointer else 1)
    assert (pinned / "snapshots/chr1/case_1.png").read_bytes() == b"one"
    merge_snapshot_outputs(root, incoming)
    assert len(_validate_snapshot_product(root)[0]) == 2
    assert (root / "snapshots/chr1/case_2.png").read_bytes() == b"two"


def test_interrupted_legacy_migration_recovers_without_rendering(tmp_path):
    root, incoming = tmp_path / "output", tmp_path / "next"
    _snapshot_product(root, [(1, "case_1", "chr1", b"one")])
    _snapshot_product(incoming, [(2, "case_2", "chr1", b"two")])
    _run_crash(_crash_migration, root, incoming)
    assert len(_validate_snapshot_product(root)[0]) == 1
    merge_snapshot_outputs(root, incoming)
    assert len(_validate_snapshot_product(root)[0]) == 2
    assert (root / "snapshots/chr1/case_1.png").read_bytes() == b"one"
    assert not (root / ".igv-pipeline/snapshot-store/bootstrap.json").exists()


def test_append_never_recopies_old_png_payloads(tmp_path, monkeypatch):
    initial, incoming, root = tmp_path / "first", tmp_path / "next", tmp_path / "output"
    _snapshot_product(initial, [(1, "case_1", "chr1", b"one")])
    _snapshot_product(incoming, [(2, "case_2", "chr1", b"two")])
    merge_snapshot_outputs(root, initial)
    calls = []
    original = snapshot_store.shutil.copyfile
    def copy(source, target, *args, **kwargs):
        calls.append(Path(source).read_bytes())
        return original(source, target, *args, **kwargs)
    monkeypatch.setattr(snapshot_store.shutil, "copyfile", copy)
    merge_snapshot_outputs(root, incoming)
    merge_snapshot_outputs(root, incoming)
    assert calls == [b"two"]
    assert (initial / "snapshots/chr1/case_1.png").stat().st_mode & 0o200


def test_generation_pointer_and_metadata_cannot_escape_or_drift(tmp_path):
    initial, root = tmp_path / "first", tmp_path / "output"
    _snapshot_product(initial, [(1, "case_1", "chr1", b"one")])
    merge_snapshot_outputs(root, initial)
    view = snapshot_view(root)
    (view / "snapshots.tsv").write_text("corrupt")
    with pytest.raises(ValueError, match="checksum differs"):
        snapshot_view(root)
    pointer = root / ".igv-pipeline/snapshot-store/CURRENT"
    pointer.unlink()
    pointer.symlink_to("../../../../../elsewhere")
    with pytest.raises(ValueError, match="target is unsafe"):
        snapshot_view(root)


def test_flat_export_preserves_payloads_and_never_overwrites(tmp_path):
    from ssqtl_igv.snapshot_store import export_snapshot_outputs
    initial, root, exported = tmp_path / "first", tmp_path / "output", tmp_path / "export"
    _snapshot_product(initial, [(1, "case_1", "chr1", b"one")])
    merge_snapshot_outputs(root, initial)
    pinned = snapshot_view(root)
    result = export_snapshot_outputs(root, exported)
    assert result["source_generation"] == pinned.name
    assert (exported / "snapshots/chr1/case_1.png").read_bytes() == b"one"
    assert not any(p.is_symlink() for p in exported.rglob("*"))
    assert len(_validate_snapshot_product(exported)[0]) == 1
    with pytest.raises(FileExistsError):
        export_snapshot_outputs(root, exported)
    assert snapshot_view(root) == pinned


def test_multiple_failure_reasons_remain_one_failed_case(tmp_path):
    from ssqtl_igv.snapshot_store import FAILURE_FIELDS, SNAPSHOT_FIELDS
    from ssqtl_igv.utils import write_tsv
    source, target = tmp_path / "batch", tmp_path / "collection"
    source.mkdir()
    (source / "snapshots").mkdir()
    rows = [{"manifest_order": "1", "task_id": "case_1", "chromosome": "chr1",
             "relative_path": "", "sha256": "", "status": "CASE_FAILED"}]
    failures = [{"manifest_order": "1", "task_id": "case_1", "chromosome": "chr1",
                 "failure_code": code, "message": code, "input_fingerprint": "a" * 64} for code in ("FIRST_FAILURE", "SECOND_FAILURE")]
    write_tsv(source / "snapshots.tsv", SNAPSHOT_FIELDS, rows)
    write_tsv(source / "failed_cases.tsv", FAILURE_FIELDS, failures)
    (source / "run_summary.json").write_text("{}")
    merge_snapshot_outputs(target, source)
    observed, failed = _validate_snapshot_product(target)
    assert observed == rows and failed == failures
    import json
    assert json.loads((target / "run_summary.json").read_text())["failed_case_count"] == 1
