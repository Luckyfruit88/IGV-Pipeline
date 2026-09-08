"""Versioned snapshot views: immutable payloads and one atomic commit pointer.

Readers pin ``snapshot_view(root)`` once. Root paths are compatibility aliases;
they must not be opened independently when a consistent multi-file read matters.
Legacy flat products are imported once, with a restartable alias migration.
"""
from __future__ import annotations

import csv
import errno
import fcntl
import json
import os
import re
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence

from .utils import atomic_write_json, reject_symlink_path_components, sha256_file, sha256_json, write_tsv

SCHEMA = "3.1-snapshot-store"
SNAPSHOT_FIELDS = ("manifest_order", "task_id", "chromosome", "relative_path", "sha256", "status")
FAILURE_FIELDS = ("manifest_order", "task_id", "chromosome", "failure_code", "message", "input_fingerprint")
PUBLIC_NAMES = ("snapshots", "snapshots.tsv", "failed_cases.tsv", "run_summary.json")
_HEX = re.compile(r"^[a-f0-9]{64}$")
_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,250}$")


def _fsync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _table(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream, delimiter="\t"))


def merge_failure_rows(existing, incoming):
    """Retain all failure reasons for a case without conflating rows and cases."""
    groups, additions = {}, {}
    for row in existing:
        groups.setdefault(row["task_id"], []).append(row)
    for row in incoming:
        additions.setdefault(row["task_id"], []).append(row)
    for tid, rows in additions.items():
        if tid in groups and groups[tid] != rows:
            raise ValueError("failed-case metadata collision: " + tid)
        groups[tid] = rows
    return sorted((row for rows in groups.values() for row in rows), key=lambda r: int(r["manifest_order"]))


def _store(root: Path) -> Path:
    return root / ".igv-pipeline" / "snapshot-store"


def _verify_generation(view: Path) -> dict[str, Any]:
    manifest = json.loads((view / "generation.json").read_text())
    identity = manifest.pop("generation_id")
    if manifest.get("schema_version") != SCHEMA or sha256_json(manifest) != identity or view.name != identity:
        raise ValueError("snapshot generation identity differs")
    for name, digest in manifest["metadata_sha256"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("snapshot generation metadata path is unsafe")
        path = view / relative
        if path.is_symlink() or sha256_file(path) != digest:
            raise ValueError(f"snapshot generation metadata checksum differs: {name}")
    return {**manifest, "generation_id": identity}


def snapshot_view(root: str | Path) -> Path:
    """Pin a validated generation, or read an unchanged legacy flat product."""
    root = Path(root).resolve(strict=True)
    store = _store(root)
    pointer = store / "CURRENT"
    if not pointer.is_symlink():
        if pointer.exists():
            raise ValueError("snapshot CURRENT must be a managed relative symlink")
        return root
    target = Path(os.readlink(pointer))
    if len(target.parts) != 2 or target.parts[0] != "generations" or not _HEX.fullmatch(target.parts[1]):
        raise ValueError("snapshot CURRENT target is unsafe")
    view = store / target
    if view.is_symlink() or view.resolve(strict=True).parent != (store / "generations").resolve(strict=True):
        raise ValueError("snapshot CURRENT escapes its generation directory")
    _verify_generation(view)
    return view


def _set_current(store: Path, generation: str) -> None:
    temporary = store / (".CURRENT-" + uuid.uuid4().hex)
    temporary.symlink_to("generations/" + generation, target_is_directory=True)
    os.replace(temporary, store / "CURRENT")
    _fsync(store)


def _link_tree(source: Path, destination: Path) -> None:
    """Copy directory entries, sharing immutable files without payload I/O."""
    shutil.copytree(source, destination, copy_function=os.link, ignore=shutil.ignore_patterns(".object-files.json"))


def export_snapshot_outputs(source: str | Path, destination: str | Path) -> dict[str, Any]:
    """Export one pinned view as ordinary files; keep execution state at source."""
    from .project_admission_v3 import _validate_snapshot_product
    from .publication import atomic_rename_noreplace
    root = reject_symlink_path_components(source, label="export source").resolve(strict=True)
    view = snapshot_view(root)
    rows, failures = _validate_snapshot_product(view)
    target = reject_symlink_path_components(destination, label="export destination").resolve(strict=False)
    if target == root or root in target.parents:
        raise ValueError("export destination must be outside its source run")
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.parent / (".igv-export-" + sha256_json(str(target))[:16] + ".lock")
    if lock_path.is_symlink():
        raise ValueError("export lock must not be a symlink")
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if target.exists() or target.is_symlink():
            raise FileExistsError(f"export destination already exists: {target}")
        stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".export-", dir=target.parent))
        try:
            shutil.copytree(view / "snapshots", stage / "snapshots")
            for name in PUBLIC_NAMES[1:]:
                shutil.copyfile(view / name, stage / name)
            receipt = {"schema_version": "3.1-flat-snapshot-export", "source_generation": view.name if view != root else None,
                       "snapshots_sha256": sha256_file(stage / "snapshots.tsv"),
                       "failed_cases_sha256": sha256_file(stage / "failed_cases.tsv"),
                       "case_count": len(rows), "failed_case_count": len({r["task_id"] for r in failures})}
            atomic_write_json(stage / "export_receipt.json", receipt)
            _validate_snapshot_product(stage)
            for path in stage.rglob("*"):
                if path.is_file():
                    _fsync(path)
            for path in sorted((p for p in stage.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
                _fsync(path)
            _fsync(stage)
            try:
                atomic_rename_noreplace(stage, target)
            except OSError as exc:
                if exc.errno not in {errno.EINVAL, errno.ENOSYS, errno.ENOTSUP, errno.EOPNOTSUPP}:
                    raise
                if target.exists() or target.is_symlink():
                    raise FileExistsError(str(target)) from exc
                os.rename(stage, target)  # Same parent, serialized by the export lock.
            _fsync(target.parent)
            return {**receipt, "status": "EXPORTED", "destination": str(target)}
        finally:
            if stage.exists():
                shutil.rmtree(stage)


def _install_alias(root: Path, name: str) -> None:
    target = root / name
    store = _store(root)
    relative = os.path.relpath(store / "CURRENT" / name, target.parent)
    if target.is_symlink():
        if os.readlink(target) != relative:
            raise ValueError(f"unexpected snapshot alias: {name}")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    backup = store / "legacy-backup" / name
    if target.exists():
        if backup.exists():
            raise ValueError(f"legacy migration has conflicting paths: {name}")
        backup.parent.mkdir(parents=True, exist_ok=True)
        os.replace(target, backup)
    temporary = target.parent / ("." + target.name + ".alias-" + uuid.uuid4().hex)
    temporary.symlink_to(relative, target_is_directory=name.endswith("snapshots") or name.endswith("cases"))
    os.replace(temporary, target)
    _fsync(target.parent)


class SnapshotTransaction:
    """One writer; imported objects and old generations survive interruptions."""

    def __init__(self, root: str | Path, *, expected_task_ids: Sequence[str] | None = None,
                 master_sha256: str | None = None):
        self.root = reject_symlink_path_components(root, label="snapshot store").resolve(strict=False)
        self.expected = list(expected_task_ids) if expected_task_ids is not None else None
        self.master_sha256 = master_sha256
        self.lock = None

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        control = self.root / ".igv-pipeline"
        if control.is_symlink():
            raise ValueError("snapshot control directory must not be a symlink")
        control.mkdir(exist_ok=True)
        lock_path = control / "snapshot-publication.lock"
        if lock_path.is_symlink():
            raise ValueError("snapshot publication lock must not be a symlink")
        self.lock = lock_path.open("a+")
        fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX)
        try:
            self.store = _store(self.root)
            if self.store.is_symlink():
                raise ValueError("snapshot store must not be a symlink")
            for part in ("objects", "case-objects", "generations"):
                reject_symlink_path_components(self.store / part, label="snapshot storage").mkdir(parents=True, exist_ok=True)
            contract = self.store / "contract.json"
            if contract.exists():
                frozen = json.loads(contract.read_text())
                if self.expected is not None and (frozen["task_ids"] != self.expected or frozen.get("master_sha256") != self.master_sha256):
                    raise ValueError("snapshot master task contract differs")
                self.expected = frozen["task_ids"]
                self.master_sha256 = frozen.get("master_sha256")
            elif self.expected is not None:
                if (self.store / "CURRENT").exists():
                    raise ValueError("a master contract must be frozen before the first snapshot generation")
                if len(self.expected) != len(set(self.expected)) or any(not _COMPONENT.fullmatch(t) for t in self.expected):
                    raise ValueError("snapshot master task IDs are invalid or duplicated")
                atomic_write_json(contract, {"schema_version": SCHEMA, "task_ids": self.expected,
                                             "master_sha256": self.master_sha256})
                _fsync(self.store)
            self._finish_bootstrap()
            if not (self.store / "CURRENT").is_symlink() and not (self.root / "snapshots.tsv").exists():
                self._import_legacy()
            self.view = snapshot_view(self.root)
            if self.view == self.root:
                from .project_admission_v3 import _validate_snapshot_product
                self.rows, self.failures = _validate_snapshot_product(self.root)
            else:
                self.rows = _table(self.view / "snapshots.tsv")
                self.failures = _table(self.view / "failed_cases.tsv")
            self.summary = json.loads((self.view / "run_summary.json").read_text())
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        if self.lock is not None:
            fcntl.flock(self.lock.fileno(), fcntl.LOCK_UN)
            self.lock.close()
            self.lock = None

    def _finish_bootstrap(self):
        journal = self.store / "bootstrap.json"
        if not journal.exists():
            return
        record = json.loads(journal.read_text())
        generation = record["generation_id"]
        if not _HEX.fullmatch(generation):
            raise ValueError("invalid bootstrap generation")
        _verify_generation(self.store / "generations" / generation)
        pointer = self.store / "CURRENT"
        if not pointer.is_symlink():
            _set_current(self.store, generation)
        elif os.readlink(pointer) != "generations/" + generation:
            raise ValueError("bootstrap pointer changed before migration completed")
        for name in record["aliases"]:
            if name not in (*PUBLIC_NAMES, ".igv-pipeline/cases", ".igv-pipeline/rerun/failed-only-state.json"):
                raise ValueError("unsafe bootstrap alias")
            _install_alias(self.root, name)
        journal.unlink()
        _fsync(self.store)
        shutil.rmtree(self.store / "legacy-backup", ignore_errors=True)

    def _import_legacy(self):
        rows, failures, summary, images, cases, extras = [], [], {}, {}, {}, {}
        if (self.root / "snapshots.tsv").exists():
            from .project_admission_v3 import _validate_snapshot_product
            rows, failures = _validate_snapshot_product(self.root)
            summary_path = self.root / "run_summary.json"
            if summary_path.is_file():
                summary = json.loads(summary_path.read_text())
            images = {r["task_id"]: self.root / r["relative_path"] for r in rows if r["status"] == "SNAPSHOT_READY"}
            from .product_paths_v3 import cases_root
            case_root = cases_root(self.root)
            if case_root.is_dir():
                cases = {p.name: p for p in case_root.iterdir() if p.is_dir()}
            state = self.root / ".igv-pipeline/rerun/failed-only-state.json"
            if state.is_file():
                extras[".igv-pipeline/rerun/failed-only-state.json"] = json.loads(state.read_text())
        elif any((self.root / name).exists() for name in PUBLIC_NAMES):
            raise ValueError("incomplete legacy product cannot be replaced by an empty generation")
        self.view = None
        self.rows, self.failures, self.summary = [], [], {}
        self.publish(rows, failures, images=images, summary=summary, case_updates=cases, extra_json=extras, bootstrap=True)

    def _object(self, source: Path, digest: str) -> Path:
        if not _HEX.fullmatch(digest) or source.is_symlink() or not source.is_file():
            raise ValueError("invalid incoming snapshot object")
        if sha256_file(source) != digest:
            raise ValueError("incoming snapshot checksum differs")
        target = self.store / "objects" / (digest + ".png")
        if target.exists():
            if sha256_file(target) != digest:
                raise ValueError("existing snapshot object checksum differs")
            return target
        temporary = target.with_name("." + target.name + "." + uuid.uuid4().hex)
        shutil.copyfile(source, temporary)
        if sha256_file(temporary) != digest:
            raise ValueError("copied snapshot checksum differs")
        temporary.chmod(0o444)
        _fsync(temporary)
        os.replace(temporary, target)
        _fsync(target.parent)
        return target

    def _case_object(self, source: Path) -> tuple[str, Path]:
        if source.is_symlink():
            raise ValueError("case evidence source must not be a symlink")
        inventory = {}
        for path in sorted(source.rglob("*")):
            if path.is_symlink():
                raise ValueError("case evidence contains a symlink")
            if path.is_file():
                if path.name == ".object-files.json":
                    continue
                inventory[str(path.relative_to(source))] = sha256_file(path)
        digest = sha256_json(inventory)
        target = self.store / "case-objects" / digest
        if not target.exists():
            temporary = target.with_name("." + digest + "." + uuid.uuid4().hex)
            shutil.copytree(source, temporary, ignore=shutil.ignore_patterns(".object-files.json"))
            for name, expected in inventory.items():
                path = temporary / name
                if sha256_file(path) != expected:
                    raise ValueError("copied case evidence checksum differs")
                path.chmod(0o444)
                _fsync(path)
            atomic_write_json(temporary / ".object-files.json", inventory)
            _fsync(temporary)
            os.replace(temporary, target)
            _fsync(target.parent)
        return digest, target

    def publish(self, rows, failures, *, images: Mapping[str, Path], summary: Mapping[str, Any],
                case_updates: Mapping[str, Path] | None = None, extra_json: Mapping[str, Any] | None = None,
                validate=None, bootstrap: bool = False) -> dict[str, Any]:
        ids = [r["task_id"] for r in rows]
        if len(ids) != len(set(ids)) or any(not _COMPONENT.fullmatch(t) for t in ids):
            raise ValueError("snapshot task IDs are invalid or duplicated")
        if self.expected is not None and not set(ids).issubset(self.expected):
            raise ValueError("snapshot contains a task outside the master contract")
        orders = [int(r["manifest_order"]) for r in rows]
        if len(orders) != len(set(orders)) or any(n < 1 for n in orders):
            raise ValueError("snapshot manifest order is invalid or duplicated")
        if self.expected is not None:
            expected_order = {tid: i + 1 for i, tid in enumerate(self.expected)}
            if any(int(r["manifest_order"]) != expected_order[r["task_id"]] for r in rows):
                raise ValueError("snapshot order differs from the frozen master task set")
        failed_ids = [r["task_id"] for r in failures]
        if set(failed_ids) != {r["task_id"] for r in rows if r["status"] == "CASE_FAILED"}:
            raise ValueError("snapshot failure coverage differs")
        for row in rows:
            if not _COMPONENT.fullmatch(row["chromosome"]):
                raise ValueError("snapshot chromosome is invalid")
            if row["status"] == "CASE_FAILED":
                if row["relative_path"] or row["sha256"]:
                    raise ValueError("failed case exposes a snapshot")
            elif row["status"] != "SNAPSHOT_READY" or not _HEX.fullmatch(row["sha256"]):
                raise ValueError("snapshot status or checksum is invalid")
        if not bootstrap and self.view == self.root:
            self._import_legacy()
        stage = self.store / (".generation-" + uuid.uuid4().hex)
        stage.mkdir()
        (stage / "snapshots").mkdir()
        for row in rows:
            if row["status"] != "SNAPSHOT_READY":
                continue
            relative = f"snapshots/{row['chromosome']}/{row['task_id']}.png"
            if not _COMPONENT.fullmatch(row["chromosome"]) or row["relative_path"] != relative:
                raise ValueError("snapshot path differs from task identity")
            if row["task_id"] in images:
                self._object(Path(images[row["task_id"]]), row["sha256"])
            obj = self.store / "objects" / (row["sha256"] + ".png")
            target = stage / relative
            target.parent.mkdir(exist_ok=True)
            os.link(obj, target)
        case_index = {}
        if self.view is not None:
            case_index = json.loads((self.view / "case-objects.json").read_text())
        for task_id, source in (case_updates or {}).items():
            if task_id not in ids:
                raise ValueError("case evidence is outside the snapshot task set")
            case_index[task_id] = self._case_object(Path(source))[0]
        for task_id, digest in case_index.items():
            if not _COMPONENT.fullmatch(task_id) or not _HEX.fullmatch(digest):
                raise ValueError("unsafe case object identity")
            destination = stage / ".igv-pipeline/cases" / task_id
            destination.parent.mkdir(parents=True, exist_ok=True)
            _link_tree(self.store / "case-objects" / digest, destination)
        atomic_write_json(stage / "case-objects.json", case_index)
        write_tsv(stage / "snapshots.tsv", SNAPSHOT_FIELDS, rows)
        write_tsv(stage / "failed_cases.tsv", FAILURE_FIELDS, failures)
        expected_count = len(self.expected) if self.expected is not None else len(rows)
        missing = expected_count - len(rows)
        derived = {**summary, "schema_version": "3.0", "pipeline_version": "3.0.0",
                   "publication_schema": SCHEMA, "authoritative": False, "projection_kind": "UX_ONLY",
                   "expected_case_count": expected_count, "observed_case_count": len(rows),
                   "missing_case_count": missing, "failed_case_count": len(set(failed_ids)),
                   "status": "INCOMPLETE" if missing else "CASE_FAILURES" if failures else "SNAPSHOTS_READY",
                   "exit_code": 1 if missing else 2 if failures else 0}
        derived["source_digests"] = {**dict(summary.get("source_digests") or {}),
                                     "snapshots": sha256_file(stage / "snapshots.tsv"),
                                     "failed_cases": sha256_file(stage / "failed_cases.tsv")}
        atomic_write_json(stage / "run_summary.json", derived)
        names = ["snapshots.tsv", "failed_cases.tsv", "run_summary.json", "case-objects.json"]
        extra = dict(extra_json or {})
        state_name = ".igv-pipeline/rerun/failed-only-state.json"
        if self.view is not None and state_name not in extra and (self.view / state_name).exists():
            extra[state_name] = json.loads((self.view / state_name).read_text())
        for name, document in extra.items():
            if name != state_name:
                raise ValueError("unsupported snapshot generation metadata")
            atomic_write_json(stage / name, document)
            names.append(name)
        if validate is not None:
            validate(stage)
        manifest = {"schema_version": SCHEMA, "parent": self.view.name if self.view is not None else None,
                    "metadata_sha256": {name: sha256_file(stage / name) for name in names}}
        generation = sha256_json(manifest)
        atomic_write_json(stage / "generation.json", {**manifest, "generation_id": generation})
        for directory in sorted((p for p in stage.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
            _fsync(directory)
        _fsync(stage)
        destination = self.store / "generations" / generation
        if destination.exists() or destination.is_symlink():
            if destination.is_symlink():
                raise ValueError("existing generation must not be a symlink")
            _verify_generation(destination)
            from .project_admission_v3 import _validate_snapshot_product
            _validate_snapshot_product(destination)
            shutil.rmtree(stage)
        else:
            os.replace(stage, destination)
        _fsync(destination.parent)
        aliases = list(PUBLIC_NAMES)
        if case_index:
            aliases.append(".igv-pipeline/cases")
        if state_name in extra:
            aliases.append(state_name)
        if bootstrap:
            atomic_write_json(self.store / "bootstrap.json", {"generation_id": generation, "aliases": aliases})
            _fsync(self.store)
            self._finish_bootstrap()
        else:
            _set_current(self.store, generation)
            for name in aliases:
                _install_alias(self.root, name)
        self.view, self.rows, self.failures, self.summary = destination, list(rows), list(failures), derived
        return {"generation_id": generation, "commit_mode": "IMMUTABLE_OBJECTS_ATOMIC_CURRENT",
                "snapshots_sha256": manifest["metadata_sha256"]["snapshots.tsv"], "summary": derived}
