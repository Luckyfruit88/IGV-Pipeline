from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssqtl_igv.contracts import validate_v3_task_document
from ssqtl_igv.rerun_v3 import freeze_case_failure_rerun, prepare_rerun_task_set
from ssqtl_igv.utils import atomic_write_json, read_jsonl, sha256_file
from ssqtl_igv.v3_manifest import GENERIC_MANIFEST_FIELDS, normalize_generic_manifest


def _source_run(tmp_path: Path) -> tuple[Path, dict]:
    input_root = tmp_path / "input"
    input_root.mkdir()
    (input_root / "sample.bam").write_bytes(b"bam")
    (input_root / "sample.bam.bai").write_bytes(b"bai")
    manifest = input_root / "cases.tsv"
    manifest.write_text(
        "\t".join(GENERIC_MANIFEST_FIELDS)
        + "\n"
        + "\t".join(
            ("3.0", "case_1", "chr1:1-10", "+", "sample.bam", "", "sample", "", "", "")
        )
        + "\n",
        encoding="utf-8",
    )
    reference_root = tmp_path / "reference"
    reference_root.mkdir()
    resources = {
        "genome.json": b'{"id":"fixture"}\n',
        "genome.fa": b">chr1\nAAAAAAAAAA\n",
        "genome.fa.fai": b"chr1\t10\t6\t10\t11\n",
        "cytoband.txt.gz": b"cytoband",
        "annotation.gff.gz": b"annotation",
    }
    for name, payload in resources.items():
        (reference_root / name).write_bytes(payload)
    reference = reference_root / "reference.yaml"
    reference.write_text(
        """schema_version: "3.0"
id: fixture
display_name: Fixture
version: v1
resources:
  definition: {path: genome.json, sha256: null}
  fasta: {path: genome.fa, sha256: null}
  fai: {path: genome.fa.fai, sha256: null}
  cytoband: {path: cytoband.txt.gz, sha256: null}
  annotation: {path: annotation.gff.gz, sha256: null}
""",
        encoding="utf-8",
    )
    source = tmp_path / "source-run"
    normalized = normalize_generic_manifest(
        manifest,
        input_root,
        reference,
        source / "contract",
        "run_001",
        "generation_001",
    )
    task = next(read_jsonl(normalized["tasks"]))
    atomic_write_json(
        source / "contract" / "run_identity.json",
        {
            "run_id": "run_001",
            "generation_id": "generation_001",
            "canonical_tasks_sha256": sha256_file(normalized["tasks"]),
        },
    )
    return source, task


def test_checksum_bound_rerun_receipt_creates_only_a_new_generation(tmp_path: Path) -> None:
    source, task = _source_run(tmp_path)
    failed = {
        key: task[key]
        for key in ("run_id", "generation_id", "task_id", "manifest_order", "input_fingerprint")
    }
    failed.update(
        eligible=False,
        failures=[{"code": "IGV_BATCH_FAILED", "message": "fixture"}],
    )
    pointer = freeze_case_failure_rerun(source, [task], [failed])
    assert pointer is not None
    receipt = source / pointer["relative_path"] / "rerun_receipt.json"

    result = prepare_rerun_task_set(
        source,
        receipt,
        tmp_path / "new-contract",
        run_id="run_001",
        generation_id="generation_002",
    )

    rerun_task = next(read_jsonl(result["tasks"]))
    validate_v3_task_document(rerun_task)
    assert rerun_task["generation_id"] == "generation_002"
    assert rerun_task["task_id"] == "case_1"
    assert rerun_task["input_fingerprint"] != task["input_fingerprint"]
    assert result["source_rerun_receipt_sha256"] == sha256_file(receipt)

    with pytest.raises(ValueError, match="must differ"):
        prepare_rerun_task_set(
            source,
            receipt,
            tmp_path / "same-generation",
            run_id="run_001",
            generation_id="generation_001",
        )


def test_rerun_import_rejects_manifest_tamper(tmp_path: Path) -> None:
    source, task = _source_run(tmp_path)
    failed = {
        key: task[key]
        for key in ("run_id", "generation_id", "task_id", "manifest_order", "input_fingerprint")
    }
    failed.update(eligible=False, failures=[{"code": "FAILED", "message": "fixture"}])
    pointer = freeze_case_failure_rerun(source, [task], [failed])
    assert pointer is not None
    generation = source / pointer["relative_path"]
    receipt = generation / "rerun_receipt.json"
    manifest = generation / "rerun_manifest.jsonl"
    request = json.loads(manifest.read_text(encoding="utf-8"))
    request["source_task_id"] = "case_other"
    manifest.write_text(json.dumps(request) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="checksummed artifact drift"):
        prepare_rerun_task_set(
            source,
            receipt,
            tmp_path / "new-contract",
            run_id="run_001",
            generation_id="generation_002",
        )
