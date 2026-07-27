from __future__ import annotations

from pathlib import Path

from ssqtl_igv.product_paths_v3 import (
    cases_root,
    contract_root,
    resolve_case_artifact,
)
from ssqtl_igv.utils import sha256_file


def test_compact_product_resolves_snapshot_and_hidden_case_evidence(
    tmp_path: Path,
) -> None:
    task_id = "AG_chr1_20_19__SNP_chr1_22_A_G"
    contract = tmp_path / ".igv-pipeline" / "contract"
    case_root = tmp_path / ".igv-pipeline" / "cases" / task_id
    snapshot = tmp_path / "snapshots" / "chr1" / f"{task_id}.png"
    contract.mkdir(parents=True)
    case_root.mkdir(parents=True)
    snapshot.parent.mkdir(parents=True)
    snapshot.write_bytes(b"combined-image")
    scientific_qc = case_root / "scientific_qc.json"
    scientific_qc.write_text("{}\n", encoding="utf-8")
    (tmp_path / "snapshots.tsv").write_text(
        "manifest_order\ttask_id\tchromosome\trelative_path\tsha256\tstatus\n"
        f"4\t{task_id}\tchr1\tsnapshots/chr1/{task_id}.png\t"
        f"{sha256_file(snapshot)}\tSNAPSHOT_READY\n",
        encoding="utf-8",
    )
    result = {"task_id": task_id}

    assert contract_root(tmp_path) == contract
    assert cases_root(tmp_path) == case_root.parent
    assert resolve_case_artifact(
        tmp_path,
        result,
        "review_image",
        {
            "relative_path": f"results/cases/{task_id}/review.png",
            "sha256": sha256_file(snapshot),
        },
    ) == snapshot
    assert resolve_case_artifact(
        tmp_path,
        result,
        "scientific_qc",
        {
            "relative_path": f"results/cases/{task_id}/scientific_qc.json",
            "sha256": sha256_file(scientific_qc),
        },
    ) == scientific_qc


def test_compact_product_rejects_snapshot_checksum_drift(tmp_path: Path) -> None:
    task_id = "AG_chr1_20_19__SNP_chr1_22_A_G"
    snapshot = tmp_path / "snapshots" / "chr1" / f"{task_id}.png"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_bytes(b"combined-image")
    (tmp_path / "snapshots.tsv").write_text(
        "manifest_order\ttask_id\tchromosome\trelative_path\tsha256\tstatus\n"
        f"1\t{task_id}\tchr1\tsnapshots/chr1/{task_id}.png\t"
        f"{'0' * 64}\tSNAPSHOT_READY\n",
        encoding="utf-8",
    )

    try:
        resolve_case_artifact(
            tmp_path,
            {"task_id": task_id},
            "review_image",
            {
                "relative_path": f"results/cases/{task_id}/review.png",
                "sha256": sha256_file(snapshot),
            },
        )
    except ValueError as exc:
        assert "snapshot differs" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("checksum drift must fail closed")
