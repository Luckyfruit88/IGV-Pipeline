from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssqtl_igv.execution_policy import (
    GIB,
    detect_resource_envelope,
    load_execution_policy,
    parse_duration_seconds,
    parse_memory_bytes,
    resolve_execution_policy,
    write_execution_policy,
)
from ssqtl_igv.utils import sha256_json


def _envelope(*, cpus: int = 8, memory: int | None = 64 * GIB) -> dict:
    return {
        "execution_mode": "test",
        "cpu_slots": cpus,
        "memory_bytes": memory,
        "cpu_observations": [{"source": "fixture", "slots": cpus}],
        "memory_observations": (
            [] if memory is None else [{"source": "fixture", "bytes": memory}]
        ),
        "scc_allocation": None,
    }


def test_default_policy_is_deterministic_and_uses_bounded_attempt_ladder() -> None:
    first = resolve_execution_policy(envelope=_envelope(), execution_mode="test")
    second = resolve_execution_policy(envelope=_envelope(), execution_mode="test")

    assert first == second
    assert first["execution_policy_sha256"] == sha256_json(
        {key: value for key, value in first.items() if key != "execution_policy_sha256"}
    )
    assert first["concurrency"] == {
        "requested": "auto",
        "hard_max": 8,
        "cpu_capacity": 8,
        "memory_capacity": 7,
        "effective_max_parallel": 7,
    }
    attempts = first["render"]["attempts"]
    assert [row["attempt"] for row in attempts] == [1, 2, 3]
    assert [row["memory_bytes"] for row in attempts] == [8 * GIB, 16 * GIB, 24 * GIB]
    assert [row["igv_heap_argument"] for row in attempts] == ["6g", "14g", "22g"]
    assert [row["timeout_seconds"] for row in attempts] == [1800, 3600, 5400]
    assert first["render"]["retry_exit_statuses"] == [75, 137, 143]
    assert first["normalization"]["max_retries"] == 0
    assert first["normalization"]["error_strategy"] == "terminate"


def test_unknown_memory_falls_back_to_one_and_rejects_unsafe_explicit_parallel() -> None:
    policy = resolve_execution_policy(
        envelope=_envelope(cpus=32, memory=None),
        execution_mode="test",
    )
    assert policy["resource_envelope"]["memory_detection_reliable"] is False
    assert policy["concurrency"]["memory_capacity"] == 1
    assert policy["concurrency"]["effective_max_parallel"] == 1

    with pytest.raises(ValueError, match="exceeds the detected safe envelope"):
        resolve_execution_policy(
            envelope=_envelope(cpus=32, memory=None),
            execution_mode="test",
            max_parallel=2,
        )


def test_expert_overrides_scale_resources_without_changing_retry_classes() -> None:
    policy = resolve_execution_policy(
        envelope=_envelope(cpus=16, memory=128 * GIB),
        execution_mode="test",
        max_parallel=3,
        igv_cpus=2,
        igv_memory="10GiB",
        igv_timeout="45m",
        normalization_cpus=4,
        normalization_memory="20GiB",
        normalization_timeout="2h",
    )
    assert policy["concurrency"]["effective_max_parallel"] == 3
    assert [row["cpus"] for row in policy["render"]["attempts"]] == [2, 2, 2]
    assert [row["memory_bytes"] for row in policy["render"]["attempts"]] == [
        10 * GIB,
        20 * GIB,
        30 * GIB,
    ]
    assert [row["igv_heap_argument"] for row in policy["render"]["attempts"]] == [
        "8g",
        "18g",
        "28g",
    ]
    assert [row["timeout_seconds"] for row in policy["render"]["attempts"]] == [
        2700,
        5400,
        8100,
    ]
    assert policy["normalization"] == {
        "cpus": 4,
        "memory_bytes": 20 * GIB,
        "timeout_seconds": 7200,
        "max_retries": 0,
        "error_strategy": "terminate",
    }


def test_scc_envelope_is_complete_scheduler_bounded_and_reserves_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "cpu.max").write_text("max 100000\n", encoding="utf-8")
    (cgroup / "cpuset.cpus.effective").write_text("0-31\n", encoding="utf-8")
    (cgroup / "memory.max").write_text("max\n", encoding="utf-8")
    monkeypatch.setattr(
        "ssqtl_igv.execution_policy.os.sched_getaffinity",
        lambda _pid: set(range(32)),
        raising=False,
    )
    monkeypatch.setattr("ssqtl_igv.execution_policy.os.cpu_count", lambda: 32)
    monkeypatch.setattr("ssqtl_igv.execution_policy._physical_memory_bytes", lambda: None)
    environment = {
        "IGV_SCC_SLOTS": "8",
        "IGV_SCC_MEMORY_PER_SLOT": "8GiB",
        "IGV_SCC_WALLTIME": "04:00:00",
        "NSLOTS": "8",
    }
    observed = detect_resource_envelope(
        execution_mode="scc",
        environ=environment,
        cgroup_root=cgroup,
    )
    assert observed["cpu_slots"] == 8
    assert observed["memory_bytes"] == 64 * GIB
    assert observed["scc_allocation"] == {
        "slots": 8,
        "memory_per_slot_bytes": 8 * GIB,
        "total_memory_bytes": 64 * GIB,
        "walltime_seconds": 4 * 3600,
    }
    policy = resolve_execution_policy(
        execution_mode="scc",
        environ=environment,
        cgroup_root=cgroup,
    )
    # 10% of 64 GiB is larger than the fixed 2 GiB reserve; seven 8 GiB
    # renders fit, but eight do not.
    assert policy["concurrency"]["effective_max_parallel"] == 7


@pytest.mark.parametrize(
    "environment",
    [
        {"IGV_SCC_SLOTS": "8"},
        {
            "IGV_SCC_SLOTS": "8",
            "IGV_SCC_MEMORY_PER_SLOT": "8GiB",
        },
        {
            "IGV_SCC_SLOTS": "8",
            "IGV_SCC_MEMORY_PER_SLOT": "8GiB",
            "IGV_SCC_WALLTIME": "04:00:00",
            "NSLOTS": "7",
        },
    ],
)
def test_scc_envelope_fails_closed_when_incomplete_or_inconsistent(
    environment: dict[str, str],
) -> None:
    with pytest.raises(ValueError, match="SCC|NSLOTS"):
        detect_resource_envelope(execution_mode="scc", environ=environment)


def test_policy_file_is_immutable_and_self_validating(tmp_path: Path) -> None:
    path = tmp_path / "execution_policy.json"
    expected = write_execution_policy(
        path,
        envelope=_envelope(),
        execution_mode="test",
    )
    assert load_execution_policy(path) == expected
    with pytest.raises(FileExistsError):
        write_execution_policy(
            path,
            envelope=_envelope(),
            execution_mode="test",
        )

    document = json.loads(path.read_text(encoding="utf-8"))
    document["render"]["attempts"][0]["memory_bytes"] += 1
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="SHA-256"):
        load_execution_policy(path)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("8GiB", 8 * GIB),
        ("8192MiB", 8 * GIB),
        ("8GB", 8_000_000_000),
        (str(8 * GIB), 8 * GIB),
    ],
)
def test_memory_parser_has_explicit_binary_and_decimal_units(
    value: str, expected: int
) -> None:
    assert parse_memory_bytes(value, label="fixture") == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [("30m", 1800), ("1.5h", 5400), ("04:00:00", 14400), (90, 90)],
)
def test_duration_parser_accepts_public_and_sge_forms(
    value: str | int, expected: int
) -> None:
    assert parse_duration_seconds(value, label="fixture") == expected
