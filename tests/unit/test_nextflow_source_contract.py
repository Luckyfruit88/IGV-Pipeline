from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _text(relative: str) -> str:
    return (PROJECT_ROOT / relative).read_text(encoding="utf-8")


def test_r_prepare_pair_is_two_explicit_fixed_stage_inputs() -> None:
    module = _text("modules/local/validate_and_normalize.nf")
    workflow = _text("workflows/plan_run.nf")

    assert (
        "path r_wrapper, stageAs: 'inputs/r_prepare/prepare_cases_wrapper.R'" in module
    )
    assert (
        "path r_implementation, stageAs: "
        "'inputs/r_prepare/prepare_cases_implementation.R'" in module
    )
    assert "--r-wrapper '${r_wrapper}'" in module
    assert "--r-implementation '${r_implementation}'" in module
    assert "file(params.r_prepare_wrapper, checkIfExists: true)" in workflow
    assert "file(params.r_prepare_implementation, checkIfExists: true)" in workflow


def test_r_wrapper_has_no_repository_lookup_or_dynamic_implementation_argument() -> None:
    wrapper = _text("bin/prepare_cases.R")

    assert 'file.path(dirname(wrapper), "prepare_cases_implementation.R")' in wrapper
    assert "project_root" not in wrapper
    assert "--implementation" not in wrapper
    assert "Sys.getenv" not in wrapper


def test_nextflow_schema_exposes_both_r_materials_and_removes_ambiguous_name() -> None:
    schema = json.loads(_text("nextflow_schema.json"))
    properties = schema["properties"]

    assert "r_prepare_wrapper" in properties
    assert "r_prepare_implementation" in properties
    assert "r_script" not in properties
