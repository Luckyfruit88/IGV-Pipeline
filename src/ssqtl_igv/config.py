from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import (
    DEFAULT_CLIENT_HEIGHT,
    DEFAULT_CLIENT_WIDTH,
    FIGURE_CONTRACT_ID,
    GUI_SETTLE_CONTRACT_ID,
)
from .utils import command_prefix, optional_text


class ConfigError(ValueError):
    pass


def _load_document(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigError(
                "PyYAML is not installed; install requirements.lock or use JSON-compatible YAML"
            ) from exc
    else:
        value = yaml.safe_load(text)
    if not isinstance(value, dict):
        raise ConfigError("configuration root must be a mapping")
    return value


def _expand(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, str):
        expanded = os.path.expandvars(os.path.expanduser(value))
        unresolved = re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", expanded)
        if unresolved:
            raise ConfigError(
                "required environment variable(s) are not set: "
                + ", ".join(sorted(set(unresolved)))
            )
        return expanded
    return value


def _positive_int(value: Any, key: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key} must be a positive integer") from exc
    if result < 1:
        raise ConfigError(f"{key} must be a positive integer")
    return result


def _path_has_url(value: str) -> bool:
    return re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value) is not None


@dataclass(frozen=True)
class WorkflowConfig:
    path: Path
    data: dict[str, Any]

    @classmethod
    def load(cls, path: str | Path) -> "WorkflowConfig":
        source = Path(path).expanduser().resolve(strict=False)
        if not source.is_file():
            raise ConfigError(f"configuration file does not exist: {source}")
        config = cls(source, _expand(_load_document(source)))
        config.validate()
        return config

    def validate(self) -> None:
        required = (
            "paths.associations",
            "paths.rds_dir",
            "paths.bam_lookup",
            "paths.violin_dir",
            "paths.output_root",
            "paths.publish_root",
            "genome.id",
            "genome.display_name",
            "genome.definition",
            "genome.fasta",
            "genome.fai",
            "genome.cytoband",
            "genome.annotation",
            "genome.annotation_version",
            "binaries.rscript",
            "binaries.igv",
        )
        missing = [key for key in required if self.get(key) in (None, "")]
        if missing:
            raise ConfigError("missing required configuration keys: " + ", ".join(missing))

        for key in (
            "paths.associations_sha256",
            "genome.definition_sha256",
            "genome.fasta_sha256",
            "genome.fai_sha256",
            "genome.cytoband_sha256",
            "genome.annotation_sha256",
        ):
            value = self.get(key)
            if value not in (None, "") and not re.fullmatch(r"[0-9a-fA-F]{64}", str(value)):
                raise ConfigError(f"{key} must be a 64-character SHA-256 when set")

        expected_count = self.get("inputs.expected_case_count")
        if expected_count not in (None, ""):
            _positive_int(expected_count, "inputs.expected_case_count")

        batch_path_keys = {
            "paths.rds_dir",
            "paths.violin_dir",
            "genome.definition",
            "genome.fasta",
            "genome.fai",
            "genome.cytoband",
            "genome.annotation",
        }
        for key in (
            "paths.associations",
            "paths.rds_dir",
            "paths.bam_lookup",
            "paths.violin_dir",
            "paths.output_root",
            "paths.publish_root",
            "genome.definition",
            "genome.fasta",
            "genome.fai",
            "genome.cytoband",
            "genome.annotation",
        ):
            value = str(self.get(key))
            if _path_has_url(value):
                raise ConfigError(f"{key} must be a local path")
            if key in batch_path_keys and any(
                char.isspace() for char in str(self.path_value(key))
            ):
                raise ConfigError(
                    f"{key} cannot contain whitespace because native tool arguments are tokenized"
                )

        if self.output_root.resolve(strict=False) == self.publish_root.resolve(strict=False):
            raise ConfigError("paths.output_root and paths.publish_root must be different")

        configured_mode = self.get("execution.mode", "local")
        mode = optional_text(configured_mode).lower()
        if mode not in {"local", "grid_engine"}:
            raise ConfigError("execution.mode must be local or grid_engine")

        max_parallel = _positive_int(self.get("scheduler.max_parallel", 1), "scheduler.max_parallel")
        _positive_int(
            self.get("scheduler.max_tasks_per_array", max_parallel),
            "scheduler.max_tasks_per_array",
        )
        _positive_int(self.get("scheduler.cases_per_task", 1), "scheduler.cases_per_task")
        memory_gb = _positive_int(self.get("scheduler.memory_gb", 1), "scheduler.memory_gb")
        total_memory = _positive_int(
            self.get("scheduler.total_parallel_memory_gb", max_parallel * memory_gb),
            "scheduler.total_parallel_memory_gb",
        )
        if max_parallel * memory_gb > total_memory:
            raise ConfigError(
                "scheduler.total_parallel_memory_gb must cover max_parallel * memory_gb"
            )
        if mode == "grid_engine":
            for key in ("binaries.qsub", "binaries.qacct"):
                try:
                    command_prefix(self.get(key))
                except ValueError as exc:
                    raise ConfigError(f"grid_engine mode requires a valid {key}") from exc
            if not optional_text(self.get("scheduler.project")):
                raise ConfigError("grid_engine mode requires scheduler.project")

        width = _positive_int(
            self.get("desktop.screen_width", DEFAULT_CLIENT_WIDTH), "desktop.screen_width"
        )
        height = _positive_int(
            self.get("desktop.screen_height", DEFAULT_CLIENT_HEIGHT), "desktop.screen_height"
        )
        _positive_int(self.get("desktop.screen_depth", 24), "desktop.screen_depth")
        for key in ("desktop.toolbar_locus_roi", "desktop.locus_field_roi"):
            roi = self.get(key)
            if roi is None:
                continue
            if not isinstance(roi, dict):
                raise ConfigError(f"{key} must be a mapping")
            try:
                x, y, roi_width, roi_height = (
                    int(roi[name]) for name in ("x", "y", "width", "height")
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ConfigError(f"{key} must define integer x/y/width/height") from exc
            if (
                x < 0
                or y < 0
                or roi_width < 1
                or roi_height < 1
                or x + roi_width > width
                or y + roi_height > height
            ):
                raise ConfigError(f"{key} must fit inside the configured desktop")

        if self.get("workflow.figure_contract_id", FIGURE_CONTRACT_ID) != FIGURE_CONTRACT_ID:
            raise ConfigError(f"workflow.figure_contract_id must be {FIGURE_CONTRACT_ID}")
        if self.get("workflow.gui_settle_contract_id", GUI_SETTLE_CONTRACT_ID) != GUI_SETTLE_CONTRACT_ID:
            raise ConfigError(f"workflow.gui_settle_contract_id must be {GUI_SETTLE_CONTRACT_ID}")
        if self.get("publication.generate_svg", False) is not False:
            raise ConfigError("publication.generate_svg must be false")

        chromosomes = self.get("publication.chromosomes", [])
        if not isinstance(chromosomes, list) or not all(
            isinstance(item, str)
            and item
            and Path(item).name == item
            and item not in {".", ".."}
            for item in chromosomes
        ):
            raise ConfigError("publication.chromosomes must contain safe directory names")
        if len(chromosomes) != len(set(chromosomes)):
            raise ConfigError("publication.chromosomes must not contain duplicates")

        for key, default in (
            ("inputs.association_columns.ag_site", "AG_site"),
            ("inputs.association_columns.snp", "SNP"),
            ("inputs.association_columns.strand", "strand"),
            ("inputs.rds_filename_template", "AGratio_SNPgeno_{strand_token}_{chrom}_list.rds"),
            ("inputs.ratio_column", "ratio"),
        ):
            if not str(self.get(key, default)).strip():
                raise ConfigError(f"{key} must be non-empty")
        for key, default in (
            ("inputs.locus_sample_columns", ["sample_id"]),
            ("inputs.bam_lookup_id_columns", ["sample_id"]),
            ("inputs.bam_lookup_path_columns", ["directory", "bam", "bam_path", "path"]),
            ("inputs.bam_suffixes", [".bam"]),
        ):
            value = self.get(key, default)
            if not isinstance(value, list) or not value or not all(
                isinstance(item, str) and item for item in value
            ):
                raise ConfigError(f"{key} must be a non-empty list of strings")

        provider = optional_text(self.get("storage.provider", "filesystem")).lower()
        if provider not in {"filesystem", "pquota"}:
            raise ConfigError("storage.provider must be filesystem or pquota")
        if provider == "pquota":
            try:
                command_prefix(self.get("binaries.pquota"))
            except ValueError as exc:
                raise ConfigError(
                    "storage.provider=pquota requires a valid binaries.pquota command"
                ) from exc
            if not optional_text(self.get("storage.project_quota_path")):
                raise ConfigError(
                    "storage.provider=pquota requires storage.project_quota_path"
                )
        for key in (
            "storage.minimum_free_gb",
            "storage.minimum_free_inodes",
            "storage.gate_max_age_seconds",
        ):
            if float(self.get(key, 0)) < 0:
                raise ConfigError(f"{key} cannot be negative")

    def get(self, dotted: str, default: Any = None) -> Any:
        value: Any = self.data
        for key in dotted.split("."):
            if not isinstance(value, dict) or key not in value:
                return default
            value = value[key]
        return value

    def path_value(self, dotted: str) -> Path:
        value = self.get(dotted)
        if not isinstance(value, str) or not value:
            raise ConfigError(f"configuration value is not a path: {dotted}")
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = self.path.parent / path
        return path.resolve(strict=False)

    @property
    def output_root(self) -> Path:
        return self.path_value("paths.output_root")

    @property
    def publish_root(self) -> Path:
        return self.path_value("paths.publish_root")

    def validate_run_root(self, value: str | Path, *, must_exist: bool = False) -> Path:
        root = Path(value).expanduser().resolve(strict=False)
        output = self.output_root.resolve(strict=False)
        if root.parent != output:
            raise ConfigError(f"run_root must be a direct child of paths.output_root ({output}): {root}")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,127}", root.name):
            raise ConfigError(f"invalid run ID: {root.name!r}")
        if must_exist and not root.is_dir():
            raise ConfigError(f"run_root does not exist: {root}")
        if root.exists() and not root.is_dir():
            raise ConfigError(f"run_root is not a directory: {root}")
        return root

    @property
    def modules(self) -> list[str]:
        value = self.get("environment.modules", [])
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ConfigError("environment.modules must be a list of strings")
        return value
