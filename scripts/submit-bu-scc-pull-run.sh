#!/usr/bin/env bash
set -euo pipefail

readonly JOB_NAME="igv-snapshot-v3"
readonly SLOT_COUNT="8"
readonly MEMORY_PER_CORE="8G"
readonly WALL_CLOCK_LIMIT="04:00:00"

usage() {
    cat >&2 <<'EOF'
Usage:
  submit-bu-scc-pull-run.sh \
    --site-config FILE \
    --sif FILE \
    --project-dir DIR \
    --output-dir DIR \
    [--batch-request FILE] \
    [--resume] [--dry-run]

Submit exactly one BU SCC SGE job. Inside that allocation, the SIF runs the
pipeline with Nextflow's local executor and at most eight concurrent cases.
Without --batch-request, the normal /project/project.yaml pull-and-run mode is
unchanged. With --batch-request, the immutable campaign root is additionally
bound read-only and the validated request is executed with campaign run-batch.

The site JSON contains four fields:
  project  required SGE project token
  qname    optional SGE queue token or null
  pe       required SGE parallel-environment token
  engine   "apptainer" or "singularity"

Project, output, and SIF paths must be absolute paths visible from compute
nodes. The project directory must contain a regular project.yaml file, and the
output directory must already exist and be writable.
EOF
}

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 2
}

site_config=""
sif_arg=""
project_dir_arg=""
output_dir_arg=""
batch_request_arg=""
resume=false
dry_run=false

while [[ $# -gt 0 ]]; do
    case "$1" in
    --site-config)
        [[ $# -ge 2 ]] || fail "--site-config requires a value"
        site_config="$2"
        shift 2
        ;;
    --sif)
        [[ $# -ge 2 ]] || fail "--sif requires a value"
        sif_arg="$2"
        shift 2
        ;;
    --project-dir)
        [[ $# -ge 2 ]] || fail "--project-dir requires a value"
        project_dir_arg="$2"
        shift 2
        ;;
    --output-dir)
        [[ $# -ge 2 ]] || fail "--output-dir requires a value"
        output_dir_arg="$2"
        shift 2
        ;;
    --batch-request)
        [[ $# -ge 2 ]] || fail "--batch-request requires a value"
        batch_request_arg="$2"
        shift 2
        ;;
    --resume)
        resume=true
        shift
        ;;
    --dry-run)
        dry_run=true
        shift
        ;;
    --help|-h)
        usage
        exit 0
        ;;
    --*)
        fail "unknown option: $1"
        ;;
    *)
        fail "unexpected positional argument: $1"
        ;;
    esac
done

[[ -n "${site_config}" ]] || fail "--site-config is required"
[[ -n "${sif_arg}" ]] || fail "--sif is required"
[[ -n "${project_dir_arg}" ]] || fail "--project-dir is required"
[[ -n "${output_dir_arg}" ]] || fail "--output-dir is required"

command -v python3 >/dev/null 2>&1 || fail "python3 is required to validate the site config"
[[ -f "${site_config}" && ! -L "${site_config}" ]] || \
    fail "site config must be a regular, non-symlink file: ${site_config}"
[[ -r "${site_config}" ]] || fail "site config is not readable: ${site_config}"

site_values="$(python3 -c '
import json
import re
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
try:
    value = json.loads(config_path.read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError) as exc:
    raise SystemExit(f"invalid BU SCC site config: {exc}") from exc

if not isinstance(value, dict):
    raise SystemExit("invalid BU SCC site config: root must be an object")
allowed = {"project", "qname", "pe", "engine"}
unknown = sorted(set(value) - allowed)
missing = sorted({"project", "pe", "engine"} - set(value))
if unknown:
    raise SystemExit(f"invalid BU SCC site config: unknown fields: {unknown}")
if missing:
    raise SystemExit(f"invalid BU SCC site config: missing fields: {missing}")

token_pattern = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
for key in ("project", "pe"):
    item = value.get(key)
    if not isinstance(item, str) or token_pattern.fullmatch(item) is None:
        raise SystemExit(f"invalid BU SCC site config: {key} is not a safe SGE token")

qname = value.get("qname")
if qname is None:
    qname = ""
elif not isinstance(qname, str) or token_pattern.fullmatch(qname) is None:
    raise SystemExit("invalid BU SCC site config: qname is not a safe SGE token or null")

engine = value.get("engine")
if engine not in {"apptainer", "singularity"}:
    raise SystemExit("invalid BU SCC site config: engine must be apptainer or singularity")

print("|".join((value["project"], qname, value["pe"], engine)))
' "${site_config}")" || exit 2

IFS='|' read -r sge_project qname parallel_environment container_engine <<<"${site_values}"
[[ -n "${sge_project}" && -n "${parallel_environment}" && -n "${container_engine}" ]] || \
    fail "site config parser returned an incomplete value"

canonical_existing_path() {
    local raw_path="$1"
    local expected_kind="$2"
    local label="$3"
    python3 -c '
import os
import sys
from pathlib import Path

raw, expected_kind, label = sys.argv[1:]
if not os.path.isabs(raw):
    raise SystemExit(f"{label} must be absolute: {raw}")
if any(character.isspace() for character in raw):
    raise SystemExit(f"{label} must not contain whitespace: {raw}")
if any(character in raw for character in "\\*?[]:,"):
    raise SystemExit(f"{label} contains a forbidden bind or glob character: {raw}")
if ".." in Path(raw).parts:
    raise SystemExit(f"{label} must not contain a parent traversal: {raw}")
try:
    resolved = Path(raw).resolve(strict=True)
except (OSError, RuntimeError) as exc:
    raise SystemExit(f"{label} cannot be resolved: {exc}") from exc
if expected_kind == "directory" and not resolved.is_dir():
    raise SystemExit(f"{label} is not a directory: {resolved}")
if expected_kind == "file" and not resolved.is_file():
    raise SystemExit(f"{label} is not a regular file: {resolved}")
print(resolved)
' "${raw_path}" "${expected_kind}" "${label}"
}

project_dir="$(canonical_existing_path "${project_dir_arg}" directory "project directory")" || exit 2
output_dir="$(canonical_existing_path "${output_dir_arg}" directory "output directory")" || exit 2
sif_path="$(canonical_existing_path "${sif_arg}" file "SIF path")" || exit 2

[[ "${sif_path}" == *.sif ]] || fail "SIF path must end in .sif: ${sif_path}"
[[ -r "${sif_path}" ]] || fail "SIF is not readable: ${sif_path}"
[[ -r "${project_dir}/project.yaml" && -f "${project_dir}/project.yaml" ]] || \
    fail "project directory must contain a readable project.yaml"
[[ ! -L "${project_dir}/project.yaml" ]] || fail "project.yaml must not be a symlink"
[[ -w "${output_dir}" ]] || fail "output directory is not writable: ${output_dir}"

if [[ "${project_dir}" == "${output_dir}" || \
      "${output_dir}" == "${project_dir}"/* || \
      "${project_dir}" == "${output_dir}"/* ]]; then
    fail "project and output directories must not overlap"
fi
if [[ "${sif_path}" == "${output_dir}"/* ]]; then
    fail "SIF must not be stored inside the writable output directory"
fi

campaign_root=""
container_batch_request=""
if [[ -n "${batch_request_arg}" ]]; then
    batch_request_path="$(
        canonical_existing_path "${batch_request_arg}" file "batch-request"
    )" || exit 2
    [[ ! -L "${batch_request_arg}" ]] || fail "batch-request must not be a symlink"
    campaign_root="$(python3 -c '
import re
import sys
from pathlib import Path

request = Path(sys.argv[1])
if request.name != "batch-request.json" or request.parent.parent.name != "batches":
    raise SystemExit(
        "batch-request must be CAMPAIGN_ROOT/batches/BATCH_ID/batch-request.json"
    )
if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", request.parent.name) is None:
    raise SystemExit("batch-request batch directory is not a safe batch ID")
root = request.parents[2]
if not root.is_dir():
    raise SystemExit("batch-request campaign root is not a directory")
print(root)
' "${batch_request_path}")" || exit 2
    if [[ "${campaign_root}" == "${output_dir}" || \
          "${output_dir}" == "${campaign_root}"/* || \
          "${campaign_root}" == "${output_dir}"/* ]]; then
        fail "campaign and output directories must not overlap"
    fi
    batch_relative="${batch_request_path#"${campaign_root}/"}"
    [[ "${batch_relative}" != "${batch_request_path}" ]] || \
        fail "batch-request is outside its campaign root"
    container_batch_request="/campaign/${batch_relative}"
fi

engine_bin="${container_engine}"
qsub_bin="qsub"
if [[ "${dry_run}" == false ]]; then
    engine_bin="$(command -v "${container_engine}")" || \
        fail "container engine is unavailable: ${container_engine}"
    qsub_bin="$(command -v qsub)" || fail "qsub is unavailable"
    [[ "${engine_bin}" == /* && "${qsub_bin}" == /* ]] || \
        fail "qsub and the container engine must resolve to absolute executables"
fi

container_command=(
    "${engine_bin}"
    run
    --cleanenv
    --containall
    --no-home
    --net
    --network none
    --bind "${project_dir}:/project:ro"
    --bind "${output_dir}:/output:rw"
)
if [[ -n "${batch_request_arg}" ]]; then
    container_command+=(
        --bind "${campaign_root}:/campaign:ro"
        "${sif_path}"
        campaign
        run-batch
        --batch-request "${container_batch_request}"
        --output /output
        --max-parallel "${SLOT_COUNT}"
    )
else
    container_command+=(
        "${sif_path}"
        run
        --max-parallel "${SLOT_COUNT}"
    )
fi
if [[ "${resume}" == true ]]; then
    container_command+=(--resume)
fi

qsub_command=(
    "${qsub_bin}"
    -terse
    -N "${JOB_NAME}"
    -P "${sge_project}"
)
if [[ -n "${qname}" ]]; then
    qsub_command+=(-q "${qname}")
fi
qsub_command+=(
    -pe "${parallel_environment}" "${SLOT_COUNT}"
    -l "mem_per_core=${MEMORY_PER_CORE}"
    -l "h_rt=${WALL_CLOCK_LIMIT}"
    -j y
    -b y
    "${container_command[@]}"
)

printf 'BU SCC pull-run command (one outer SGE job):\n'
printf ' %q' "${qsub_command[@]}"
printf '\n'

if [[ "${dry_run}" == true ]]; then
    printf 'Dry run only; no job was submitted.\n'
    exit 0
fi

job_id="$("${qsub_command[@]}")" || {
    status=$?
    printf 'ERROR: qsub failed with exit status %s\n' "${status}" >&2
    exit "${status}"
}
job_id="${job_id//$'\r'/}"
job_id="${job_id//$'\n'/}"
[[ "${job_id}" =~ ^[1-9][0-9]*([.][A-Za-z0-9_.-]+)?$ ]] || {
    printf 'ERROR: qsub returned an unexpected terse job id: %s\n' "${job_id}" >&2
    exit 1
}
printf 'Submitted BU SCC pull-run job: %s\n' "${job_id}"
