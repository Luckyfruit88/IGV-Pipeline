#!/usr/bin/env bash
set -euo pipefail

# Stage a tested runtime from one prebuilt distribution artifact. The artifact
# and optional wheelhouse are deployment inputs; this script never builds from
# source and never changes an existing runtime.
RUNTIME_ROOT=${IGV_RUNTIME_ROOT:-${RUNTIME_ROOT:-"$HOME/.cache/igv-snapshot-workflow/runtime"}}
PYTHON_BIN=${PYTHON_BIN:-python3}

case "$RUNTIME_ROOT" in
  /*) ;;
  *)
    printf '%s\n' "IGV_RUNTIME_ROOT must be an absolute path: $RUNTIME_ROOT" >&2
    exit 1
    ;;
esac
if [[ -z "${IGV_PACKAGE_ARTIFACT:-}" ]]; then
  printf '%s\n' "IGV_PACKAGE_ARTIFACT must name a prebuilt igv-snapshot-workflow wheel" >&2
  exit 1
fi
if [[ "$IGV_PACKAGE_ARTIFACT" != /* ]]; then
  printf '%s\n' "IGV_PACKAGE_ARTIFACT must be an absolute path: $IGV_PACKAGE_ARTIFACT" >&2
  exit 1
fi
if [[ -L "$IGV_PACKAGE_ARTIFACT" || ! -f "$IGV_PACKAGE_ARTIFACT" ]]; then
  printf '%s\n' "package artifact is missing, not a regular file, or a symlink: $IGV_PACKAGE_ARTIFACT" >&2
  exit 1
fi
case "${IGV_PACKAGE_ARTIFACT##*/}" in
  igv_snapshot_workflow-*.whl) ;;
  *)
    printf '%s\n' "IGV_PACKAGE_ARTIFACT must be an igv_snapshot_workflow wheel: $IGV_PACKAGE_ARTIFACT" >&2
    exit 1
    ;;
esac

if [[ -e "$RUNTIME_ROOT" || -L "$RUNTIME_ROOT" ]]; then
  printf '%s\n' "refusing to replace existing runtime: $RUNTIME_ROOT" >&2
  exit 1
fi

PIP_LOCATION_ARGS=()
if [[ -n "${IGV_WHEELHOUSE:-}" ]]; then
  if [[ "$IGV_WHEELHOUSE" != /* ]]; then
    printf '%s\n' "IGV_WHEELHOUSE must be an absolute path: $IGV_WHEELHOUSE" >&2
    exit 1
  fi
  if [[ -L "$IGV_WHEELHOUSE" || ! -d "$IGV_WHEELHOUSE" ]]; then
    printf '%s\n' "wheelhouse is missing, not a directory, or a symlink: $IGV_WHEELHOUSE" >&2
    exit 1
  fi
  PIP_LOCATION_ARGS=(--no-index --find-links "$IGV_WHEELHOUSE")
fi

runtime_parent=$(dirname -- "$RUNTIME_ROOT")
complete=false
created_runtime=false
cleanup() {
  if [[ "$complete" != true && "$created_runtime" == true ]]; then
    rm -rf -- "$RUNTIME_ROOT"
  fi
}
trap cleanup EXIT HUP INT TERM
mkdir -p "$runtime_parent"
mkdir "$RUNTIME_ROOT"
created_runtime=true

# Virtual-environment entry points contain absolute interpreter paths, so the
# venv must be created at its final location.  The directory remains
# uncommitted until every check passes and the readiness marker is renamed.
"$PYTHON_BIN" -m venv "$RUNTIME_ROOT/venv"
"$RUNTIME_ROOT/venv/bin/python" -m pip install \
  --disable-pip-version-check \
  --only-binary=:all: \
  "${PIP_LOCATION_ARGS[@]}" \
  "$IGV_PACKAGE_ARTIFACT"
"$RUNTIME_ROOT/venv/bin/python" -m pip check
"$RUNTIME_ROOT/venv/bin/python" - <<'PY'
import importlib.metadata
import importlib.resources

import ssqtl_igv

installed = importlib.metadata.version("igv-snapshot-workflow")
if installed != ssqtl_igv.__version__:
    raise SystemExit(
        "installed distribution/package version mismatch: "
        f"{installed} != {ssqtl_igv.__version__}"
    )

resources = importlib.resources.files("ssqtl_igv.resources")
for name in ("prepare_cases.R", "workflow.example.yaml"):
    resource = resources.joinpath(name)
    if not resource.is_file():
        raise SystemExit(f"missing installed package resource: {name}")
    if not resource.read_bytes():
        raise SystemExit(f"installed package resource is empty: {name}")

print(f"igv-snapshot-workflow {installed}; installed resources PASS")
PY
"$RUNTIME_ROOT/venv/bin/igv-snapshot-workflow" --help >/dev/null
"$RUNTIME_ROOT/venv/bin/ssqtl-igv" --help >/dev/null

printf '%s\n' "igv-snapshot-workflow runtime 1.0.1" > "$RUNTIME_ROOT/.READY.tmp"
mv -- "$RUNTIME_ROOT/.READY.tmp" "$RUNTIME_ROOT/READY"
complete=true
trap - EXIT HUP INT TERM
printf 'staged igv-snapshot-workflow runtime: %s\n' "$RUNTIME_ROOT"
