#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' \
        "usage: $0 --tag local-name:version --build-record /absolute/path/build-record.json" \
        "Build the clean checkout as the single linux/amd64 pull-and-run image." >&2
}

tag=""
build_record=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            tag="$2"
            shift 2
            ;;
        --build-record)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            build_record="$2"
            shift 2
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

[[ "${tag}" =~ ^[a-z0-9][a-z0-9._/-]*:[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || {
    usage
    exit 2
}
[[ "${build_record}" == /* && "${build_record}" == *.json ]] || {
    usage
    exit 2
}
[[ ! -e "${build_record}" && ! -L "${build_record}" ]] || {
    printf 'refusing to overwrite build record: %s\n' "${build_record}" >&2
    exit 2
}

for command_name in docker git python3; do
    command -v "${command_name}" >/dev/null 2>&1 || {
        printf 'required build command is unavailable: %s\n' "${command_name}" >&2
        exit 2
    }
done

project_root="$(git rev-parse --show-toplevel)"
[[ -f "${project_root}/containers/runtime.Dockerfile" ]] || {
    printf '%s\n' 'run from the IGV-Pipeline Git checkout' >&2
    exit 2
}
git -C "${project_root}" diff --quiet
git -C "${project_root}" diff --cached --quiet
[[ -z "$(git -C "${project_root}" ls-files --others --exclude-standard)" ]] || {
    printf '%s\n' 'runtime OCI source checkout must be clean' >&2
    exit 2
}

source_commit="$(git -C "${project_root}" rev-parse HEAD)"
source_tree="$(git -C "${project_root}" rev-parse 'HEAD^{tree}')"
metadata="$(mktemp "${TMPDIR:-/tmp}/igv-pipeline-build.XXXXXX.json")"
partial_record="${build_record}.partial.$$"
cleanup() {
    status=$?
    rm -f -- "${metadata}" "${partial_record}"
    exit "${status}"
}
trap cleanup EXIT

docker buildx build \
    --platform linux/amd64 \
    --load \
    --provenance=mode=max \
    --file "${project_root}/containers/runtime.Dockerfile" \
    --build-arg "SOURCE_COMMIT=${source_commit}" \
    --build-arg "SOURCE_TREE=${source_tree}" \
    --label "org.opencontainers.image.revision=${source_commit}" \
    --label "org.opencontainers.image.source.tree=${source_tree}" \
    --metadata-file "${metadata}" \
    --tag "${tag}" \
    "${project_root}"

docker run --rm \
    --platform linux/amd64 \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --network none \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=256m \
    --tmpfs /run/home:rw,noexec,nosuid,nodev,size=256m,uid=65532,gid=65532,mode=0700 \
    --tmpfs /output:rw,nosuid,nodev,size=512m,uid=65532,gid=65532,mode=0750 \
    --entrypoint runtime-self-test \
    "${tag}"

local_image_id="$(docker image inspect --format '{{.Id}}' "${tag}")"
python3 - \
    "${partial_record}" \
    "${tag}" \
    "${local_image_id}" \
    "${source_commit}" \
    "${source_tree}" \
    "${metadata}" <<'PY'
import json
import sys
from pathlib import Path

destination, tag, image_id, commit, tree, metadata = sys.argv[1:]
payload = {
    "schema_version": "igv-runtime-local-build-v2",
    "platform": "linux/amd64",
    "local_tag": tag,
    "local_image_id": image_id,
    "source": {"commit": commit, "tree": tree, "clean": True},
    "runtime_manifest_path": "/opt/igv-pipeline/runtime-manifest.json",
    "self_test": "PASS",
    "buildx_metadata": json.loads(Path(metadata).read_text(encoding="utf-8")),
}
Path(destination).write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
chmod 0444 "${partial_record}"
mv -- "${partial_record}" "${build_record}"

printf 'local_image=%s\n' "${tag}"
printf 'local_image_id=%s\n' "${local_image_id}"
printf 'build_record=%s\n' "${build_record}"
printf '%s\n' 'runtime_self_test=PASS'
