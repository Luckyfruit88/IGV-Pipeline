#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' \
        "usage: $0 --production-runtime ghcr.io/...@sha256:<64hex> --tag local-runtime-debug:version --build-record /absolute/path.json" \
        "Builds a separate diagnostic image only after its offline noVNC material lock is complete." >&2
}

production_runtime=""
tag=""
build_record=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --production-runtime)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            production_runtime="$2"
            shift 2
            ;;
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

[[ "${production_runtime}" =~ ^ghcr\.io/luckyfruit88/igv-pipeline@sha256:[a-f0-9]{64}$ ]] || {
    echo "production runtime must be the digest-pinned production repository" >&2
    exit 2
}
[[ "${tag}" =~ ^[a-z0-9][a-z0-9._/-]*:[A-Za-z0-9][A-Za-z0-9._-]*$ && "${tag}" == *runtime-debug* ]] || {
    echo "local tag must identify a separate runtime-debug image" >&2
    exit 2
}
[[ "${build_record}" == /* && "${build_record}" == *.json ]] || {
    usage
    exit 2
}
[[ ! -e "${build_record}" && ! -L "${build_record}" ]] || {
    echo "refusing to overwrite debug build record: ${build_record}" >&2
    exit 2
}

for command_name in docker git python3; do
    command -v "${command_name}" >/dev/null 2>&1 || {
        echo "required debug build command is unavailable: ${command_name}" >&2
        exit 2
    }
done

project_root="$(git rev-parse --show-toplevel)"
lock_path="${project_root}/containers/runtime-debug-materials.lock.json"
[[ -f "${lock_path}" && ! -L "${lock_path}" ]] || {
    echo "runtime-debug material lock is unavailable or is a symlink" >&2
    exit 2
}
IFS=$'\t' read -r lock_state bundle_relative bundle_sha256 < <(python3 - "${lock_path}" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
bundle = payload.get("debug_tools_bundle", {})
print(
    payload.get("lock_state", ""),
    bundle.get("path", ""),
    bundle.get("sha256") or "",
    sep="\t",
)
PY
)
[[ "${lock_state}" == "LOCKED" ]] || {
    echo "runtime-debug materials are not locked; refusing build (${lock_state:-UNKNOWN})" >&2
    exit 2
}
[[ "${bundle_relative}" == "containers/debug-runtime-tools.tar.gz" ]] || {
    echo "runtime-debug bundle path is not the approved build-context path" >&2
    exit 2
}
[[ "${bundle_sha256}" =~ ^[a-f0-9]{64}$ ]] || {
    echo "runtime-debug bundle has no valid SHA-256 lock" >&2
    exit 2
}
bundle_path="${project_root}/${bundle_relative}"
[[ -f "${bundle_path}" && ! -L "${bundle_path}" ]] || {
    echo "locked runtime-debug bundle is unavailable or is a symlink" >&2
    exit 2
}
actual_bundle_sha256="$(python3 - "${bundle_path}" <<'PY'
import hashlib
import sys
from pathlib import Path

digest = hashlib.sha256()
with Path(sys.argv[1]).open("rb") as handle:
    for block in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(block)
print(digest.hexdigest())
PY
)"
[[ "${actual_bundle_sha256}" == "${bundle_sha256}" ]] || {
    echo "runtime-debug bundle checksum does not match its lock" >&2
    exit 2
}
python3 - "${bundle_path}" <<'PY'
import sys
import tarfile
from pathlib import PurePosixPath

required = {"bin/novnc_proxy", "bin/websockify", "bin/x11vnc"}
with tarfile.open(sys.argv[1], mode="r:gz") as archive:
    members = archive.getmembers()
    names = set()
    for member in members:
        path = PurePosixPath(member.name)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise SystemExit(f"unsafe runtime-debug archive path: {member.name}")
        if member.issym() or member.islnk() or member.isdev() or member.isfifo():
            raise SystemExit(f"runtime-debug archive contains a prohibited entry: {member.name}")
        names.add(path.as_posix().removeprefix("./"))
missing = sorted(required - names)
if missing:
    raise SystemExit(f"runtime-debug archive lacks required commands: {missing}")
PY

git -C "${project_root}" diff --quiet
git -C "${project_root}" diff --cached --quiet
while IFS= read -r untracked; do
    [[ -z "${untracked}" || "${untracked}" == "${bundle_relative}" ]] || {
        echo "runtime-debug source checkout has an unexpected untracked file: ${untracked}" >&2
        exit 2
    }
done < <(git -C "${project_root}" ls-files --others --exclude-standard)

source_commit="$(git -C "${project_root}" rev-parse HEAD)"
source_tree="$(git -C "${project_root}" rev-parse 'HEAD^{tree}')"
lock_sha256="$(python3 - "${lock_path}" <<'PY'
import hashlib
import sys
from pathlib import Path

print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"
metadata="$(mktemp "${TMPDIR:-/tmp}/igv-runtime-debug-build.XXXXXX.json")"
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
    --file "${project_root}/containers/runtime-debug.Dockerfile" \
    --build-arg "PRODUCTION_RUNTIME_REF=${production_runtime}" \
    --build-arg "DEBUG_TOOLS_BUNDLE_SHA256=${bundle_sha256}" \
    --label "org.opencontainers.image.revision=${source_commit}" \
    --label "org.opencontainers.image.source.tree=${source_tree}" \
    --label "org.opencontainers.image.debug.materials.sha256=${lock_sha256}" \
    --metadata-file "${metadata}" \
    --tag "${tag}" \
    "${project_root}"

docker run --rm \
    --platform linux/amd64 \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --network none \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m \
    --entrypoint runtime-debug-entrypoint \
    "${tag}" --self-test

local_image_id="$(docker image inspect --format '{{.Id}}' "${tag}")"
python3 - \
    "${partial_record}" \
    "${tag}" \
    "${local_image_id}" \
    "${production_runtime}" \
    "${source_commit}" \
    "${source_tree}" \
    "${lock_sha256}" \
    "${bundle_sha256}" \
    "${metadata}" <<'PY'
import json
import sys
from pathlib import Path

(
    destination,
    tag,
    image_id,
    production_runtime,
    commit,
    tree,
    lock_sha256,
    bundle_sha256,
    metadata,
) = sys.argv[1:]
payload = {
    "schema_version": "igv-runtime-debug-local-build-v1",
    "platform": "linux/amd64",
    "image_role": "runtime-debug",
    "artifact_class": "DEBUG_ONLY",
    "review_eligible": False,
    "publication_eligible": False,
    "local_tag": tag,
    "local_image_id": image_id,
    "production_runtime": production_runtime,
    "source": {"commit": commit, "tree": tree, "clean": True},
    "debug_materials": {
        "path": "containers/runtime-debug-materials.lock.json",
        "sha256": lock_sha256,
        "bundle_sha256": bundle_sha256,
    },
    "buildx_metadata": json.loads(Path(metadata).read_text(encoding="utf-8")),
}
Path(destination).write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
chmod 0444 "${partial_record}"
mv -- "${partial_record}" "${build_record}"

printf 'debug_image=%s\n' "${tag}"
printf 'debug_image_id=%s\n' "${local_image_id}"
printf 'artifact_class=%s\n' 'DEBUG_ONLY'
