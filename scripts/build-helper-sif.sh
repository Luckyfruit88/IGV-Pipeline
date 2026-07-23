#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 --output /absolute/path/igv-helper-<commit>.sif" >&2
}

output=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            output="$2"
            shift 2
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

[[ -n "${output}" && "${output}" == /* && "${output}" == *.sif ]] || {
    usage
    exit 2
}
[[ "$(uname -s)" == "Linux" && "$(uname -m)" == "x86_64" ]] || {
    echo "helper SIF builds require Linux x86_64" >&2
    exit 2
}
for command_name in curl git python3 rpm sha256sum singularity tar; do
    command -v "${command_name}" >/dev/null 2>&1 || {
        echo "required build command is unavailable: ${command_name}" >&2
        exit 2
    }
done
[[ "$(rpm -q singularity-ce)" == "singularity-ce-4.5.0-1.el8.x86_64" ]] || {
    echo "reproducible helper build requires singularity-ce-4.5.0-1.el8.x86_64" >&2
    exit 2
}

project_root="$(git rev-parse --show-toplevel)"
[[ -f "${project_root}/containers/helpers.def" ]] || {
    echo "run from the igv-snapshot-workflow Git checkout" >&2
    exit 2
}
git -C "${project_root}" diff --quiet
git -C "${project_root}" diff --cached --quiet
[[ -z "$(git -C "${project_root}" ls-files --others --exclude-standard)" ]] || {
    echo "helper SIF source checkout must be clean" >&2
    exit 2
}

output_parent="$(dirname "${output}")"
[[ -d "${output_parent}" ]] || {
    echo "output parent does not exist: ${output_parent}" >&2
    exit 2
}
for candidate in "${output}" "${output}.sha256" "${output}.build.json" "${output}.inventory.txt" "${output}.claim"; do
    [[ ! -e "${candidate}" && ! -L "${candidate}" ]] || {
        echo "refusing to overwrite existing build artifact: ${candidate}" >&2
        exit 2
    }
done

source_commit="$(git -C "${project_root}" rev-parse HEAD)"
source_tree="$(git -C "${project_root}" rev-parse 'HEAD^{tree}')"
short_commit="${source_commit:0:12}"
[[ "$(basename "${output}")" == *"${short_commit}"*.sif ]] || {
    echo "output filename must contain source commit prefix ${short_commit}" >&2
    exit 2
}

set -o noclobber
: >"${output}.claim"
set +o noclobber

temporary="$(mktemp -d "${TMPDIR:-/tmp}/igv-helper-build.XXXXXX")"
partial_sif="${output}.partial.$$"
partial_inventory="${output}.inventory.txt.partial.$$"
partial_sha256="${output}.sha256.partial.$$"
partial_build_json="${output}.build.json.partial.$$"
cleanup() {
    status=$?
    rm -rf -- "${temporary}"
    rm -f -- \
        "${partial_sif}" \
        "${partial_inventory}" \
        "${partial_sha256}" \
        "${partial_build_json}" \
        "${output}.claim"
    if [[ ! -e "${output}" && ! -L "${output}" ]]; then
        rm -f -- "${output}.sha256" "${output}.build.json" "${output}.inventory.txt"
    fi
    exit "${status}"
}
trap cleanup EXIT

context="${temporary}/context"
mkdir -p "${context}/source" "${temporary}/singularity-cache" "${temporary}/singularity-tmp"
install -m 0644 "${project_root}/containers/helpers.def" "${context}/helpers.def"
install -m 0644 "${project_root}/containers/helper-linux-64.lock" "${context}/helper-linux-64.lock"
install -m 0644 "${project_root}/containers/samtools-linux-64.lock" "${context}/samtools-linux-64.lock"
install -m 0644 "${project_root}/containers/materials.lock.json" "${context}/materials.lock.json"
git -C "${project_root}" archive --format=tar HEAD | tar -xf - -C "${context}/source"

mapfile -t micromamba_material < <(
    python3 - "${project_root}/containers/helper-lock-spec.json" <<'PY'
import json
import sys
from pathlib import Path

spec = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(spec["micromamba"]["linux_64_url"])
print(spec["micromamba"]["linux_64_sha256"])
PY
)
micromamba_url="${micromamba_material[0]}"
micromamba_sha256="${micromamba_material[1]}"
curl --fail --location --silent --show-error \
    --output "${context}/micromamba" \
    "${micromamba_url}"
printf '%s  %s\n' "${micromamba_sha256}" "${context}/micromamba" | sha256sum --check --status
chmod 0755 "${context}/micromamba"

built_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
definition_sha256="$(sha256sum "${context}/helpers.def" | cut -d' ' -f1)"
lock_spec_sha256="$(sha256sum "${project_root}/containers/helper-lock-spec.json" | cut -d' ' -f1)"
materials_sha256="$(sha256sum "${context}/materials.lock.json" | cut -d' ' -f1)"
python3 - \
    "${context}/build-provenance.json" \
    "${source_commit}" \
    "${source_tree}" \
    "${built_at}" \
    "${definition_sha256}" \
    "${lock_spec_sha256}" \
    "${materials_sha256}" \
    "$(rpm -q singularity-ce)" <<'PY'
import json
import sys
from pathlib import Path

keys = (
    "source_commit",
    "source_tree",
    "built_at",
    "definition_sha256",
    "lock_spec_sha256",
    "materials_sha256",
    "singularity_rpm",
)
payload = {"schema_version": "igv-helper-build-v1", **dict(zip(keys, sys.argv[2:]))}
Path(sys.argv[1]).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

export SINGULARITY_CACHEDIR="${temporary}/singularity-cache"
export SINGULARITY_TMPDIR="${temporary}/singularity-tmp"
(
    cd "${context}"
    singularity build --fakeroot --disable-cache "${partial_sif}" helpers.def
)
singularity test "${partial_sif}"
singularity sif list "${partial_sif}" >/dev/null

singularity exec --cleanenv "${partial_sif}" /bin/bash -lc '
set -eu
python --version
Rscript --version
pdftotext -v
magick -version
tesseract --version
samtools --version
htsfile --version
igv-snapshot-workflow --help >/dev/null
' >"${partial_inventory}" 2>&1

sif_sha256="$(sha256sum "${partial_sif}" | cut -d' ' -f1)"
chmod 0444 "${partial_sif}"
printf '%s  %s\n' "${sif_sha256}" "$(basename "${output}")" >"${partial_sha256}"
python3 - \
    "${context}/build-provenance.json" \
    "${partial_build_json}" \
    "${output}" \
    "${sif_sha256}" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
payload["sif_path"] = sys.argv[3]
payload["sif_sha256"] = sys.argv[4]
Path(sys.argv[2]).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
chmod 0444 "${partial_sha256}" "${partial_build_json}" "${partial_inventory}"
mv -- "${partial_inventory}" "${output}.inventory.txt"
mv -- "${partial_sha256}" "${output}.sha256"
mv -- "${partial_build_json}" "${output}.build.json"
mv -- "${partial_sif}" "${output}"

echo "helper_sif=${output}"
echo "helper_sif_sha256=${sif_sha256}"
