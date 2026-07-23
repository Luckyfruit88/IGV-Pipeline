#!/usr/bin/env bash
set -euo pipefail

fail() {
    printf 'runtime self-test failed: %s\n' "$*" >&2
    exit 1
}

[[ "$(uname -m)" == x86_64 ]] || fail 'runtime must be linux/amd64'
[[ "$(id -u)" != 0 ]] || fail 'runtime must execute as a non-root user'
[[ "${NXF_HOME:-}" == /* ]] || fail 'NXF_HOME must be an absolute path'
mkdir -p "${NXF_HOME}" || fail "NXF_HOME cannot be created: ${NXF_HOME}"
[[ -w "${NXF_HOME}" ]] || fail "NXF_HOME is not writable: ${NXF_HOME}"
[[ -x /opt/igv-helper/bin/igv-snapshot ]] || fail 'public igv-snapshot console script is unavailable'
[[ -r /opt/igv-pipeline/runtime-manifest.json ]] || fail 'embedded runtime manifest is unavailable'
/usr/local/bin/runtime-entrypoint --version | grep -Fx 'igv-snapshot 3.0.0'

python - <<'PY'
import importlib.metadata as metadata
import json
import sys
from pathlib import Path

assert sys.version_info[:3] == (3, 10, 12)
assert metadata.version("igv-snapshot-workflow") == "3.0.0"
assert metadata.version("Pillow") == "11.3.0"
assert metadata.version("PyYAML") == "6.0.2"
assert metadata.version("jsonschema") == "4.25.1"
manifest = json.loads(
    Path("/opt/igv-pipeline/runtime-manifest.json").read_text(encoding="utf-8")
)
assert manifest["schema_version"] == "igv-runtime-manifest-v1"
assert manifest["platform"] == "linux/amd64"
assert manifest["pipeline"] == {"name": "igv-pipeline", "version": "3.0.0"}
PY

self_test_root="$(mktemp -d "${NXF_HOME}/runtime-self-test.XXXXXX")"
cleanup() {
    rm -rf -- "${self_test_root}"
}
trap cleanup EXIT
/opt/igv-helper/bin/python /opt/igv-pipeline/pipeline/bin/validate_runtime_identity.py \
    --runtime-manifest /opt/igv-pipeline/runtime-manifest.json \
    --output-dir "${self_test_root}/manifest-validation"
grep -F '"status": "PASS"' "${self_test_root}/manifest-validation/validation.json"

Rscript -e 'stopifnot(as.character(getRversion()) == "4.5.2")'
samtools --version | grep -F 'samtools 1.18'
htsfile --version | grep -F 'htsfile (htslib) 1.18'
pdftotext -v 2>&1 | grep -F '26.07.0'
magick -version | grep -F 'ImageMagick 7.1.2-27'
tesseract --version | grep -F 'tesseract 5.5.2'
tesseract --list-langs | grep -Fx 'eng'
Xvfb -help >/dev/null 2>&1 || fail 'Xvfb executable cannot start'
command -v xwininfo >/dev/null
command -v xprop >/dev/null
command -v import >/dev/null
fc-list | grep -F 'DejaVu Sans' >/dev/null
fc-list --version 2>&1 | grep -F '2.18.1'
fc-match --format '%{family}\n' sans-serif | head -n 1 | grep -F 'DejaVu Sans' >/dev/null
/opt/java-21/bin/java -version 2>&1 | grep -Eq 'version "21\.'
/opt/java-21/bin/java -version 2>&1 | grep -F '21.0.8'
/opt/igv/bin/igv --runtime-self-test
rpm -q --qf '%{VERSION}-%{RELEASE}\n' xorg-x11-server-Xvfb \
    | grep -Fx '1.20.11-28.el8_10.3'
if ! nextflow_info="$(/usr/local/bin/nextflow info 2>&1)"; then
    nextflow_trace="$(IGV_NEXTFLOW_LAUNCHER_TRACE=true /usr/local/bin/nextflow info 2>&1 || true)"
    fail "nextflow info failed: ${nextflow_info}; launcher trace: ${nextflow_trace}"
fi
grep -Eq '^[[:space:]]*Version:[[:space:]]*25\.04\.7([[:space:]]|$)' <<<"${nextflow_info}" \
    || fail "unexpected Nextflow version: ${nextflow_info}"
grep -Eq '^[[:space:]]*Runtime:.*(^|[[:space:]])21([.+[:space:]]|$)' <<<"${nextflow_info}" \
    || fail "unexpected Nextflow Java runtime: ${nextflow_info}"

printf '%s\n' 'RUNTIME_MANIFEST_SELF_TEST=PASS'
printf '%s\n' 'PORTABLE_RUNTIME_SELF_TEST=PASS'
