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
for maintenance_command in reconcile export-snapshots; do
    /usr/local/bin/runtime-entrypoint "${maintenance_command}" --help >/dev/null
done
printf '%s\n' 'MAINTENANCE_ENTRYPOINT_SELF_TEST=PASS'

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
probe_display_pid=''
cleanup() {
    if [[ -n "${probe_display_pid}" ]]; then
        kill "${probe_display_pid}" 2>/dev/null || true
        wait "${probe_display_pid}" 2>/dev/null || true
    fi
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
printf '%s\n' \
    '@HD	VN:1.6	SO:coordinate' \
    '@SQ	SN:chr1	LN:100' \
    'read1	0	chr1	1	60	1M	*	0	0	A	I' \
    > "${self_test_root}/explicit-index.sam"
samtools view -b \
    -o "${self_test_root}/explicit-index.bam" \
    "${self_test_root}/explicit-index.sam"
samtools index \
    -o "${self_test_root}/custom-index.bai" \
    "${self_test_root}/explicit-index.bam"
samtools idxstats \
    "${self_test_root}/explicit-index.bam##idx##${self_test_root}/custom-index.bai" \
    > "${self_test_root}/explicit-index.idxstats"
grep -Fx $'chr1\t100\t1\t0' "${self_test_root}/explicit-index.idxstats"
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
(cd /opt/igv-probes && sha256sum --check --strict SHA256SUMS)
Xvfb -displayfd 3 -screen 0 640x480x24 -nolisten tcp \
    3>"${self_test_root}/probe.display" \
    >"${self_test_root}/probe.xvfb.log" 2>&1 &
probe_display_pid=$!
for probe_wait in {1..50}; do
    [[ -s "${self_test_root}/probe.display" ]] && break
    kill -0 "${probe_display_pid}" 2>/dev/null || fail 'probe Xvfb exited'
    sleep 0.1
done
read -r probe_display < "${self_test_root}/probe.display" || fail 'probe Xvfb did not become ready'
[[ "${probe_display}" =~ ^[0-9]+$ ]] || fail 'probe Xvfb returned an invalid display'
DISPLAY=":${probe_display}" LD_LIBRARY_PATH="/opt/igv-helper/lib:/opt/igv/jdk-11/lib:/opt/igv/jdk-11/lib/server${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" \
    /opt/igv/jdk-11/bin/java -Xmx128m -XX:ActiveProcessorCount=1 -Djava.awt.headless=false \
    -cp /opt/igv-probes LocusClipboardProbe --self-test \
    | grep -Fx 'LOCUS_PROBE_SELF_TEST=PASS'
kill "${probe_display_pid}"
wait "${probe_display_pid}" 2>/dev/null || true
probe_display_pid=''
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
