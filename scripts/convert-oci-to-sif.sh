#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' \
        "usage: $0 --oci-ref ghcr.io/luckyfruit88/igv-pipeline:<tag-or-digest> --output /absolute/path/igv-pipeline.sif" >&2
}

oci_ref=""
output=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --oci-ref)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            oci_ref="$2"
            shift 2
            ;;
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

[[ "${oci_ref}" =~ ^ghcr\.io/luckyfruit88/igv-pipeline(:[A-Za-z0-9][A-Za-z0-9._-]*|@sha256:[a-f0-9]{64})$ ]] || {
    usage
    exit 2
}
[[ "${output}" == /* && "${output}" == *.sif ]] || {
    usage
    exit 2
}
[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || {
    printf '%s\n' 'OCI to SIF conversion requires Linux x86_64' >&2
    exit 2
}

if command -v apptainer >/dev/null 2>&1; then
    engine=apptainer
elif command -v singularity >/dev/null 2>&1; then
    engine=singularity
else
    printf '%s\n' 'apptainer or singularity is required' >&2
    exit 2
fi
command -v sha256sum >/dev/null 2>&1 || {
    printf '%s\n' 'sha256sum is required' >&2
    exit 2
}
for candidate in "${output}" "${output}.sha256"; do
    [[ ! -e "${candidate}" && ! -L "${candidate}" ]] || {
        printf 'refusing to overwrite runtime artifact: %s\n' "${candidate}" >&2
        exit 2
    }
done

temporary="$(mktemp -d "${TMPDIR:-/tmp}/igv-pipeline-sif.XXXXXX")"
partial_sif="${output}.partial.$$"
partial_sum="${output}.sha256.partial.$$"
completed=0
cleanup() {
    status=$?
    rm -rf -- "${temporary}"
    rm -f -- "${partial_sif}" "${partial_sum}"
    if [[ "${completed}" != 1 ]]; then
        rm -f -- "${output}" "${output}.sha256"
    fi
    exit "${status}"
}
trap cleanup EXIT

export APPTAINER_CACHEDIR="${temporary}/cache"
export APPTAINER_TMPDIR="${temporary}/tmp"
export SINGULARITY_CACHEDIR="${temporary}/cache"
export SINGULARITY_TMPDIR="${temporary}/tmp"
mkdir -p "${temporary}/cache" "${temporary}/tmp"

"${engine}" pull --arch amd64 --disable-cache "${partial_sif}" "docker://${oci_ref}"
"${engine}" exec \
    --cleanenv \
    --containall \
    --no-home \
    --env NXF_HOME=/tmp/.nextflow \
    "${partial_sif}" runtime-self-test

sif_sha256="$(sha256sum "${partial_sif}" | cut -d' ' -f1)"
printf '%s  %s\n' "${sif_sha256}" "$(basename "${output}")" >"${partial_sum}"
chmod 0444 "${partial_sif}" "${partial_sum}"
mv -- "${partial_sum}" "${output}.sha256"
mv -- "${partial_sif}" "${output}"
completed=1

printf 'runtime_sif=%s\n' "${output}"
printf 'runtime_sif_sha256=%s\n' "${sif_sha256}"
printf 'source=%s\n' "docker://${oci_ref}"
