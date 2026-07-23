#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' \
        "usage: $0 --image ghcr.io/...runtime-debug@sha256:<64hex> --input /absolute/input --reference /absolute/reference --debug-output /absolute/empty-dir [--port 6080]" \
        "The browser endpoint is exposed only as http://127.0.0.1:<port>/vnc.html." >&2
}

image=""
input_dir=""
reference_dir=""
debug_output=""
host_port="6080"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --image)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            image="$2"
            shift 2
            ;;
        --input)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            input_dir="$2"
            shift 2
            ;;
        --reference)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            reference_dir="$2"
            shift 2
            ;;
        --debug-output)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            debug_output="$2"
            shift 2
            ;;
        --port)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            host_port="$2"
            shift 2
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

[[ "${image}" =~ ^ghcr\.io/luckyfruit88/igv-pipeline-debug@sha256:[a-f0-9]{64}$ ]] || {
    echo "runtime-debug must use its own digest-pinned GHCR repository" >&2
    exit 2
}
[[ "${host_port}" =~ ^[0-9]+$ ]] || {
    echo "debug host port must be an integer" >&2
    exit 2
}
(( 10#${host_port} >= 1024 && 10#${host_port} <= 65535 )) || {
    echo "debug host port must be between 1024 and 65535" >&2
    exit 2
}

for path_name in "${input_dir}" "${reference_dir}" "${debug_output}"; do
    [[ "${path_name}" == /* && -d "${path_name}" && ! -L "${path_name}" ]] || {
        echo "input, reference, and debug-output must be absolute non-symlink directories" >&2
        exit 2
    }
done
input_dir="$(cd "${input_dir}" && pwd -P)"
reference_dir="$(cd "${reference_dir}" && pwd -P)"
debug_output="$(cd "${debug_output}" && pwd -P)"
[[ -w "${debug_output}" ]] || {
    echo "debug-output is not writable" >&2
    exit 2
}
paths_overlap() {
    [[ "$1" == "$2" || "$1" == "$2/"* || "$2" == "$1/"* ]]
}
if paths_overlap "${debug_output}" "${input_dir}" \
    || paths_overlap "${debug_output}" "${reference_dir}"; then
    echo "debug-output must be isolated from input and reference" >&2
    exit 2
fi
[[ ! -e "${debug_output}/DEBUG_ONLY.json" && ! -L "${debug_output}/DEBUG_ONLY.json" ]] || {
    echo "debug-output already contains a debug generation marker" >&2
    exit 2
}
[[ -z "$(find "${debug_output}" -mindepth 1 -maxdepth 1 -print -quit)" ]] || {
    echo "debug-output must be empty for a new diagnostic generation" >&2
    exit 2
}
command -v docker >/dev/null 2>&1 || {
    echo "docker is required to run runtime-debug" >&2
    exit 2
}

network_name="igv-snapshot-debug-$$-${RANDOM}"
network_created=false
cleanup() {
    status=$?
    if [[ "${network_created}" == true ]]; then
        docker network rm "${network_name}" >/dev/null 2>&1 || true
    fi
    exit "${status}"
}
trap cleanup EXIT INT TERM

# An internal bridge permits host-to-container port forwarding without giving
# the diagnostic container an outbound route. The published host address is
# fixed, not user-configurable.
docker network create --driver bridge --internal "${network_name}" >/dev/null
network_created=true

printf 'runtime-debug URL: http://127.0.0.1:%s/vnc.html\n' "${host_port}"
printf '%s\n' 'All files in the selected output directory are DEBUG_ONLY.'
docker run --rm \
    --platform linux/amd64 \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --pids-limit 512 \
    --network "${network_name}" \
    --publish "127.0.0.1:${host_port}:6080" \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=256m \
    --tmpfs /run/home:rw,noexec,nosuid,nodev,size=128m,uid=65532,gid=65532,mode=0700 \
    --mount "type=bind,src=${input_dir},dst=/input,readonly" \
    --mount "type=bind,src=${reference_dir},dst=/reference,readonly" \
    --mount "type=bind,src=${debug_output},dst=/run/debug-only" \
    --env IGV_SNAPSHOT_IMAGE_ROLE=runtime-debug \
    --env IGV_SNAPSHOT_ARTIFACT_CLASS=DEBUG_ONLY \
    --env DEBUG_OUTPUT_ROOT=/run/debug-only \
    --label org.igv-snapshot.artifact-class=DEBUG_ONLY \
    --entrypoint runtime-debug-entrypoint \
    "${image}"
