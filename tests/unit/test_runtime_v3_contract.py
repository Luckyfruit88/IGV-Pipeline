from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import jsonschema


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTAINERS = PROJECT_ROOT / "containers"


def _text(relative: str) -> str:
    return (PROJECT_ROOT / relative).read_text(encoding="utf-8")


def _json(relative: str) -> dict[str, object]:
    return json.loads(_text(relative))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def test_runtime_materials_are_amd64_and_byte_identified() -> None:
    materials = _json("containers/runtime-materials.lock.json")
    dockerfile = _text("containers/runtime.Dockerfile")

    assert materials["platform"] == "linux/amd64"
    assert "TARGETPLATFORM" in dockerfile
    assert '[[ "${TARGETPLATFORM}" == "linux/amd64" ]]' in dockerfile
    assert "latest" not in dockerfile.lower()

    for artifact in materials["external_artifacts"].values():
        checksum = artifact["sha256"]
        assert len(checksum) == 64
        assert f"--checksum=sha256:{checksum}" in dockerfile

    for name, checksum in materials["explicit_environment_locks"].items():
        assert _sha256(CONTAINERS / name) == checksum


def test_runtime_separates_controller_java_from_bundled_igv_java() -> None:
    dockerfile = _text("containers/runtime.Dockerfile")
    igv_wrapper = _text("containers/bin/igv")
    nextflow_wrapper = _text("containers/bin/nextflow")
    entrypoint = _text("containers/bin/runtime-entrypoint")
    assert "readonly cli=/opt/igv-helper/bin/igv-snapshot" in entrypoint
    runtime_config = _text("src/ssqtl_igv/resources/v3-runtime.yaml")
    desktop_renderer = _text("src/ssqtl_igv/desktop.py")
    self_test = _text("containers/runtime-self-test.sh")

    assert "IGV_JAVA_HOME=/opt/igv/jdk-11" in dockerfile
    assert "NXF_JAVA_HOME=/opt/java-21" in dockerfile
    assert "IGV_SNAPSHOT_PIPELINE_DIR=/opt/igv-pipeline/pipeline" in dockerfile
    assert "IGV_RUNTIME_MANIFEST=/opt/igv-pipeline/runtime-manifest.json" in dockerfile
    assert 'igv_java_home="${IGV_JAVA_HOME:-${igv_root}/jdk-11}"' in igv_wrapper
    assert 'igv_heap="${IGV_HEAP:-6g}"' in igv_wrapper
    assert '-Xmx"${igv_heap}"' in igv_wrapper
    assert "/opt/java-21" in nextflow_wrapper
    assert "nextflow-25.04.7-one.jar" in nextflow_wrapper
    assert "nextflow-25.04.7-launcher" in nextflow_wrapper
    assert 'export NXF_JAVA_HOME="${controller_java_home}"' in nextflow_wrapper
    assert 'export NXF_BIN="${nextflow_jar}"' in nextflow_wrapper
    assert "IGV_NEXTFLOW_LAUNCHER_TRACE" in nextflow_wrapper
    assert "ADD --chmod=0444 --checksum=sha256:231a3c0f" in dockerfile
    assert "ADD --chmod=0555 --checksum=sha256:a57f8042" in dockerfile
    assert "chmod 0555 /opt/nextflow" in dockerfile
    assert "chmod 0444 /opt/nextflow/nextflow-25.04.7-one.jar" in dockerfile
    assert 'exec "${nextflow_launcher}" "$@"' in nextflow_wrapper
    assert 'ENTRYPOINT ["runtime-entrypoint"]' in dockerfile
    assert 'CMD ["--help"]' in dockerfile
    assert 'doctor|run|review|publish|campaign)' in entrypoint
    assert 'init|import-v2)' in entrypoint
    assert '--help|-h|--version)' in entrypoint
    assert 'exec "${cli}" "$@"' in entrypoint
    assert '[[ "$1" == "nextflow" ]]' in entrypoint
    assert '[[ "$1" == "run" ]]' in entrypoint
    assert 'exec /usr/local/bin/nextflow "$@"' in entrypoint
    assert '[[ "$1" == "/bin/bash" ]]' in entrypoint
    assert '[[ "$(id -u)" != 0 ]]' in entrypoint
    assert '[[ "$1" == "run" ||' in entrypoint
    assert '"${2:-}" == "prepare-master"' in entrypoint
    assert '"${2:-}" == "run-batch"' in entrypoint
    assert "/usr/local/bin/runtime-self-test >/dev/null" in entrypoint
    assert '[[ "$2" == "-ue" && "$3" == /*/.command.sh ]]' in entrypoint
    assert '[[ "$2" == /*/.command.run && "$3" == "nxf_trace" ]]' in entrypoint
    assert "command_listener_enabled: false" in runtime_config
    assert "command_port_base" not in runtime_config
    assert 'config.get("desktop.command_listener_enabled", True)' in desktop_renderer
    assert '"command_listener_enabled"' in desktop_renderer
    assert '"LC_ALL": "C.UTF-8"' in desktop_renderer
    assert '"LANG": "C.UTF-8"' in desktop_renderer
    assert "--port, and --batch" not in igv_wrapper
    assert "PORTABLE_RUNTIME_SELF_TEST=PASS" in self_test
    assert "/usr/local/bin/nextflow info" in self_test
    assert "launcher trace" in self_test
    assert (
        "IGV_NEXTFLOW_LAUNCHER_TRACE=true /usr/local/bin/nextflow info"
        in self_test
    )
    assert "bash -x /usr/local/bin/nextflow info" not in self_test
    assert "Runtime:" in self_test
    assert 'mkdir -p "${NXF_HOME}"' in self_test
    assert "RUN NXF_HOME=/work/.nextflow runtime-self-test" in dockerfile
    assert "USER 65532:65532" in dockerfile


def test_contract_ci_uses_the_checksum_pinned_nextflow_launcher() -> None:
    workflow = _text(".github/workflows/ci.yml")

    assert "NXF_BIN: ${{ runner.temp }}/nextflow-25.04.7-one.jar" in workflow
    assert "NXF_LAUNCHER: ${{ runner.temp }}/nextflow-25.04.7-launcher" in workflow
    assert "a57f804243c6fa3b1e3194ab05a054f7799b5d4423049b62bbb171530dba9fe2" in workflow
    assert 'chmod 0555 "${NXF_LAUNCHER}"' in workflow
    assert '"${NXF_LAUNCHER}" lint .' in workflow
    assert workflow.count('"${NXF_LAUNCHER}" run .') == 3
    assert "java -jar" not in workflow


def test_runtime_entrypoint_self_tests_only_execution_capable_commands() -> None:
    entrypoint = _text("containers/bin/runtime-entrypoint")

    assert entrypoint.count("/usr/local/bin/runtime-self-test >/dev/null") == 2
    nextflow_condition = entrypoint[
        entrypoint.index('if [[ "$1" == "nextflow"')
        : entrypoint.index('case "$1" in')
    ]
    assert '[[ "$1" == "run" ]]' in nextflow_condition
    assert 'exec /usr/local/bin/nextflow "$@"' in nextflow_condition
    condition = entrypoint[
        entrypoint.index('if [[ "$1" == "run"')
        : entrypoint.rindex("/usr/local/bin/runtime-self-test >/dev/null")
    ]
    assert '"${2:-}" == "prepare-master"' in condition
    assert '"${2:-}" == "run-batch"' in condition
    for control_only in ("prepare", "status", "next"):
        assert f'"${{2:-}}" == "{control_only}"' not in condition


def test_runtime_entrypoint_allows_only_exact_nextflow_worker_shell(tmp_path: Path) -> None:
    entrypoint = CONTAINERS / "bin/runtime-entrypoint"
    worker = tmp_path / ".command.sh"
    worker.write_text("printf 'NEXTFLOW_WORKER_OK\\n'\n", encoding="utf-8")
    trace_worker = tmp_path / ".command.run"
    trace_worker.write_text(
        "[[ \"${1:-}\" == nxf_trace ]] || exit 93\n"
        "printf 'NEXTFLOW_TRACE_WORKER_OK\\n'\n",
        encoding="utf-8",
    )

    allowed = subprocess.run(
        ["bash", str(entrypoint), "/bin/bash", "-ue", str(worker)],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert allowed.returncode == 0
    assert allowed.stdout == "NEXTFLOW_WORKER_OK\n"

    trace_allowed = subprocess.run(
        ["bash", str(entrypoint), "/bin/bash", str(trace_worker), "nxf_trace"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert trace_allowed.returncode == 0
    assert trace_allowed.stdout == "NEXTFLOW_TRACE_WORKER_OK\n"

    for command in (
        ["/bin/bash", "-c", str(worker)],
        ["/bin/bash", "-ue", "/tmp/not-a-nextflow-worker.sh"],
        ["/bin/bash", str(trace_worker), "not_nxf_trace"],
        ["/bin/bash", "-ue", str(trace_worker)],
        ["/bin/sh", str(worker)],
    ):
        blocked = subprocess.run(
            ["bash", str(entrypoint), *command],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert blocked.returncode == 64
        assert "runtime-entrypoint:" in blocked.stderr


def test_runtime_manifest_schema_accepts_unsigned_embedded_contract() -> None:
    schema = _json("containers/runtime-manifest.schema.json")
    jsonschema.Draft202012Validator.check_schema(schema)
    payload = {
        "schema_version": "igv-runtime-manifest-v1",
        "workflow_schema_version": "3.0",
        "pipeline": {"name": "igv-pipeline", "version": "3.0.0"},
        "platform": "linux/amd64",
        "source": {"commit": "c" * 40, "tree": "d" * 40},
        "materials": {
            "path": "containers/runtime-materials.lock.json",
            "sha256": "e" * 64,
        },
        "render_contract": {
            "screen": "1920x2160x24",
            "igv_heap": "6g",
            "locale": "C.UTF-8",
            "font_family": "DejaVu Sans",
            "command_listener_enabled": False,
            "config_path": "src/ssqtl_igv/resources/v3-runtime.yaml",
            "config_sha256": "f" * 64,
        },
        "tools": {
            "igv": "2.16.2",
            "igv_jre": "11",
            "nextflow": "25.04.7",
            "controller_java": "21.0.8+9",
            "python": "3.10.12",
            "r": "4.5.2",
            "samtools": "1.18",
            "poppler": "26.07.0",
            "imagemagick": "7.1.2_27",
            "fontconfig": "2.18.1",
            "fonts": "DejaVu Sans",
            "tesseract": "5.5.2",
            "xvfb": "1.20.11-28.el8_10.3",
        },
    }
    jsonschema.Draft202012Validator(schema).validate(payload)
    assert "oci" not in payload
    assert "sif" not in payload
    assert "created_at" not in payload
    assert "host" not in payload


def test_profiles_keep_portable_and_site_native_execution_separate() -> None:
    root_config = _text("nextflow.config")
    base = _text("conf/base.config")
    standalone = _text("conf/standalone.config")
    docker = _text("conf/docker.config")
    scc = _text("conf/scc.config")
    test = _text("conf/test.config")

    for profile in ("standalone", "docker", "scc"):
        assert f"{profile} {{" in root_config
    for parameter in (
        "runtime_image",
        "observed_oci_digest",
        "runtime_manifest",
        "runtime_manifest_sha256",
        "runtime_fingerprint_sha256",
        "runtime_sif",
        "runtime_sif_sha256",
    ):
        assert parameter in base

    assert "docker.enabled = false" in standalone
    assert "singularity.enabled = false" in standalone
    assert "withLabel: portable_runtime" in docker
    assert "container = params.runtime_image" in docker
    assert "stageInMode = 'copy'" in docker
    assert "--read-only" in docker
    assert "--network=none" in docker
    assert "container = params.helper_sif" in scc
    assert "container = params.runtime_sif" in scc
    assert "singularity" in scc
    assert "igv/2.16.2" in scc
    assert "withLabel: accounting" in scc
    assert "runtime_image = null" in test
    assert "docker.enabled = false" in test


def test_v3_portable_label_does_not_mutate_frozen_v2_process_labels() -> None:
    v2_helper_modules = (
        "aggregate_shards.nf",
        "build_review_package.nf",
        "compose_case.nf",
        "create_shards.nf",
        "publish_reviewed.nf",
        "qc_case.nf",
        "summarize_shard.nf",
        "validate_and_normalize.nf",
        "validate_case_inputs.nf",
        "validate_review.nf",
    )
    for name in v2_helper_modules:
        text = _text(f"modules/local/{name}")
        assert "label 'helper'" in text, name
        assert "label 'portable_runtime'" not in text, name

    portable = _text("modules/local/run_portable_case.nf")
    legacy_igv = _text("modules/local/run_igv.nf")
    accounting = _text("modules/local/collect_qacct.nf")
    environment = _text("modules/local/validate_environment.nf")
    assert "label 'portable_runtime'" in portable
    assert "label 'portable_render'" in portable
    assert "label 'igv_render'" not in portable
    assert "label 'igv_render'" in legacy_igv
    assert "label 'portable_runtime'" not in legacy_igv
    assert "label 'accounting'" in accounting
    assert "label 'portable_runtime'" not in accounting
    assert "label 'environment'" in environment
    assert "label 'portable_runtime'" not in environment

    scc = _text("conf/scc.config")
    assert "withLabel: portable_render" in scc
    assert scc.count("containerOptions = '--containall --no-home'") == 2


def test_portable_run_closes_runtime_manifest_preflight_before_cases() -> None:
    workflow = _text("workflows/portable_run.nf")
    validator = _text("modules/local/validate_runtime_identity.nf")
    portable = _text("modules/local/run_portable_case.nf")

    assert "include { VALIDATE_RUNTIME_IDENTITY }" in workflow
    assert "VALIDATE_RUNTIME_IDENTITY(" in workflow
    assert "VALIDATE_RUNTIME_IDENTITY.out.bundle.first()" in workflow
    assert "runtimeValidation" in workflow
    assert "runtime_manifest_validation = runtimeValidation" in workflow
    assert "fingerprintSha" in workflow
    assert "val expected_fingerprint_sha256" in validator
    assert "--expected-fingerprint-sha256" in validator
    assert "val runtime_fingerprint_sha256" in portable
    assert "IGV_RUNTIME_FINGERPRINT_SHA256" in portable
    assert "val runtime_validation_identity" in portable
    assert "runtimeValidationIdentity" in workflow
    assert "['PASS', 'STUB']" in workflow
    assert "--explicit-lock-dir contract/material-locks" in validator
    assert "--observed-oci-digest" in validator
    assert "--observed-sif-sha256" in validator


def test_runtime_build_and_conversion_scripts_parse_as_bash() -> None:
    scripts = [
        PROJECT_ROOT / "scripts/build-runtime-oci.sh",
        PROJECT_ROOT / "scripts/convert-oci-to-sif.sh",
        PROJECT_ROOT / "scripts/scc-controller-launcher-v3.sh",
        CONTAINERS / "bin/igv",
        CONTAINERS / "bin/nextflow",
        CONTAINERS / "bin/runtime-entrypoint",
        CONTAINERS / "runtime-self-test.sh",
    ]
    for script in scripts:
        subprocess.run(["bash", "-n", str(script)], check=True)

    build = scripts[0].read_text(encoding="utf-8")
    conversion = scripts[1].read_text(encoding="utf-8")
    assert "--push" not in build
    assert "--provenance=mode=max" in build
    assert "/opt/igv-pipeline/runtime-manifest.json" in build
    assert "runtime_self_test=PASS" in build
    assert "cosign" not in build
    assert "--identity-output" not in conversion
    assert "cosign" not in conversion
    assert " sign " not in conversion
    assert 'engine=apptainer' in conversion
    assert 'engine=singularity' in conversion
    assert 'pull --arch amd64 --disable-cache' in conversion
    assert 'runtime-self-test' in conversion
    assert 'sha256sum "${partial_sif}"' in conversion
    assert "--cleanenv" in conversion
    assert "--containall" in conversion
    assert "--no-home" in conversion
    assert "--env NXF_HOME=/tmp/.nextflow" in conversion


def test_nextflow_wrapper_trace_executes_the_pinned_launcher(tmp_path: Path) -> None:
    java_home = tmp_path / "java-21"
    java_bin = java_home / "bin"
    java_bin.mkdir(parents=True)
    java = java_bin / "java"
    java.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    java.chmod(0o755)

    nextflow_dir = tmp_path / "nextflow"
    nextflow_dir.mkdir()
    jar = nextflow_dir / "nextflow-25.04.7-one.jar"
    jar.write_bytes(b"fixture-jar")
    launcher = nextflow_dir / "nextflow-25.04.7-launcher"
    launcher.write_text(
        "#!/usr/bin/env bash\n"
        "printf 'PINNED_LAUNCHER:%s\\n' \"$1\"\n",
        encoding="utf-8",
    )
    launcher.chmod(0o755)

    wrapper = tmp_path / "nextflow-wrapper"
    wrapper.write_text(
        _text("containers/bin/nextflow")
        .replace("/opt/java-21", str(java_home))
        .replace(
            "/opt/nextflow/nextflow-25.04.7-one.jar",
            str(jar),
        )
        .replace(
            "/opt/nextflow/nextflow-25.04.7-launcher",
            str(launcher),
        ),
        encoding="utf-8",
    )
    wrapper.chmod(0o755)

    completed = subprocess.run(
        [str(wrapper), "info"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={"IGV_NEXTFLOW_LAUNCHER_TRACE": "true", "PATH": "/usr/bin:/bin"},
    )

    assert completed.returncode == 0
    assert completed.stdout == "PINNED_LAUNCHER:info\n"
    assert completed.stderr == "+ printf 'PINNED_LAUNCHER:%s\\n' info\n"


def test_deferred_scc_controller_launcher_fails_closed() -> None:
    path = PROJECT_ROOT / "scripts/scc-controller-launcher-v3.sh"
    launcher = path.read_text(encoding="utf-8")

    assert "distributed host-Nextflow/SGE controller mode is deferred" in launcher
    assert "submit-bu-scc-pull-run.sh" in launcher
    assert "exec igv-snapshot run" not in launcher
    assert "--profile scc" not in launcher
    completed = subprocess.run(
        ["bash", str(path)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 64
    assert "controller mode is deferred" in completed.stderr
    assert "available in the IGV Pipeline 3.0 pull-only release" in completed.stderr


def test_candidate_workflow_builds_one_hardened_public_main_oci() -> None:
    candidate = _text(".github/workflows/pilot-candidate.yml")

    assert "workflow_dispatch:" in candidate
    assert "pull_request:" not in candidate
    assert "REPOSITORY_PRIVATE" in candidate
    assert 'test "$REPOSITORY_PRIVATE" = "false"' in candidate
    assert 'test "$GITHUB_REF" = "refs/heads/main"' in candidate
    assert "refs/remotes/origin/main" in candidate
    assert "pilot-${{ steps.source.outputs.commit }}" not in candidate
    assert "printf 'tag=pilot-%s" in candidate
    assert candidate.count("docker/build-push-action@") == 1
    assert "platforms: linux/amd64" in candidate
    assert "push: true" in candidate
    assert "provenance: mode=max" in candidate
    assert "sbom: true" in candidate
    assert "SOURCE_COMMIT=${{ steps.source.outputs.commit }}" in candidate
    assert "SOURCE_TREE=${{ steps.source.outputs.tree }}" in candidate
    assert "--pull always" in candidate
    assert "--user 65532:65532" in candidate
    assert "--read-only" in candidate
    assert "--cap-drop ALL" in candidate
    assert "--security-opt no-new-privileges" in candidate
    assert "--network none" in candidate
    assert '${IMAGE}@${{ steps.build.outputs.digest }}' in candidate
    assert "OCI_DIGEST.txt" in candidate
    assert "SOURCE_COMMIT.txt" in candidate
    assert "SOURCE_TREE.txt" in candidate
    assert "embedded-source.json" in candidate
    assert "SHA256SUMS" in candidate
    assert 'if [[ "$status" != "404" ]]' in candidate
    assert "cannot prove pilot candidate absence" in candidate
    for removed_gate in ("cosign", "public_key", "certification", "jre11_risk"):
        assert removed_gate not in candidate.lower()


def test_release_workflow_promotes_candidate_supply_chain_evidence() -> None:
    release = _text(".github/workflows/release.yml")

    assert 'tags:\n      - "v3.0.0"' in release
    assert "IMAGE: ghcr.io/luckyfruit88/igv-pipeline" in release
    assert "refs/remotes/origin/main" in release
    assert "scripts/verify-v3-release-tag.py" in release
    assert "--expected-commit" in release
    assert "pilot-candidate.yml/runs" in release
    assert "gh run download" in release
    assert "sha256sum --check --strict SHA256SUMS" in release
    assert "embedded-source.json" in release
    assert "pilot-%s" in release
    assert "docker/build-push-action@" not in release
    assert "docker buildx imagetools create" in release
    assert '--tag "${IMAGE}:3.0.0"' in release
    assert '--tag "${IMAGE}:3.0"' in release
    assert '--tag "${IMAGE}:latest"' in release
    assert '"$immutable"' in release
    assert "test \"$release_digest\" = \"$pilot_digest\"" in release
    assert "OCI_DIGEST.txt" in release
    assert "PILOT_REFERENCE.txt" in release
    assert "PILOT_CANDIDATE_RUN_ID.txt" in release
    assert "RELEASE_TAG_DIGESTS.txt" in release
    assert "SHA256SUMS" in release
    assert "trivy-report.json" in release
    assert 'exit-code: "0"' in release
    for removed_gate in ("cosign", "public_key", "certification", "jre11_risk"):
        assert removed_gate not in release.lower()


def test_runtime_debug_is_separate_local_only_and_fail_closed() -> None:
    production = _text("containers/runtime.Dockerfile").lower()
    debug = _text("containers/runtime-debug.Dockerfile")
    materials = _json("containers/runtime-debug-materials.lock.json")
    policy = _json("containers/runtime-debug-policy.json")
    builder = _text("scripts/build-runtime-debug.sh")
    launcher = _text("scripts/run-runtime-debug.sh")
    production_ignore = _text(".dockerignore")
    debug_ignore = _text("containers/runtime-debug.Dockerfile.dockerignore")

    effective_ignore_lines = [
        line.strip()
        for line in production_ignore.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert effective_ignore_lines[0] == "**"
    assert "COPY ." not in production
    assert "COPY ." not in _text("containers/runtime.Dockerfile")

    for debug_tool in ("novnc", "websockify", "x11vnc"):
        assert debug_tool not in production
    assert "FROM ${PRODUCTION_RUNTIME_REF}" in debug
    assert 'org.opencontainers.image.runtime.role="runtime-debug"' in debug
    assert 'org.opencontainers.image.artifact.class="DEBUG_ONLY"' in debug
    assert 'org.opencontainers.image.review.eligible="false"' in debug
    assert 'org.opencontainers.image.publication.eligible="false"' in debug
    assert "ADD http" not in debug
    assert "containers/debug-runtime-tools.tar.gz" in production_ignore
    assert "containers/debug-runtime-tools.tar.gz" not in debug_ignore.splitlines()

    assert materials["lock_state"] == "MISSING_MATERIALS"
    assert materials["debug_tools_bundle"]["sha256"] is None
    assert materials["availability"]["state"] == "MISSING_MATERIALS"
    assert policy["host_publish_address"] == "127.0.0.1"
    assert policy["artifact_class"] == "DEBUG_ONLY"
    assert policy["production_eligible"] is False
    assert policy["review_eligible"] is False
    assert policy["publication_eligible"] is False

    assert "runtime-debug materials are not locked; refusing build" in builder
    assert "DEBUG_TOOLS_BUNDLE_SHA256" in builder
    assert "artifact_class=%s" in builder
    assert "certification" not in builder.lower()
    assert "--push" not in builder
    assert '--publish "127.0.0.1:${host_port}:6080"' in launcher
    assert "docker network create --driver bridge --internal" in launcher
    assert "--read-only" in launcher
    assert "--cap-drop ALL" in launcher
    assert "--security-opt no-new-privileges" in launcher
    assert "/var/run/docker.sock" not in launcher


def test_debug_capture_is_visibly_marked_and_rejected_by_production_gate(
    tmp_path: Path,
) -> None:
    entrypoint = _text("containers/bin/runtime-debug-entrypoint")
    screenshot = _text("containers/bin/debug-screenshot")
    gate = PROJECT_ROOT / "scripts/verify-production-artifact-tree.py"

    assert '"artifact_class": "DEBUG_ONLY"' in entrypoint
    assert '"review_eligible": false' in entrypoint
    assert '"publication_eligible": false' in entrypoint
    assert "/opt/igv/bin/igv --runtime-self-test" in entrypoint
    assert '--igvDirectory "${igv_directory}"' in entrypoint
    assert 'igv_directory=/run/home/igv-debug' in entrypoint
    assert "igv_pid" in entrypoint
    assert "DEBUG_ONLY.png" in screenshot
    assert "-annotate +24+24 'DEBUG_ONLY'" in screenshot
    assert "DEBUG_ONLY.json" in screenshot

    candidate = tmp_path / "candidate"
    candidate.mkdir()
    (candidate / "ordinary.json").write_text(
        json.dumps({"schema_version": "unit-test", "status": "PASS"}),
        encoding="utf-8",
    )
    passed = subprocess.run(
        [sys.executable, str(gate), "--tree", str(candidate.resolve())],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert passed.returncode == 0
    assert "PRODUCTION_ARTIFACT_GATE=PASS" in passed.stdout

    (candidate / "diagnostic-marker.json").write_text(
        json.dumps({"debug_only": True}), encoding="utf-8"
    )
    blocked = subprocess.run(
        [sys.executable, str(gate), "--tree", str(candidate.resolve())],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert blocked.returncode == 2
    assert "PRODUCTION_ARTIFACT_GATE=BLOCKED" in blocked.stderr


def test_release_tags_share_one_build_digest_and_maintainer_artifact_set() -> None:
    candidate = _text(".github/workflows/pilot-candidate.yml")
    release = _text(".github/workflows/release.yml")

    assert candidate.count("docker/build-push-action@") == 1
    assert release.count("docker/build-push-action@") == 0
    assert "provenance: mode=max" in candidate
    assert "sbom: true" in candidate
    assert "provenance:" not in release
    assert "sbom:" not in release
    assert release.count("steps.pilot_evidence.outputs.digest") >= 3
    assert release.index("Require public immutable pilot artifact") < release.index(
        "Log in to GHCR"
    )
    assert "test \"$(jq -r '.digest' \"$tagged_manifest\")\" = \"$pilot_digest\"" in release
    assert "for tag in 3.0.0 3.0 latest" in release
    assert "name: igv-pipeline-3.0.0-release-evidence" in release
    assert "gh release create" in release
    assert "--verify-tag" in release
    assert "signed" not in release.lower()


def test_debug_and_release_gate_scripts_parse() -> None:
    shell_scripts = (
        "containers/bin/runtime-debug-entrypoint",
        "containers/bin/debug-screenshot",
        "scripts/build-runtime-debug.sh",
        "scripts/run-runtime-debug.sh",
    )
    for relative in shell_scripts:
        subprocess.run(["bash", "-n", str(PROJECT_ROOT / relative)], check=True)
    for relative in ("scripts/verify-production-artifact-tree.py",):
        compile(_text(relative), relative, "exec")
