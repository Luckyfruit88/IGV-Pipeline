process VALIDATE_RUNTIME_IDENTITY {
    tag "${params.runtime_execution_mode}"
    label 'portable_runtime'
    label 'control'
    cache 'deep'

    publishDir "${params.session_output}/runtime_manifest", mode: 'copy', overwrite: false

    input:
    path runtime_manifest, stageAs: 'contract/runtime-manifest.json'
    path manifest_schema, stageAs: 'contract/runtime-manifest.schema.json'
    path materials_lock, stageAs: 'contract/runtime-materials.lock.json'
    path explicit_locks, stageAs: 'contract/material-locks/*'
    path runtime_config, stageAs: 'contract/v3-runtime.yaml'
    path helper_script, stageAs: 'software/validate_runtime_identity.py'
    val expected_manifest_sha256
    val expected_fingerprint_sha256
    val observed_oci_digest
    val observed_sif_sha256

    output:
    path 'runtime_manifest_validation', emit: bundle

    script:
    def expectedArgs = []
    if (expected_manifest_sha256) {
        expectedArgs << "--expected-manifest-sha256 '${expected_manifest_sha256}'"
    }
    if (expected_fingerprint_sha256) {
        expectedArgs << "--expected-fingerprint-sha256 '${expected_fingerprint_sha256}'"
    }
    if (observed_oci_digest) {
        expectedArgs << "--observed-oci-digest '${observed_oci_digest}'"
    }
    if (observed_sif_sha256) {
        expectedArgs << "--observed-sif-sha256 '${observed_sif_sha256}'"
    }
    def expectedArg = expectedArgs.join(' ')
    """
    export PYTHONDONTWRITEBYTECODE=1
    '${params.python}' '${helper_script}' \
        --runtime-manifest '${runtime_manifest}' \
        --manifest-schema '${manifest_schema}' \
        --materials-lock '${materials_lock}' \
        --explicit-lock-dir contract/material-locks \
        --runtime-config '${runtime_config}' \
        --output-dir runtime_manifest_validation \
        --allow-staged-symlink ${expectedArg}
    """

    stub:
    """
    mkdir -p runtime_manifest_validation
    printf '{"schema_version":"3.0-runtime-manifest-validation","status":"STUB"}\n' > runtime_manifest_validation/validation.json
    """
}
