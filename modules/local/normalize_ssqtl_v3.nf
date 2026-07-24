def ssqtlNormalizationPolicy(policyDocument) {
    return new groovy.json.JsonSlurperClassic()
        .parseText(policyDocument.toString())
        .normalization
}


process NORMALIZE_SSQTL_V3 {
    tag "${run_id}:${generation_id}"
    label 'portable_runtime'
    label 'adaptive_normalization'
    cache 'deep'
    cpus { ssqtlNormalizationPolicy(execution_policy_doc).cpus as int }
    memory { "${ssqtlNormalizationPolicy(execution_policy_doc).memory_bytes} B" }
    time { "${ssqtlNormalizationPolicy(execution_policy_doc).timeout_seconds} sec" }
    errorStrategy 'terminate'
    maxRetries 0

    publishDir { normalization_output }, mode: 'copy', overwrite: false

    input:
    val run_id
    val generation_id
    val associations
    val rds_dir
    val bam_lookup
    val violin_dir
    val input_root
    val reference
    val adapter_config
    val normalization_output
    path bind_contract, stageAs: 'contract/ssqtl_bind_contract.json'
    path source_binding, stageAs: 'contract/source_binding.json'
    path runtime_validation, stageAs: 'contract/runtime-manifest-validation'
    val runtime_fingerprint_sha256
    path execution_policy, stageAs: 'contract/execution_policy.json'
    val execution_policy_doc

    output:
    path 'normalization_bundle', emit: bundle

    script:
    def configArg = adapter_config ? "--config '${adapter_config}'" : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export LC_ALL=C.UTF-8
    export LANG=C.UTF-8
    export IGV_RUNTIME_FINGERPRINT_SHA256='${runtime_fingerprint_sha256}'
    test -s '${bind_contract}'
    test -s '${source_binding}'
    '${params.python}' -m ssqtl_igv.ssqtl_adapter_v3 \
        --associations '${associations}' \
        --rds-dir '${rds_dir}' \
        --bam-lookup '${bam_lookup}' \
        --violin-dir '${violin_dir}' \
        --input-root '${input_root}' \
        --reference '${reference}' \
        --output-dir normalization_bundle \
        --run-id '${run_id}' \
        --generation-id '${generation_id}' \
        ${configArg}
    """

    stub:
    """
    mkdir -p normalization_bundle
    printf '{"schema_version":"3.0","pipeline_version":"3.0.0","adapter_id":"ssqtl","adapter_schema_version":"3.0-ssqtl","run_id":"${run_id}","generation_id":"${generation_id}","status":"STUB","task_count":0}\n' > normalization_bundle/validation.json
    : > normalization_bundle/tasks.jsonl
    printf 'manifest_order\ttask_id\tinput_fingerprint\n' > normalization_bundle/normalized_manifest.tsv
    printf '{"schema_version":"3.0","run_id":"${run_id}","generation_id":"${generation_id}","status":"STUB"}\n' > normalization_bundle/parameters.json
    printf '{"schema_version":"3.0-ssqtl-preparation","status":"STUB"}\n' > normalization_bundle/ssqtl_preparation.json
    printf 'association_row\tag_site\tsnp\n' > normalization_bundle/prepared_cases.tsv
    printf 'sample_id\tgenotype\n' > normalization_bundle/prepared_samples.tsv
    : > normalization_bundle/r_prepare.stdout.log
    : > normalization_bundle/r_prepare.stderr.log
    printf '{"schema_version":"3.0-ssqtl-r-prepare","status":"STUB"}\n' > normalization_bundle/r_prepare.json
    """
}
