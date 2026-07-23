process SUMMARIZE_SHARD {
    tag "${shard_id}:${session_id}"
    label 'helper'
    label 'aggregate'

    publishDir "${params.run_output}/summary", mode: 'copy', overwrite: false

    input:
    path shard_manifest, stageAs: 'contract/shard_manifest.jsonl'
    path qc_bundles, stageAs: 'qc_bundles/??/*'
    path helper_script, stageAs: 'software/summarize_shard.py'
    val shard_id
    val session_id

    output:
    path "shard_summary_${shard_id}", emit: bundle

    script:
    def qcList = qc_bundles instanceof java.util.Collection ? qc_bundles : [qc_bundles]
    def qcArgs = qcList.collect { item -> "--qc-bundle '${item}'" }.join(' ')
    def controllerArg = params.controller_job_id ? "--controller-job-id '${params.controller_job_id}'" : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    cp -L '${shard_manifest}' shard_manifest.input.jsonl
    '${params.python}' '${helper_script}' \
        --shard-manifest shard_manifest.input.jsonl \
        ${qcArgs} \
        --output-dir 'shard_summary_${shard_id}' \
        --shard-id '${shard_id}' \
        --session-id '${session_id}' \
        --pipeline-commit '${params.pipeline_commit}' \
        ${controllerArg} \
        --schema-dir '${params.schema_dir}'
    """

    stub:
    """
    mkdir -p 'shard_summary_${shard_id}'
    : > 'shard_summary_${shard_id}/case_results.jsonl'
    : > 'shard_summary_${shard_id}/shard_ledger.jsonl'
    printf '{"schema_version":"2.0","status":"STUB","shard_id":"${shard_id}"}\n' > 'shard_summary_${shard_id}/shard_summary.json'
    """
}
