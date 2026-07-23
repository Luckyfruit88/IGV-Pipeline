process QC_CASE {
    tag "${shard_id}:${task_id}"
    label 'helper'
    label 'qc'

    publishDir "${params.run_output}/stages/qc_case", mode: 'copy', overwrite: false

    input:
    tuple val(task_id), val(meta), path(validation_bundle), path(render_bundle), path(compose_bundle)
    path helper_script, stageAs: 'software/qc_case.py'
    path shard_manifest, stageAs: 'contract/shard_manifest.jsonl'
    val shard_id
    val session_id

    output:
    tuple val(task_id), val(meta), path("qc_case_${task_id}"), emit: bundle

    script:
    def testArg = params.fake_runtime ? '--test-mode' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${helper_script}' \
        --task-manifest '${shard_manifest}' \
        --task-id '${task_id}' \
        --validation-bundle '${validation_bundle}' \
        --render-bundle '${render_bundle}' \
        --compose-bundle '${compose_bundle}' \
        --output-dir 'qc_case_${task_id}' \
        --shard-id '${shard_id}' \
        --session-id '${session_id}' \
        --attempt '${params.attempt}' \
        ${testArg} \
        --schema-dir '${params.schema_dir}'
    """

    stub:
    """
    mkdir -p 'qc_case_${task_id}'
    printf '{"schema_version":"2.0","status":"STUB","task_id":"${task_id}"}\n' > 'qc_case_${task_id}/stage_result.json'
    """
}
