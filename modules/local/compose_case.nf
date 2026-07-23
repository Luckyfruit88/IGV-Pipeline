process COMPOSE_CASE {
    tag "${shard_id}:${task_id}"
    label 'helper'
    label 'compose'

    publishDir "${params.run_output}/stages/compose_case", mode: 'copy', overwrite: false

    input:
    tuple val(task_id), val(meta), path(violin_pdf), path(render_bundle)
    path helper_script, stageAs: 'software/compose_case.py'
    path shard_manifest, stageAs: 'contract/shard_manifest.jsonl'
    path params_file, stageAs: 'contract/params_source'
    val shard_id
    val session_id

    output:
    tuple val(task_id), val(meta), path("compose_case_${task_id}"), emit: bundle

    script:
    def testArg = params.fake_runtime ? '--test-mode' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${helper_script}' \
        --task-manifest '${shard_manifest}' \
        --task-id '${task_id}' \
        --render-bundle '${render_bundle}' \
        --violin-pdf '${violin_pdf}' \
        --params '${params_file}' \
        --output-dir 'compose_case_${task_id}' \
        --shard-id '${shard_id}' \
        --session-id '${session_id}' \
        --attempt '${params.attempt}' \
        ${testArg} \
        --schema-dir '${params.schema_dir}'
    """

    stub:
    """
    mkdir -p 'compose_case_${task_id}'
    printf '{"schema_version":"2.0","status":"STUB","task_id":"${task_id}"}\n' > 'compose_case_${task_id}/stage_result.json'
    """
}
