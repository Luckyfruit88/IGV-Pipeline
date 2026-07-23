process RUN_IGV {
    tag "${shard_id}:${task_id}"
    label 'igv_render'

    publishDir "${params.run_output}/stages/run_igv", mode: 'copy', overwrite: false

    input:
    tuple val(task_id), val(meta), path(staged_inputs, stageAs: 'inputs/??/*'), path(validation_bundle)
    path helper_script, stageAs: 'software/run_igv_case.py'
    path shard_manifest, stageAs: 'contract/shard_manifest.jsonl'
    path params_file, stageAs: 'contract/params_source'
    val shard_id
    val session_id

    output:
    tuple val(task_id), val(meta), path("run_igv_${task_id}"), emit: bundle

    script:
    def stagedList = staged_inputs instanceof java.util.Collection ? staged_inputs : [staged_inputs]
    def stagedArgs = stagedList.collect { item -> "--staged-input '${item}'" }.join(' ')
    def testArg = params.fake_runtime ? '--test-mode' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${helper_script}' \
        --task-manifest '${shard_manifest}' \
        --task-id '${task_id}' \
        ${stagedArgs} \
        --validation-bundle '${validation_bundle}' \
        --params '${params_file}' \
        --output-dir 'run_igv_${task_id}' \
        --shard-id '${shard_id}' \
        --session-id '${session_id}' \
        --attempt '${params.attempt}' \
        ${testArg} \
        --schema-dir '${params.schema_dir}'
    """

    stub:
    """
    mkdir -p 'run_igv_${task_id}'
    printf '{"schema_version":"2.0","status":"STUB","task_id":"${task_id}"}\n' > 'run_igv_${task_id}/stage_result.json'
    """
}
