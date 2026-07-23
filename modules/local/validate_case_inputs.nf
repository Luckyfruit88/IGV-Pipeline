process VALIDATE_CASE_INPUTS {
    tag "${shard_id}:${task_id}"
    label 'helper'
    label 'qc'

    publishDir "${params.run_output}/stages/validate_case_inputs", mode: 'copy', overwrite: false

    input:
    tuple val(task_id), val(meta), path(staged_inputs, stageAs: 'inputs/??/*')
    path environment_bundle
    path gate_script, stageAs: 'software/assert_gate.py'
    path helper_script, stageAs: 'software/validate_case_inputs.py'
    path shard_manifest, stageAs: 'contract/shard_manifest.jsonl'
    val shard_id
    val session_id

    output:
    tuple val(task_id), val(meta), path("validate_case_inputs_${task_id}"), emit: bundle

    script:
    def stagedList = staged_inputs instanceof java.util.Collection ? staged_inputs : [staged_inputs]
    def stagedArgs = stagedList.collect { item -> "--staged-input '${item}'" }.join(' ')
    def testArg = params.fake_runtime ? '--test-mode' : ''
    def gateTestArg = params.test_mode ? '--allow-test' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${gate_script}' --kind environment --phase run_shard --bundle '${environment_bundle}' ${gateTestArg}
    '${params.python}' '${helper_script}' \
        --task-manifest '${shard_manifest}' \
        --task-id '${task_id}' \
        ${stagedArgs} \
        --output-dir 'validate_case_inputs_${task_id}' \
        --shard-id '${shard_id}' \
        --session-id '${session_id}' \
        --attempt '${params.attempt}' \
        ${testArg} \
        --schema-dir '${params.schema_dir}'
    """

    stub:
    """
    mkdir -p 'validate_case_inputs_${task_id}'
    printf '{"schema_version":"2.0","status":"STUB","task_id":"${task_id}"}\n' > 'validate_case_inputs_${task_id}/stage_result.json'
    """
}
