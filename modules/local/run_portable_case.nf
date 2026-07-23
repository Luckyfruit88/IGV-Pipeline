process RUN_PORTABLE_CASE {
    tag "${task_id}"
    label 'portable_runtime'
    label 'portable_render'
    cache 'deep'

    publishDir "${params.session_output}/case_outputs", mode: 'copy', overwrite: false

    input:
    tuple val(task_id), val(task_doc), path(staged_inputs, stageAs: 'inputs/??/resource')
    path task_manifest, stageAs: 'contract/tasks.jsonl'
    path worker_source, stageAs: 'software/v3_worker.py'
    path runtime_config, stageAs: 'contract/v3-runtime.yaml'
    path runtime_manifest, stageAs: 'contract/runtime-manifest.json'
    val runtime_fingerprint_sha256
    path runtime_validation, stageAs: 'contract/runtime-manifest-validation'
    path schema_directory, stageAs: 'schema'

    output:
    tuple val(task_id), path("${task_id}"), emit: case_bundle

    script:
    def stagedList = staged_inputs instanceof java.util.Collection ? staged_inputs : [staged_inputs]
    def resourceNames = []
    if (task_doc.core.preflight.state == 'READY') {
        task_doc.core.tracks.each { track ->
            resourceNames << track.bam.stage_name.toString()
            resourceNames << track.bai.stage_name.toString()
        }
        task_doc.core.reference.resources.each { _role, resource ->
            resourceNames << resource.stage_name.toString()
        }
        if (task_doc.core.auxiliary.state == 'PRESENT') {
            resourceNames << task_doc.core.auxiliary.stage_name.toString()
        }
    }
    if (resourceNames.size() != stagedList.size()) {
        error("staged path count differs from canonical resources for ${task_id}")
    }
    def stagedArgs = [resourceNames, stagedList].transpose().collect { pair ->
        def payload = "${pair[0]}=${pair[1]}".getBytes('UTF-8')
        def encoded = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(payload)
        "--staged-input-b64 ${encoded}"
    }.join(' ')
    def fakeArg = params.fake_runtime ? '--fake-runtime' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export LC_ALL=C.UTF-8
    export LANG=C.UTF-8
    export IGV_RUNTIME_FINGERPRINT_SHA256='${runtime_fingerprint_sha256}'
    '${params.python}' -m ssqtl_igv.v3_worker \
        --task-manifest '${task_manifest}' \
        --task-id '${task_id}' \
        ${stagedArgs} \
        --output-dir '${task_id}' \
        --runtime-config '${runtime_config}' \
        --schema-dir '${schema_directory}' \
        ${fakeArg}
    """

    stub:
    """
    mkdir -p '${task_id}'
    printf '{"schema_version":"3.0","task_id":"${task_id}","status":"STUB"}\n' > '${task_id}/terminal_bundle.json'
    printf '{"schema_version":"3.0","task_id":"${task_id}","eligible":false,"artifact_review_state":"REVIEW_PENDING","publication_state":"NOT_READY"}\n' > '${task_id}/case_result.json'
    """
}
