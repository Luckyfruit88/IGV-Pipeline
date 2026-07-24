def portableRenderAttempt(policyDocument, attemptNumber) {
    def attempts = new groovy.json.JsonSlurperClassic()
        .parseText(policyDocument.toString())
        .render.attempts
    def index = Math.min(Math.max(attemptNumber as int, 1), attempts.size()) - 1
    return attempts[index]
}


process RUN_PORTABLE_CASE {
    tag "${task_id}"
    label 'portable_runtime'
    label 'portable_render'
    cache true
    cpus { portableRenderAttempt(execution_policy_doc, task.attempt).cpus as int }
    memory {
        "${portableRenderAttempt(execution_policy_doc, task.attempt).memory_bytes} B"
    }
    time {
        "${portableRenderAttempt(execution_policy_doc, task.attempt).timeout_seconds} sec"
    }
    errorStrategy {
        task.exitStatus in [75, 137, 143] && task.attempt <= 2
            ? 'retry'
            : 'terminate'
    }
    maxRetries 2

    publishDir "${params.session_output}/case_outputs",
        mode: 'copy',
        overwrite: false,
        enabled: params.publish_intermediate_case_outputs

    input:
    tuple val(task_id), val(task_doc_json), path(staged_inputs, stageAs: 'inputs/??/resource')
    path worker_source, stageAs: 'software/v3_worker.py'
    path runtime_config, stageAs: 'contract/v3-runtime.yaml'
    path runtime_manifest, stageAs: 'contract/runtime-manifest.json'
    val runtime_fingerprint_sha256
    path runtime_validation, stageAs: 'contract/runtime-manifest-validation'
    val execution_policy_doc
    path schema_directory, stageAs: 'schema'

    output:
    tuple val(task_id), path("${task_id}"), emit: case_bundle

    script:
    def task_doc = new groovy.json.JsonSlurperClassic().parseText(task_doc_json)
    def attemptPolicy = portableRenderAttempt(execution_policy_doc, task.attempt)
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
    def stagedArgs = [resourceNames, stagedList]
        .transpose()
        .collect { pair ->
            def payload = "${pair[0]}=${pair[1]}".getBytes('UTF-8')
            def encoded = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(payload)
            "--staged-input-b64 ${encoded}"
        }
        .join(' ')
    def fakeArg = params.fake_runtime ? '--fake-runtime' : ''
    def taskPayload = java.util.Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(task_doc_json.getBytes('UTF-8'))
    """
    export PYTHONDONTWRITEBYTECODE=1
    export LC_ALL=C.UTF-8
    export LANG=C.UTF-8
    export IGV_RUNTIME_FINGERPRINT_SHA256='${runtime_fingerprint_sha256}'
    export IGV_HEAP='${attemptPolicy.igv_heap_argument}'
    '${params.python}' -m ssqtl_igv.v3_worker \
        --task-json-b64 '${taskPayload}' \
        --task-id '${task_id}' \
        ${stagedArgs} \
        --output-dir '${task_id}' \
        --runtime-config '${runtime_config}' \
        --schema-dir '${schema_directory}' \
        --attempt '${task.attempt}' \
        --max-attempts '3' \
        ${fakeArg}
    """

    stub:
    """
    mkdir -p '${task_id}'
    printf '{"schema_version":"3.0","task_id":"${task_id}","status":"STUB"}\n' > '${task_id}/terminal_bundle.json'
    printf '{"schema_version":"3.0","task_id":"${task_id}","eligible":false,"artifact_review_state":"REVIEW_PENDING","publication_state":"NOT_READY"}\n' > '${task_id}/case_result.json'
    """
}
