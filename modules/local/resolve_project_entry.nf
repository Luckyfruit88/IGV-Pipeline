def encodeProjectEntryValue(value) {
    return java.util.Base64
        .getUrlEncoder()
        .withoutPadding()
        .encodeToString((value ?: '').toString().getBytes('UTF-8'))
}


process RESOLVE_PROJECT_ENTRY {
    tag 'project-entry'
    label 'portable_runtime'
    label 'control'
    cache false

    input:
    path helper_script, stageAs: 'software/project_admission_v3.py'
    path runtime_manifest, stageAs: 'contract/runtime-manifest.json'
    val project_path
    val batch_request_path
    val requested_run_id
    val requested_generation_id
    val execution_mode

    output:
    path 'entry_source', emit: bundle

    script:
    def projectB64 = project_path ? encodeProjectEntryValue(project_path) : ''
    def batchB64 = batch_request_path ? encodeProjectEntryValue(batch_request_path) : ''
    def projectArg = project_path ? "--project-b64 '${projectB64}'" : ''
    def batchArg = batch_request_path ? "--batch-request-b64 '${batchB64}'" : ''
    def runArg = requested_run_id ? "--run-id '${requested_run_id}'" : ''
    def generationArg = requested_generation_id ? "--generation-id '${requested_generation_id}'" : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export LC_ALL=C.UTF-8
    export LANG=C.UTF-8
    cp -L '${runtime_manifest}' runtime-manifest.input.json
    '${params.python}' '${helper_script}' resolve \
        ${projectArg} \
        ${batchArg} \
        --runtime-manifest runtime-manifest.input.json \
        --output-dir entry_source \
        ${runArg} \
        ${generationArg} \
        --profile '${execution_mode}'
    """

    stub:
    """
    mkdir -p entry_source/normalization
    printf '{"schema_version":"3.0-project-entry-source","entry_kind":"project","adapter":"generic","normalization_required":false,"run_id":"stub-run","generation_id":"generation-001","profile":"test","runtime_manifest_sha256":"%064d","runtime_fingerprint_sha256":"%064d","descriptor_sha256":"%064d"}\n' 0 0 0 > entry_source/descriptor.json
    printf '{"schema_version":"3.0","pipeline_version":"3.0.0","run_id":"stub-run","generation_id":"generation-001","task_id":"stub-case","manifest_order":1,"adapter_id":"generic","core":{"preflight":{"state":"CASE_INPUT_INVALID"}}}\n' > entry_source/normalization/tasks.jsonl
    printf '{"schema_version":"3.0","status":"STUB","adapter_id":"generic","run_id":"stub-run","generation_id":"generation-001","task_count":1}\n' > entry_source/normalization/validation.json
    printf '{"schema_version":"3.0","adapter_id":"generic"}\n' > entry_source/normalization/parameters.json
    cp -L '${runtime_manifest}' entry_source/runtime_manifest.snapshot.json
    """
}
