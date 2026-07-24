process ADMIT_PROJECT_TASKS {
    tag 'canonical-task-admission'
    label 'portable_runtime'
    label 'control'
    cache 'deep'

    input:
    tuple path(entry_source, stageAs: 'source/entry'), path(normalization_bundle, stageAs: 'source/normalization')
    path execution_policy, stageAs: 'contract/execution_policy.json'
    path helper_script, stageAs: 'software/project_admission_v3.py'
    val max_cases_per_shard

    output:
    path 'admission_bundle', emit: bundle

    script:
    """
    export PYTHONDONTWRITEBYTECODE=1
    export LC_ALL=C.UTF-8
    export LANG=C.UTF-8
    '${params.python}' '${helper_script}' admit \
        --entry-source '${entry_source}' \
        --normalization-bundle '${normalization_bundle}' \
        --execution-policy '${execution_policy}' \
        --output-dir admission_bundle \
        --max-cases-per-shard '${max_cases_per_shard}' \
        --allow-staged-symlink
    """

    stub:
    """
    mkdir -p admission_bundle/contract admission_bundle/shards
    cp -L '${normalization_bundle}/tasks.jsonl' admission_bundle/contract/tasks.jsonl
    cp -L '${execution_policy}' admission_bundle/contract/execution_policy.json
    printf '{"schema_version":"3.0","pipeline_version":"3.0.0","run_id":"stub-run","generation_id":"generation-001","profile":"test","adapter":"generic","runtime_fingerprint_sha256":"%064d"}\n' 0 > admission_bundle/contract/run_identity.json
    printf '{"schema_version":"3.0","case_count":0,"max_cases_per_shard":%s,"scheduling_role":"LOGICAL_ONLY","shard_count":0,"shards":[]}\n' '${max_cases_per_shard}' > admission_bundle/shards/shard_plan.json
    printf '{"schema_version":"3.0-project-task-admission","status":"STUB","task_count":0}\n' > admission_bundle/admission.json
    """
}
