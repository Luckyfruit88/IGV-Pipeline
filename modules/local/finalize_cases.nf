process FINALIZE_CASES {
    tag 'direct-output'
    label 'portable_runtime'
    label 'aggregate'
    cache 'deep'

    publishDir "${params.output}", mode: 'copy', overwrite: false

    input:
    path admission_bundle, stageAs: 'source/admission'
    path case_bundles, stageAs: 'source/case_bundles/??/*'
    path helper_script, stageAs: 'software/project_admission_v3.py'
    val allow_debug_only

    output:
    path 'results', emit: results
    path 'contract', emit: contract
    path 'shards', emit: shards
    path 'snapshots.tsv', emit: snapshots
    path 'failed_cases.tsv', emit: failures
    path 'run_summary.json', emit: summary

    script:
    def debugArg = allow_debug_only ? '--allow-debug-only' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export LC_ALL=C.UTF-8
    export LANG=C.UTF-8
    '${params.python}' '${helper_script}' finalize \
        --admission-bundle '${admission_bundle}' \
        --case-bundle-root source/case_bundles \
        --output-dir finalized \
        ${debugArg}
    mv finalized/results results
    mv finalized/contract contract
    mv finalized/shards shards
    mv finalized/snapshots.tsv snapshots.tsv
    mv finalized/failed_cases.tsv failed_cases.tsv
    mv finalized/run_summary.json run_summary.json
    """

    stub:
    """
    mkdir -p results/cases contract shards
    cp -L '${admission_bundle}/contract/tasks.jsonl' contract/tasks.jsonl
    cp -L '${admission_bundle}/contract/run_identity.json' contract/run_identity.json
    cp -L '${admission_bundle}/contract/execution_policy.json' contract/execution_policy.json
    cp -L '${admission_bundle}/shards/shard_plan.json' shards/shard_plan.json
    printf 'manifest_order\ttask_id\tstatus\tadapter_type\tscientific_interpretation\treview_png\treview_sha256\traw_igv_png\traw_igv_sha256\tcase_result_json\tinput_fingerprint\n' > snapshots.tsv
    printf 'manifest_order\ttask_id\tfailure_code\tmessage\tcase_result_json\tinput_fingerprint\n' > failed_cases.tsv
    printf '{"schema_version":"3.0","pipeline_version":"3.0.0","authoritative":false,"projection_kind":"UX_ONLY","status":"STUB","exit_code":0,"expected_case_count":0,"observed_case_count":0,"failed_case_count":0}\n' > run_summary.json
    """
}
