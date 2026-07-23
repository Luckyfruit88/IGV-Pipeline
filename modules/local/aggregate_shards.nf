process AGGREGATE_SHARDS {
    tag "${params.run_id}:${params.generation_id}"
    label 'helper'
    label 'aggregate'

    publishDir "${params.aggregate_output}", mode: 'copy', overwrite: false

    input:
    path environment_bundle
    path gate_script, stageAs: 'software/assert_gate.py'
    path helper_script, stageAs: 'software/aggregate_run.py'
    path canonical_tasks, stageAs: 'contract/tasks.jsonl'
    path shard_plan, stageAs: 'contract/shard_plan.json'
    path shard_summaries, stageAs: 'shard_summaries/??/*'

    output:
    path 'aggregate_bundle', emit: bundle

    script:
    def summaryList = shard_summaries instanceof java.util.Collection ? shard_summaries : [shard_summaries]
    def summaryArgs = summaryList.collect { item -> "--shard-summary '${item}'" }.join(' ')
    def testArg = params.test_mode ? '--allow-test' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${gate_script}' --kind environment --phase aggregate_run --bundle '${environment_bundle}' ${testArg}
    cp -L '${canonical_tasks}' canonical_tasks.input.jsonl
    '${params.python}' '${helper_script}' \
        --canonical-tasks canonical_tasks.input.jsonl \
        --shard-plan '${shard_plan}' \
        ${summaryArgs} \
        --output-dir aggregate_bundle \
        --schema-dir '${params.schema_dir}'
    """

    stub:
    """
    mkdir -p aggregate_bundle
    : > aggregate_bundle/case_results.jsonl
    : > aggregate_bundle/run_ledger.jsonl
    printf '{"schema_version":"2.0","status":"STUB"}\n' > aggregate_bundle/run_summary.json
    printf '{"schema_version":"2.0","status":"STUB"}\n' > aggregate_bundle/reconciliation.json
    """
}
