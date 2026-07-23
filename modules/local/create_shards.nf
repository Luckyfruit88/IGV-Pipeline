process CREATE_SHARDS {
    tag "${params.run_id}:${params.generation_id}"
    label 'helper'
    label 'control'

    publishDir "${params.plan_output}", mode: 'copy', overwrite: false

    input:
    path normalization_bundle
    path helper_script, stageAs: 'software/create_shards.py'

    output:
    path 'shard_bundle', emit: bundle

    script:
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${helper_script}' \
        --tasks '${normalization_bundle}/tasks.jsonl' \
        --output-dir shard_bundle \
        --max-cases-per-shard '${params.max_cases_per_shard}' \
        --score-budget-seconds '${params.shard_score_budget}'
    """

    stub:
    """
    mkdir -p shard_bundle/shards
    printf '{"schema_version":"2.0","status":"STUB","shards":[]}\n' > shard_bundle/shard_plan.json
    printf 'shard_id\trelative_path\ttask_count\n' > shard_bundle/shard_plan.tsv
    """
}
