process VALIDATE_REVIEW {
    tag "${params.run_id}:${params.generation_id}"
    label 'helper'
    label 'aggregate'

    publishDir "${params.validated_review_output}", mode: 'copy', overwrite: false

    input:
    path environment_bundle
    path gate_script, stageAs: 'software/assert_gate.py'
    path helper_script, stageAs: 'software/validate_review.py'
    path review_package
    path review_records, stageAs: 'contract/reviews.jsonl'
    path aggregate_case_results, stageAs: 'contract/case_results.jsonl'

    output:
    path 'validated_review_bundle', emit: bundle

    script:
    def testArg = params.test_mode ? '--allow-test' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${gate_script}' --kind environment --phase publish_run --bundle '${environment_bundle}' ${testArg}
    cp -L '${review_records}' reviews.input.jsonl
    cp -L '${aggregate_case_results}' case_results.input.jsonl
    '${params.python}' '${helper_script}' \
        --review-contract '${review_package}/review_contract.jsonl' \
        --reviews reviews.input.jsonl \
        --case-results case_results.input.jsonl \
        --output-dir validated_review_bundle \
        --schema-dir '${params.schema_dir}'
    """

    stub:
    """
    mkdir -p validated_review_bundle
    : > validated_review_bundle/accepted_reviews.jsonl
    : > validated_review_bundle/reviewed_case_results.jsonl
    printf '{"schema_version":"2.0-review-validation","status":"STUB"}\n' > validated_review_bundle/review_validation.json
    """
}
