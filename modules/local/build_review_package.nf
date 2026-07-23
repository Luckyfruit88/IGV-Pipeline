process BUILD_REVIEW_PACKAGE {
    tag "${params.run_id}:${params.generation_id}"
    label 'helper'
    label 'aggregate'

    publishDir "${params.review_output}", mode: 'copy', overwrite: false

    input:
    path gate_script, stageAs: 'software/assert_gate.py'
    path helper_script, stageAs: 'software/build_review_package.py'
    path canonical_tasks, stageAs: 'contract/tasks.jsonl'
    path aggregate_bundle
    path accounting_bundle
    path compose_bundles, stageAs: 'compose_bundles/??/*'
    path qc_bundles, stageAs: 'qc_bundles/??/*'

    output:
    path 'review_package', emit: bundle

    script:
    def composeList = compose_bundles instanceof java.util.Collection ? compose_bundles : [compose_bundles]
    def qcList = qc_bundles instanceof java.util.Collection ? qc_bundles : [qc_bundles]
    def composeArgs = composeList.collect { item -> "--compose-bundle '${item}'" }.join(' ')
    def qcArgs = qcList.collect { item -> "--qc-bundle '${item}'" }.join(' ')
    def testArg = params.test_mode ? '--allow-test' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${gate_script}' --kind accounting --bundle '${accounting_bundle}' ${testArg}
    cp -L '${canonical_tasks}' canonical_tasks.input.jsonl
    '${params.python}' '${helper_script}' \
        --canonical-tasks canonical_tasks.input.jsonl \
        --case-results '${aggregate_bundle}/case_results.jsonl' \
        ${composeArgs} \
        ${qcArgs} \
        --output-dir review_package
    """

    stub:
    """
    mkdir -p review_package
    : > review_package/review_contract.jsonl
    : > review_package/SHA256SUMS
    printf '{"schema_version":"2.0-review-package","status":"STUB"}\n' > review_package/package.json
    """
}
