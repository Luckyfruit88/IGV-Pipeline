process PUBLISH_REVIEWED {
    tag "${params.run_id}:${params.generation_id}"
    label 'helper'
    label 'aggregate'
    cache false

    publishDir "${params.validated_review_output}", mode: 'copy', overwrite: false

    input:
    path helper_script, stageAs: 'software/publish_reviewed.py'
    path review_package
    path validated_review_bundle

    output:
    path 'publication_result.json', emit: receipt

    script:
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${helper_script}' \
        --review-package '${review_package}' \
        --validated-reviews '${validated_review_bundle}' \
        --destination '${params.publication_destination}' \
        > publication_result.json
    """

    stub:
    """
    printf '{"schema_version":"2.0-publication","status":"STUB"}\n' > publication_result.json
    """
}
