include { VALIDATE_ENVIRONMENT } from '../modules/local/validate_environment'
include { VALIDATE_REVIEW } from '../modules/local/validate_review'
include { PUBLISH_REVIEWED } from '../modules/local/publish_reviewed'


workflow PUBLISH_RUN {

    main:
    def pipelineCommit = params.pipeline_commit?.toString()
    if (!params.test_mode && (!(pipelineCommit ==~ /[a-f0-9]{40}/) || pipelineCommit == '0' * 40)) {
        error('production execution requires a real 40-character pipeline_commit')
    }
    if (params.require_helper_sif) {
        def helperSif = params.helper_sif?.toString()?.trim()
        def helperSha = params.helper_sif_sha256?.toString()?.trim()
        if (!helperSif || !(helperSha ==~ /[a-f0-9]{64}/)) {
            error('this profile requires helper_sif and its lowercase SHA-256 identity')
        }
    }
    def required = [
        'review_package',
        'review_records',
        'aggregate_case_results',
        'validated_review_output',
        'publication_destination',
    ]
    def missing = required.findAll { key -> params[key] == null || params[key].toString().trim() == '' }
    if (missing) {
        error("PUBLISH_RUN missing required parameters: ${missing.join(', ')}")
    }

    def reviewPackage = file(params.review_package, checkIfExists: true)
    def reviewRecords = file(params.review_records, checkIfExists: true)
    def aggregateCaseResults = file(params.aggregate_case_results, checkIfExists: true)
    def gateScript = file("${projectDir}/bin/assert_gate.py", checkIfExists: true)
    def environmentScript = file("${projectDir}/bin/validate_environment.py", checkIfExists: true)
    def validateReviewScript = file("${projectDir}/bin/validate_review.py", checkIfExists: true)
    def publishScript = file("${projectDir}/bin/publish_reviewed.py", checkIfExists: true)

    VALIDATE_ENVIRONMENT('publish_run', [], environmentScript)
    VALIDATE_REVIEW(
        VALIDATE_ENVIRONMENT.out.bundle,
        gateScript,
        validateReviewScript,
        reviewPackage,
        reviewRecords,
        aggregateCaseResults,
    )
    PUBLISH_REVIEWED(publishScript, reviewPackage, VALIDATE_REVIEW.out.bundle)

    emit:
    environment = VALIDATE_ENVIRONMENT.out.bundle
    validated_review = VALIDATE_REVIEW.out.bundle
    publication_receipt = PUBLISH_REVIEWED.out.receipt
}
