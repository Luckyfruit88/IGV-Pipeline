include { VALIDATE_ENVIRONMENT } from '../modules/local/validate_environment'
include { AGGREGATE_SHARDS } from '../modules/local/aggregate_shards'
include { COLLECT_QACCT } from '../modules/local/collect_qacct'
include { BUILD_REVIEW_PACKAGE } from '../modules/local/build_review_package'


workflow AGGREGATE_RUN {

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
        'canonical_tasks',
        'shard_plan',
        'shard_summaries',
        'compose_bundles',
        'qc_bundles',
        'trace_files',
        'aggregate_output',
        'review_output',
    ]
    def missing = required.findAll { key -> params[key] == null || params[key].toString().trim() == '' }
    if (missing) {
        error("AGGREGATE_RUN missing required parameters: ${missing.join(', ')}")
    }

    def canonicalTasks = file(params.canonical_tasks, checkIfExists: true)
    def shardPlan = file(params.shard_plan, checkIfExists: true)
    def gateScript = file("${projectDir}/bin/assert_gate.py", checkIfExists: true)
    def environmentScript = file("${projectDir}/bin/validate_environment.py", checkIfExists: true)
    def aggregateScript = file("${projectDir}/bin/aggregate_run.py", checkIfExists: true)
    def accountingScript = file("${projectDir}/bin/collect_qacct.py", checkIfExists: true)
    def reviewScript = file("${projectDir}/bin/build_review_package.py", checkIfExists: true)
    shardSummaries = channel.fromPath(params.shard_summaries, checkIfExists: true, type: 'dir').collect()
    composeBundles = channel.fromPath(params.compose_bundles, checkIfExists: true, type: 'dir').collect()
    qcBundles = channel.fromPath(params.qc_bundles, checkIfExists: true, type: 'dir').collect()
    traceFiles = channel.fromPath(params.trace_files, checkIfExists: true, type: 'file').collect()

    def accountingCommands = params.skip_qacct ? [] : ['qacct']
    VALIDATE_ENVIRONMENT('aggregate_run', accountingCommands, environmentScript)
    AGGREGATE_SHARDS(
        VALIDATE_ENVIRONMENT.out.bundle,
        gateScript,
        aggregateScript,
        canonicalTasks,
        shardPlan,
        shardSummaries,
    )
    COLLECT_QACCT(
        VALIDATE_ENVIRONMENT.out.bundle,
        gateScript,
        accountingScript,
        traceFiles,
    )
    BUILD_REVIEW_PACKAGE(
        gateScript,
        reviewScript,
        canonicalTasks,
        AGGREGATE_SHARDS.out.bundle,
        COLLECT_QACCT.out.bundle,
        composeBundles,
        qcBundles,
    )

    emit:
    environment = VALIDATE_ENVIRONMENT.out.bundle
    aggregate = AGGREGATE_SHARDS.out.bundle
    accounting = COLLECT_QACCT.out.bundle
    review_package = BUILD_REVIEW_PACKAGE.out.bundle
}
