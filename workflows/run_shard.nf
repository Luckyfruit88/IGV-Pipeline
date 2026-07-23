include { VALIDATE_ENVIRONMENT } from '../modules/local/validate_environment'
include { VALIDATE_CASE_INPUTS } from '../modules/local/validate_case_inputs'
include { RUN_IGV } from '../modules/local/run_igv'
include { COMPOSE_CASE } from '../modules/local/compose_case'
include { QC_CASE } from '../modules/local/qc_case'
include { SUMMARIZE_SHARD } from '../modules/local/summarize_shard'


workflow RUN_SHARD {

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
    def required = ['params_file', 'shard_manifest', 'shard_id', 'session_id', 'run_output']
    def missing = required.findAll { key -> params[key] == null || params[key].toString().trim() == '' }
    if (missing) {
        error("RUN_SHARD missing required parameters: ${missing.join(', ')}")
    }
    if (params.fake_runtime && !params.test_mode) {
        error('fake_runtime is permitted only when test_mode is enabled')
    }

    def shardManifest = file(params.shard_manifest, checkIfExists: true)
    def paramsFile = file(params.params_file, checkIfExists: true)
    def referenceRoles = ['definition', 'fasta', 'fai', 'cytoband', 'annotation']
    def gateScript = file("${projectDir}/bin/assert_gate.py", checkIfExists: true)
    def environmentScript = file("${projectDir}/bin/validate_environment.py", checkIfExists: true)
    def validateScript = file("${projectDir}/bin/validate_case_inputs.py", checkIfExists: true)
    def renderScript = file("${projectDir}/bin/run_igv_case.py", checkIfExists: true)
    def composeScript = file("${projectDir}/bin/compose_case.py", checkIfExists: true)
    def qcScript = file("${projectDir}/bin/qc_case.py", checkIfExists: true)
    def summarizeScript = file("${projectDir}/bin/summarize_shard.py", checkIfExists: true)

    tasks = channel.fromPath(params.shard_manifest, checkIfExists: true)
        .splitText()
        .filter { line -> line.trim() }
        .map { line ->
            def task = new groovy.json.JsonSlurper().parseText(line)
            def meta = [
                task_id: task.task_id.toString(),
                manifest_order: task.manifest_order as Integer,
                input_fingerprint: task.input_fingerprint.toString(),
                preflight_state: task.preflight_state.toString(),
            ]
            def stagedInputs = []
            if (task.preflight_state == 'CASE_INPUT_INVALID') {
                stagedInputs << shardManifest
            }
            else {
                task.tracks.each { track ->
                    stagedInputs << file(track.bam.toString(), checkIfExists: true)
                    stagedInputs << file(track.bai.toString(), checkIfExists: true)
                }
                referenceRoles.each { role ->
                    stagedInputs << file(
                        task.reference.resources[role].source_path.toString(),
                        checkIfExists: true
                    )
                }
                if (task.plot.state == 'PRESENT') {
                    stagedInputs << file(task.plot.pdf.toString(), checkIfExists: true)
                }
            }
            def violinPdf = task.plot.state == 'PRESENT'
                ? file(task.plot.pdf.toString(), checkIfExists: true)
                : shardManifest
            tuple(task.task_id.toString(), meta, stagedInputs, violinPdf)
        }

    caseInputs = tasks.map { task_id, meta, staged_inputs, _violin_pdf ->
        tuple(task_id, meta, staged_inputs)
    }
    plotInputs = tasks.map { task_id, meta, _staged_inputs, violin_pdf ->
        tuple(task_id, meta, violin_pdf)
    }

    def runtimeCommands = params.fake_runtime ? [] : ['java', 'samtools', 'igv']
    VALIDATE_ENVIRONMENT('run_shard', runtimeCommands, environmentScript)
    VALIDATE_CASE_INPUTS(
        caseInputs,
        VALIDATE_ENVIRONMENT.out.bundle,
        gateScript,
        validateScript,
        shardManifest,
        params.shard_id,
        params.session_id,
    )

    renderInputs = caseInputs
        .join(
            VALIDATE_CASE_INPUTS.out.bundle,
            by: 0,
            failOnDuplicate: true,
            failOnMismatch: true,
        )
        .map { task_id, source_meta, staged_inputs, result_meta, validation_bundle ->
            if (source_meta != result_meta) {
                error("VALIDATE_CASE_INPUTS metadata drift: ${task_id}")
            }
            tuple(task_id, source_meta, staged_inputs, validation_bundle)
        }
    RUN_IGV(
        renderInputs,
        renderScript,
        shardManifest,
        paramsFile,
        params.shard_id,
        params.session_id,
    )

    composeInputs = plotInputs
        .join(
            RUN_IGV.out.bundle,
            by: 0,
            failOnDuplicate: true,
            failOnMismatch: true,
        )
        .map { task_id, source_meta, violin_pdf, result_meta, render_bundle ->
            if (source_meta != result_meta) {
                error("RUN_IGV metadata drift: ${task_id}")
            }
            tuple(task_id, source_meta, violin_pdf, render_bundle)
        }
    COMPOSE_CASE(
        composeInputs,
        composeScript,
        shardManifest,
        paramsFile,
        params.shard_id,
        params.session_id,
    )

    qcInputs = VALIDATE_CASE_INPUTS.out.bundle
        .join(
            RUN_IGV.out.bundle,
            by: 0,
            failOnDuplicate: true,
            failOnMismatch: true,
        )
        .join(
            COMPOSE_CASE.out.bundle,
            by: 0,
            failOnDuplicate: true,
            failOnMismatch: true,
        )
        .map { task_id, validation_meta, validation_bundle, render_meta, render_bundle, compose_meta, compose_bundle ->
            if (validation_meta != render_meta || validation_meta != compose_meta) {
                error("case-stage metadata drift: ${task_id}")
            }
            tuple(
                task_id,
                validation_meta,
                validation_bundle,
                render_bundle,
                compose_bundle,
            )
        }
    QC_CASE(
        qcInputs,
        qcScript,
        shardManifest,
        params.shard_id,
        params.session_id,
    )

    qcBundles = QC_CASE.out.bundle
        .map { _task_id, _meta, bundle -> bundle }
        .collect()
    SUMMARIZE_SHARD(
        shardManifest,
        qcBundles,
        summarizeScript,
        params.shard_id,
        params.session_id,
    )

    emit:
    environment = VALIDATE_ENVIRONMENT.out.bundle
    validation_bundles = VALIDATE_CASE_INPUTS.out.bundle
    render_bundles = RUN_IGV.out.bundle
    compose_bundles = COMPOSE_CASE.out.bundle
    qc_bundles = QC_CASE.out.bundle
    shard_summary = SUMMARIZE_SHARD.out.bundle
}
