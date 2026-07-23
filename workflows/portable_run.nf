include { RUN_PORTABLE_CASE } from '../modules/local/run_portable_case'
include { VALIDATE_RUNTIME_IDENTITY } from '../modules/local/validate_runtime_identity'


workflow PORTABLE_RUN {

    main:
    def required = ['canonical_tasks', 'run_output', 'session_id', 'runtime_manifest']
    def missing = required.findAll { key -> params[key] == null || params[key].toString().trim() == '' }
    if (missing) {
        error("PORTABLE_RUN missing required parameters: ${missing.join(', ')}")
    }
    if (params.fake_runtime && !params.test_mode) {
        error('fake_runtime is permitted only when test_mode is enabled')
    }
    def manifestSha = params.runtime_manifest_sha256?.toString()?.trim() ?: ''
    def fingerprintSha = params.runtime_fingerprint_sha256?.toString()?.trim() ?: ''
    def ociDigest = params.observed_oci_digest?.toString()?.trim() ?: ''
    def runtimeImage = params.runtime_image?.toString()?.trim() ?: ''
    def executionMode = params.runtime_execution_mode?.toString()?.trim() ?: ''
    def hostUid = params.host_uid?.toString()?.trim() ?: ''
    def hostGid = params.host_gid?.toString()?.trim() ?: ''
    def runtimeSif = params.runtime_sif?.toString()?.trim() ?: ''
    def runtimeSifSha = params.runtime_sif_sha256?.toString()?.trim() ?: ''
    if (!(executionMode in ['standalone', 'docker', 'scc', 'test'])) {
        error('PORTABLE_RUN requires an explicit standalone, docker, scc, or test profile')
    }
    if (executionMode == 'docker') {
        if (!(hostUid ==~ /[1-9][0-9]{0,9}/) || !(hostGid ==~ /[1-9][0-9]{0,9}/)) {
            error('docker profile requires canonical non-root host_uid and host_gid values')
        }
        if (hostUid.toLong() > 2147483647L || hostGid.toLong() > 2147483647L) {
            error('docker host_uid/host_gid exceed the supported POSIX ID limit')
        }
    } else if (hostUid || hostGid) {
        error('host_uid and host_gid are valid only with the docker profile')
    }
    if (!(manifestSha ==~ /[a-f0-9]{64}/)) {
        error('runtime_manifest_sha256 must be a lowercase SHA-256')
    }
    if (!(fingerprintSha ==~ /[a-f0-9]{64}/)) {
        error('runtime_fingerprint_sha256 must be a lowercase SHA-256')
    }
    if (ociDigest && !(ociDigest ==~ /sha256:[a-f0-9]{64}/)) {
        error('runtime_oci_digest must be sha256:<64 lowercase hex>')
    }
    if (runtimeSifSha && !(runtimeSifSha ==~ /[a-f0-9]{64}/)) {
        error('runtime_sif_sha256 must be a lowercase SHA-256')
    }
    if ((runtimeSif && !runtimeSifSha) || (!runtimeSif && runtimeSifSha)) {
        error('runtime_sif and runtime_sif_sha256 must be supplied together')
    }
    if (runtimeImage && runtimeImage != "ghcr.io/luckyfruit88/igv-pipeline@${ociDigest}") {
        error('advanced runtime_image must match its automatically observed OCI digest')
    }

    def taskManifest = file(params.canonical_tasks, checkIfExists: true)
    def workerScript = file("${projectDir}/src/ssqtl_igv/v3_worker.py", checkIfExists: true)
    def runtimeConfig = file("${projectDir}/src/ssqtl_igv/resources/v3-runtime.yaml", checkIfExists: true)
    def runtimeManifest = file(params.runtime_manifest, checkIfExists: true)
    def manifestSchema = file("${projectDir}/containers/runtime-manifest.schema.json", checkIfExists: true)
    def runtimeMaterials = file("${projectDir}/containers/runtime-materials.lock.json", checkIfExists: true)
    def explicitLocks = [
        file("${projectDir}/containers/fonts-local.conf", checkIfExists: true),
        file("${projectDir}/containers/runtime-system-packages.lock", checkIfExists: true),
        file("${projectDir}/containers/helper-linux-64.lock", checkIfExists: true),
        file("${projectDir}/containers/samtools-linux-64.lock", checkIfExists: true),
        file("${projectDir}/containers/materials.lock.json", checkIfExists: true),
    ]
    def identityValidator = file("${projectDir}/bin/validate_runtime_identity.py", checkIfExists: true)
    def schemaDirectory = file(params.schema_dir, checkIfExists: true)

    VALIDATE_RUNTIME_IDENTITY(
        runtimeManifest,
        manifestSchema,
        runtimeMaterials,
        explicitLocks,
        runtimeConfig,
        identityValidator,
        manifestSha,
        fingerprintSha,
        ociDigest,
        runtimeSifSha,
    )
    runtimeValidation = VALIDATE_RUNTIME_IDENTITY.out.bundle.first()

    tasks = channel.fromPath(params.canonical_tasks, checkIfExists: true)
        .splitText()
        .filter { line -> line.trim() }
        .map { line ->
            def task = new groovy.json.JsonSlurper().parseText(line)
            def stagedInputs = []
            if (task.core.preflight.state == 'READY') {
                task.core.tracks.each { track ->
                    stagedInputs << file(track.bam.source_path.toString(), checkIfExists: true)
                    stagedInputs << file(track.bai.source_path.toString(), checkIfExists: true)
                }
                task.core.reference.resources.each { _role, resource ->
                    stagedInputs << file(resource.source_path.toString(), checkIfExists: true)
                }
                if (task.core.auxiliary.state == 'PRESENT') {
                    stagedInputs << file(task.core.auxiliary.source_path.toString(), checkIfExists: true)
                }
            }
            tuple(task.task_id.toString(), task, stagedInputs)
        }

    RUN_PORTABLE_CASE(
        tasks,
        taskManifest,
        workerScript,
        runtimeConfig,
        runtimeManifest,
        fingerprintSha,
        runtimeValidation,
        schemaDirectory,
    )

    emit:
    case_bundles = RUN_PORTABLE_CASE.out.case_bundle
    runtime_manifest_validation = runtimeValidation
}
