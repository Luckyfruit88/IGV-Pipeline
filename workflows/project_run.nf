include { RESOLVE_PROJECT_ENTRY } from '../modules/local/resolve_project_entry'
include { RESOLVE_EXECUTION_POLICY } from '../modules/local/resolve_execution_policy'
include { VALIDATE_RUNTIME_IDENTITY } from '../modules/local/validate_runtime_identity'
include { NORMALIZE_SSQTL_V3 } from '../modules/local/normalize_ssqtl_v3'
include { ADMIT_PROJECT_TASKS } from '../modules/local/admit_project_tasks'
include { RUN_PORTABLE_CASE } from '../modules/local/run_portable_case'
include { FINALIZE_CASES } from '../modules/local/finalize_cases'


workflow PROJECT_RUN {

    main:
    def explicitProject = params.project?.toString()?.trim() ?: ''
    def batchRequest = params.batch_request?.toString()?.trim() ?: ''
    if (explicitProject && batchRequest) {
        error('PROJECT_RUN accepts --project or --batch_request, never both')
    }
    def projectPath = batchRequest ? '' : (explicitProject ?: '/project/project.yaml')
    def outputRoot = params.output?.toString()?.trim() ?: '/output'
    def shardLimit = params.max_cases_per_shard as Integer
    if (!(1..256).contains(shardLimit)) {
        error('max_cases_per_shard must be between 1 and 256')
    }
    if (params.fake_runtime && !params.test_mode) {
        error('fake_runtime is permitted only when test_mode is enabled')
    }
    def executionMode = params.runtime_execution_mode?.toString()?.trim()
    if (!executionMode || executionMode == 'unconfigured') {
        executionMode = 'standalone'
    }
    if (System.getenv('IGV_SCC_SLOTS')?.trim()) {
        executionMode = 'scc'
    }
    if (!(executionMode in ['standalone', 'docker', 'scc', 'test'])) {
        error('PROJECT_RUN requires standalone, docker, scc, or test execution mode')
    }

    // These are output projections only.  Nextflow trace/cache remains the
    // execution authority; FINALIZE_CASES deliberately never opens trace.txt.
    params.output = outputRoot
    params.session_output = "${outputRoot}/reports"

    def runtimeManifestPath = params.runtime_manifest?.toString()?.trim() ?: '/opt/igv-pipeline/runtime-manifest.json'
    def runtimeManifest = file(runtimeManifestPath, checkIfExists: true)
    def entryHelper = file(
        "${projectDir}/bin/project_admission_v3.py",
        checkIfExists: true
    )
    def policyResolver = file(
        "${projectDir}/bin/resolve_execution_policy.py",
        checkIfExists: true
    )

    RESOLVE_EXECUTION_POLICY(
        policyResolver,
        executionMode,
        params.max_parallel,
        params.igv_cpus,
        params.igv_memory,
        params.igv_timeout,
        params.normalization_cpus,
        params.normalization_memory,
        params.normalization_timeout,
    )
    executionPolicy = RESOLVE_EXECUTION_POLICY.out.policy
    executionPolicyDoc = executionPolicy
        .map { policyPath -> policyPath.text }

    RESOLVE_PROJECT_ENTRY(
        entryHelper,
        runtimeManifest,
        projectPath,
        batchRequest,
        params.run_id?.toString()?.trim() ?: '',
        params.generation_id?.toString()?.trim() ?: '',
        executionMode,
    )
    entryRecords = RESOLVE_PROJECT_ENTRY.out.bundle.map { bundle ->
        def descriptorPath = bundle.resolve('descriptor.json')
        def descriptor = new groovy.json.JsonSlurperClassic().parse(
            descriptorPath.toFile()
        )
        tuple(descriptor, bundle)
    }

    def manifestSchema = file(
        "${projectDir}/containers/runtime-manifest.schema.json",
        checkIfExists: true
    )
    def runtimeMaterials = file(
        "${projectDir}/containers/runtime-materials.lock.json",
        checkIfExists: true
    )
    def explicitLocks = [
        file("${projectDir}/containers/fonts-local.conf", checkIfExists: true),
        file(
            "${projectDir}/containers/runtime-system-packages.lock",
            checkIfExists: true
        ),
        file("${projectDir}/containers/helper-linux-64.lock", checkIfExists: true),
        file("${projectDir}/containers/samtools-linux-64.lock", checkIfExists: true),
        file("${projectDir}/containers/materials.lock.json", checkIfExists: true),
    ]
    def runtimeConfig = file(
        "${projectDir}/src/ssqtl_igv/resources/v3-runtime.yaml",
        checkIfExists: true
    )
    def identityValidator = file(
        "${projectDir}/bin/validate_runtime_identity.py",
        checkIfExists: true
    )
    def manifestSha = entryRecords.map { descriptor, _bundle ->
        descriptor.runtime_manifest_sha256.toString()
    }
    runtimeFingerprint = entryRecords
        .map { descriptor, _bundle ->
            descriptor.runtime_fingerprint_sha256.toString()
        }

    VALIDATE_RUNTIME_IDENTITY(
        runtimeManifest,
        manifestSchema,
        runtimeMaterials,
        explicitLocks,
        runtimeConfig,
        identityValidator,
        manifestSha,
        runtimeFingerprint,
        params.observed_oci_digest?.toString()?.trim() ?: '',
        params.runtime_sif_sha256?.toString()?.trim() ?: '',
    )
    runtimeValidation = VALIDATE_RUNTIME_IDENTITY.out.bundle
    // The validation report is an observation and intentionally contains
    // validated_at plus host paths.  Keep it as a user-facing report, but feed
    // only its stable assertions into downstream cache identities.
    runtimeValidationIdentity = runtimeValidation.map { bundle ->
        def validation = new groovy.json.JsonSlurperClassic().parse(
            bundle.resolve('validation.json').toFile()
        )
        if (validation.status != 'PASS') {
            error('runtime manifest validation did not pass')
        }
        groovy.json.JsonOutput.toJson([
            schema_version: validation.schema_version,
            status: validation.status,
            runtime_manifest_sha256: validation.runtime_manifest_sha256,
            runtime_fingerprint_sha256: validation.runtime_fingerprint_sha256,
            materials_sha256: validation.materials_sha256,
            runtime_config_sha256: validation.runtime_config_sha256,
            observed_provenance: validation.observed_provenance,
        ])
    }

    directEntries = entryRecords
        .filter { descriptor, _bundle -> !descriptor.normalization_required }
        .map { _descriptor, bundle ->
            tuple(bundle, bundle.resolve('normalization'))
        }
    ssqtlEntries = entryRecords.filter { descriptor, _bundle ->
        descriptor.normalization_required
    }
    NORMALIZE_SSQTL_V3(
        ssqtlEntries.map { descriptor, _bundle -> descriptor.run_id.toString() },
        ssqtlEntries.map { descriptor, _bundle ->
            descriptor.generation_id.toString()
        },
        ssqtlEntries.map { descriptor, _bundle ->
            descriptor.ssqtl.associations.toString()
        },
        ssqtlEntries.map { descriptor, _bundle ->
            descriptor.ssqtl.rds_dir.toString()
        },
        ssqtlEntries.map { descriptor, _bundle ->
            descriptor.ssqtl.bam_lookup.toString()
        },
        ssqtlEntries.map { descriptor, _bundle ->
            descriptor.ssqtl.violin_dir.toString()
        },
        ssqtlEntries.map { descriptor, _bundle ->
            descriptor.ssqtl.input_root.toString()
        },
        ssqtlEntries.map { descriptor, _bundle ->
            descriptor.ssqtl.reference.toString()
        },
        ssqtlEntries.map { descriptor, _bundle ->
            descriptor.ssqtl.config?.toString() ?: ''
        },
        ssqtlEntries.map { _descriptor, _bundle ->
            "${outputRoot}/normalization"
        },
        ssqtlEntries.map { _descriptor, bundle ->
            bundle.resolve('ssqtl_bind_contract.json')
        },
        ssqtlEntries.map { _descriptor, bundle ->
            bundle.resolve('project_binding.json')
        },
        runtimeValidationIdentity,
        runtimeFingerprint,
        executionPolicy,
        executionPolicyDoc,
    )
    normalizedSsqtlEntries = ssqtlEntries
        .combine(NORMALIZE_SSQTL_V3.out.bundle)
        .map { _descriptor, bundle, normalization ->
            tuple(bundle, normalization)
        }
    normalizedEntries = directEntries.mix(normalizedSsqtlEntries)

    ADMIT_PROJECT_TASKS(
        normalizedEntries,
        executionPolicy,
        entryHelper,
        shardLimit,
    )
    admissionBundle = ADMIT_PROJECT_TASKS.out.bundle.first()
    taskManifest = ADMIT_PROJECT_TASKS.out.bundle
        .map { bundle -> bundle.resolve('contract/tasks.jsonl') }
        .first()
    tasks = taskManifest
        .splitText()
        .filter { line -> line.trim() }
        .map { line ->
            def task = new groovy.json.JsonSlurperClassic().parseText(line)
            def stagedInputs = []
            if (task.core.preflight.state == 'READY') {
                task.core.tracks.each { track ->
                    stagedInputs << file(
                        track.bam.source_path.toString(),
                        checkIfExists: true
                    )
                    stagedInputs << file(
                        track.bai.source_path.toString(),
                        checkIfExists: true
                    )
                }
                task.core.reference.resources.each { _role, resource ->
                    stagedInputs << file(
                        resource.source_path.toString(),
                        checkIfExists: true
                    )
                }
                if (task.core.auxiliary.state == 'PRESENT') {
                    stagedInputs << file(
                        task.core.auxiliary.source_path.toString(),
                        checkIfExists: true
                    )
                }
            }
            tuple(task.task_id.toString(), line.trim(), stagedInputs)
        }

    def workerScript = file(
        "${projectDir}/src/ssqtl_igv/v3_worker.py",
        checkIfExists: true
    )
    def schemaDirectory = file(params.schema_dir, checkIfExists: true)
    RUN_PORTABLE_CASE(
        tasks,
        workerScript,
        runtimeConfig,
        runtimeManifest,
        runtimeFingerprint,
        runtimeValidationIdentity,
        executionPolicyDoc,
        schemaDirectory,
    )
    caseBundles = RUN_PORTABLE_CASE.out.case_bundle
        .map { _taskId, bundle -> bundle }
        .collect()
    FINALIZE_CASES(
        admissionBundle,
        caseBundles,
        entryHelper,
        params.test_mode && params.fake_runtime,
    )

    emit:
    results = FINALIZE_CASES.out.results
    contract = FINALIZE_CASES.out.contract
    shards = FINALIZE_CASES.out.shards
    snapshots = FINALIZE_CASES.out.snapshots
    failures = FINALIZE_CASES.out.failures
    summary = FINALIZE_CASES.out.summary
    runtime_manifest_validation = runtimeValidation
}
