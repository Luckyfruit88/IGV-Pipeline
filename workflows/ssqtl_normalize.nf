include { NORMALIZE_SSQTL_V3 } from '../modules/local/normalize_ssqtl_v3'
include { RESOLVE_EXECUTION_POLICY } from '../modules/local/resolve_execution_policy'
include { VALIDATE_RUNTIME_IDENTITY } from '../modules/local/validate_runtime_identity'

workflow SSQTL_NORMALIZE {

    main:
    def required = ['run_id', 'generation_id', 'ssqtl_associations', 'ssqtl_rds_dir', 'ssqtl_bam_lookup', 'ssqtl_violin_dir', 'ssqtl_input_root', 'ssqtl_reference', 'ssqtl_reference_root', 'ssqtl_normalization_output', 'ssqtl_bind_contract', 'runtime_manifest']
    def missing = required.findAll { key -> params[key] == null || params[key].toString().trim() == '' }
    if (missing) {
        error("SSQTL_NORMALIZE missing required parameters: ${missing.join(', ')}")
    }
    def manifestSha = params.runtime_manifest_sha256?.toString()?.trim() ?: ''
    def fingerprintSha = params.runtime_fingerprint_sha256?.toString()?.trim() ?: ''
    def ociDigest = params.observed_oci_digest?.toString()?.trim() ?: ''
    def runtimeSifSha = params.runtime_sif_sha256?.toString()?.trim() ?: ''
    if (!(manifestSha ==~ /[a-f0-9]{64}/)) {
        error('SSQTL_NORMALIZE requires runtime_manifest_sha256')
    }
    if (!(fingerprintSha ==~ /[a-f0-9]{64}/)) {
        error('SSQTL_NORMALIZE requires runtime_fingerprint_sha256')
    }
    if (ociDigest && !(ociDigest ==~ /sha256:[a-f0-9]{64}/)) {
        error('SSQTL_NORMALIZE observed OCI digest is invalid')
    }

    def runtimeManifest = file(params.runtime_manifest, checkIfExists: true)
    def bindContract = file(params.ssqtl_bind_contract, checkIfExists: true)
    def manifestSchema = file("${projectDir}/containers/runtime-manifest.schema.json", checkIfExists: true)
    def runtimeMaterials = file("${projectDir}/containers/runtime-materials.lock.json", checkIfExists: true)
    def explicitLocks = [
        file("${projectDir}/containers/fonts-local.conf", checkIfExists: true),
        file("${projectDir}/containers/runtime-system-packages.lock", checkIfExists: true),
        file("${projectDir}/containers/helper-linux-64.lock", checkIfExists: true),
        file("${projectDir}/containers/samtools-linux-64.lock", checkIfExists: true),
        file("${projectDir}/containers/materials.lock.json", checkIfExists: true),
    ]
    def runtimeConfig = file("${projectDir}/src/ssqtl_igv/resources/v3-runtime.yaml", checkIfExists: true)
    def identityValidator = file("${projectDir}/bin/validate_runtime_identity.py", checkIfExists: true)
    def policyResolver = file("${projectDir}/bin/resolve_execution_policy.py", checkIfExists: true)
    def executionMode = params.runtime_execution_mode?.toString()?.trim() ?: 'standalone'

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
    executionPolicy = RESOLVE_EXECUTION_POLICY.out.policy.first()
    executionPolicyDoc = executionPolicy
        .map { policyPath -> policyPath.text }
        .first()

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
    NORMALIZE_SSQTL_V3(
        params.run_id.toString(),
        params.generation_id.toString(),
        params.ssqtl_associations.toString(),
        params.ssqtl_rds_dir.toString(),
        params.ssqtl_bam_lookup.toString(),
        params.ssqtl_violin_dir.toString(),
        params.ssqtl_input_root.toString(),
        params.ssqtl_reference.toString(),
        (params.ssqtl_config ?: '').toString(),
        bindContract,
        bindContract,
        runtimeValidation,
        fingerprintSha,
        executionPolicy,
        executionPolicyDoc,
    )

    emit:
    normalization_bundle = NORMALIZE_SSQTL_V3.out.bundle
    runtime_manifest_validation = runtimeValidation
    execution_policy = executionPolicy
}
