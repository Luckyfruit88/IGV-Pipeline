include { VALIDATE_ENVIRONMENT } from '../modules/local/validate_environment'
include { VALIDATE_AND_NORMALIZE } from '../modules/local/validate_and_normalize'
include { CREATE_SHARDS } from '../modules/local/create_shards'


workflow PLAN_RUN {

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
        'run_id',
        'generation_id',
        'params_file',
        'associations',
        'rds_dir',
        'bam_lookup',
        'violin_dir',
        'genome_definition',
        'fasta',
        'fai',
        'cytoband',
        'annotation',
        'plan_output',
    ]
    def missing = required.findAll { key -> params[key] == null || params[key].toString().trim() == '' }
    if (missing) {
        error("PLAN_RUN missing required parameters: ${missing.join(', ')}")
    }
    def hasPreparedCases = params.prepared_cases != null && params.prepared_cases.toString().trim() != ''
    def hasPreparedSamples = params.prepared_samples != null && params.prepared_samples.toString().trim() != ''
    if (hasPreparedCases != hasPreparedSamples) {
        error('PLAN_RUN requires prepared_cases and prepared_samples together')
    }

    def paramsFile = file(params.params_file, checkIfExists: true)
    def associations = file(params.associations, checkIfExists: true)
    def rdsDir = file(params.rds_dir, checkIfExists: true)
    def bamLookup = file(params.bam_lookup, checkIfExists: true)
    def violinDir = file(params.violin_dir, checkIfExists: true)
    def genomeDefinition = file(params.genome_definition, checkIfExists: true)
    def fasta = file(params.fasta, checkIfExists: true)
    def fai = file(params.fai, checkIfExists: true)
    def cytoband = file(params.cytoband, checkIfExists: true)
    def annotation = file(params.annotation, checkIfExists: true)
    def rWrapper = file(params.r_prepare_wrapper, checkIfExists: true)
    def rImplementation = file(params.r_prepare_implementation, checkIfExists: true)
    def preparedCases = hasPreparedCases ? file(params.prepared_cases, checkIfExists: true) : rWrapper
    def preparedSamples = hasPreparedSamples ? file(params.prepared_samples, checkIfExists: true) : rImplementation
    def gateScript = file("${projectDir}/bin/assert_gate.py", checkIfExists: true)
    def environmentScript = file("${projectDir}/bin/validate_environment.py", checkIfExists: true)
    def normalizeScript = file("${projectDir}/bin/normalize_manifest.py", checkIfExists: true)
    def shardScript = file("${projectDir}/bin/create_shards.py", checkIfExists: true)

    def requiredCommands = hasPreparedCases ? [] : ['Rscript']
    VALIDATE_ENVIRONMENT('plan', requiredCommands, environmentScript)
    VALIDATE_AND_NORMALIZE(
        params.run_id,
        params.generation_id,
        hasPreparedCases,
        VALIDATE_ENVIRONMENT.out.bundle,
        gateScript,
        normalizeScript,
        paramsFile,
        associations,
        rdsDir,
        bamLookup,
        violinDir,
        genomeDefinition,
        fasta,
        fai,
        cytoband,
        annotation,
        rWrapper,
        rImplementation,
        preparedCases,
        preparedSamples,
    )
    CREATE_SHARDS(VALIDATE_AND_NORMALIZE.out.bundle, shardScript)

    emit:
    environment = VALIDATE_ENVIRONMENT.out.bundle
    normalization = VALIDATE_AND_NORMALIZE.out.bundle
    shards = CREATE_SHARDS.out.bundle
}
