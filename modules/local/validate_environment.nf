process VALIDATE_ENVIRONMENT {
    tag "${phase}"
    label 'environment'

    publishDir "${params.session_output}/environment", mode: 'copy', overwrite: false

    input:
    val phase
    val required_commands
    path helper_script, stageAs: 'software/validate_environment.py'

    output:
    path "environment_${phase}", emit: bundle

    script:
    def commandArgs = required_commands.collect { item -> "--require-command '${item}'" }.join(' ')
    def testArg = params.test_mode ? '--test-mode' : ''
    def sifArgs = !params.test_mode && params.helper_sif
        ? "--helper-sif '${params.helper_sif}'" + (params.helper_sif_sha256 ? " --helper-sif-sha256 '${params.helper_sif_sha256}'" : '')
        : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${helper_script}' \
        --output-dir 'environment_${phase}' \
        --phase '${phase}' \
        --pipeline-commit '${params.pipeline_commit}' \
        --nextflow-version '${workflow.nextflow.version}' \
        ${commandArgs} \
        ${sifArgs} \
        ${testArg}
    """

    stub:
    """
    mkdir -p 'environment_${phase}'
    printf '{"schema_version":"2.0-environment","status":"STUB","phase":"${phase}"}\n' > 'environment_${phase}/environment.json'
    """
}
