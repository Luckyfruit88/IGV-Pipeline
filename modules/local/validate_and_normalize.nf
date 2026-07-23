process VALIDATE_AND_NORMALIZE {
    tag "${run_id}:${generation_id}"
    label 'helper'
    label 'prepare'

    publishDir "${params.plan_output}", mode: 'copy', overwrite: false

    input:
    val run_id
    val generation_id
    val use_prepared
    path environment_bundle
    path gate_script, stageAs: 'software/assert_gate.py'
    path helper_script, stageAs: 'software/normalize_manifest.py'
    path params_file, stageAs: 'inputs/params_source'
    path associations, stageAs: 'inputs/associations_source'
    path rds_dir, stageAs: 'inputs/rds_dir'
    path bam_lookup, stageAs: 'inputs/bam_lookup_source'
    path violin_dir, stageAs: 'inputs/violin_dir'
    path genome_definition, stageAs: 'inputs/genome_definition_source'
    path fasta, stageAs: 'inputs/reference_fasta'
    path fai, stageAs: 'inputs/reference_fai'
    path cytoband, stageAs: 'inputs/reference_cytoband'
    path annotation, stageAs: 'inputs/reference_annotation'
    path r_wrapper, stageAs: 'inputs/r_prepare/prepare_cases_wrapper.R'
    path r_implementation, stageAs: 'inputs/r_prepare/prepare_cases_implementation.R'
    path prepared_cases, stageAs: 'inputs/prepared_cases_source'
    path prepared_samples, stageAs: 'inputs/prepared_samples_source'

    output:
    path 'normalization_bundle', emit: bundle

    script:
    def preparedArgs = use_prepared
        ? "--prepared-cases '${prepared_cases}' --prepared-samples '${prepared_samples}'"
        : ''
    def expectedArg = params.expected_case_count != null
        ? "--expected-case-count '${params.expected_case_count}'"
        : ''
    def testArg = params.test_mode ? '--allow-test' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${gate_script}' --kind environment --phase plan --bundle '${environment_bundle}' ${testArg}
    '${params.python}' '${helper_script}' \
        --params '${params_file}' \
        --output-dir normalization_bundle \
        --run-id '${run_id}' \
        --generation-id '${generation_id}' \
        --associations '${associations}' \
        --rds-dir '${rds_dir}' \
        --bam-lookup '${bam_lookup}' \
        --violin-dir '${violin_dir}' \
        --genome-definition '${genome_definition}' \
        --fasta '${fasta}' \
        --fai '${fai}' \
        --cytoband '${cytoband}' \
        --annotation '${annotation}' \
        --r-wrapper '${r_wrapper}' \
        --r-implementation '${r_implementation}' \
        --estimated-runtime-seconds '${params.estimated_seconds_per_case}' \
        ${preparedArgs} \
        ${expectedArg}
    """

    stub:
    """
    mkdir -p normalization_bundle
    : > normalization_bundle/tasks.jsonl
    printf '{"schema_version":"2.0","status":"STUB"}\n' > normalization_bundle/validation.json
    printf 'manifest_order\ttask_id\n' > normalization_bundle/normalized_manifest.tsv
    printf '{}\n' > normalization_bundle/parameters.json
    printf '{}\n' > normalization_bundle/r_prepare.json
    """
}
