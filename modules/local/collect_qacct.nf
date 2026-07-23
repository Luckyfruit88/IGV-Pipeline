process COLLECT_QACCT {
    tag "${params.run_id}:${params.generation_id}"
    label 'accounting'
    label 'aggregate'

    publishDir "${params.aggregate_output}", mode: 'copy', overwrite: false

    input:
    path environment_bundle
    path gate_script, stageAs: 'software/assert_gate.py'
    path helper_script, stageAs: 'software/collect_qacct.py'
    path trace_files, stageAs: 'traces/??/*'

    output:
    path 'accounting_bundle', emit: bundle

    script:
    def traceList = trace_files instanceof java.util.Collection ? trace_files : [trace_files]
    def traceCopies = traceList
        .withIndex()
        .collect { source, index ->
            "cp -L '${source}' 'trace_input_${String.format('%03d', index + 1)}.txt'"
        }
        .join('\n')
    def traceArgs = traceList
        .withIndex()
        .collect { _source, index ->
            "--trace 'trace_input_${String.format('%03d', index + 1)}.txt'"
        }
        .join(' ')
    def skipArg = params.skip_qacct ? '--skip-qacct' : ''
    def testArg = params.test_mode ? '--test-mode' : ''
    def gateTestArg = params.test_mode ? '--allow-test' : ''
    """
    export PYTHONDONTWRITEBYTECODE=1
    export IGV_PIPELINE_COMMIT='${params.pipeline_commit}'
    '${params.python}' '${gate_script}' --kind environment --phase aggregate_run --bundle '${environment_bundle}' ${gateTestArg}
    ${traceCopies}
    '${params.python}' '${helper_script}' \
        ${traceArgs} \
        --output-dir accounting_bundle \
        ${skipArg} \
        ${testArg}
    """

    stub:
    """
    mkdir -p accounting_bundle/raw
    printf '{"schema_version":"2.0-nextflow-qacct","status":"STUB"}\n' > accounting_bundle/accounting.json
    printf 'trace_file\ttask_id\tprocess\tstatus\tnative_id\n' > accounting_bundle/scheduler.tsv
    """
}
