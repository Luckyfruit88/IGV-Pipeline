process RESOLVE_EXECUTION_POLICY {
    tag "execution-policy:${execution_mode}"
    label 'control'
    cache false

    input:
    path resolver_source, stageAs: 'software/resolve_execution_policy.py'
    val execution_mode
    val max_parallel
    val igv_cpus
    val igv_memory
    val igv_timeout
    val normalization_cpus
    val normalization_memory
    val normalization_timeout

    output:
    path 'execution_policy.json', emit: policy

    script:
    """
    export PYTHONDONTWRITEBYTECODE=1
    '${params.python}' 'software/resolve_execution_policy.py' \
        --output execution_policy.json \
        --execution-mode '${execution_mode}' \
        --max-parallel '${max_parallel}' \
        --igv-cpus '${igv_cpus}' \
        --igv-memory '${igv_memory}' \
        --igv-timeout '${igv_timeout}' \
        --normalization-cpus '${normalization_cpus}' \
        --normalization-memory '${normalization_memory}' \
        --normalization-timeout '${normalization_timeout}'
    """
}
