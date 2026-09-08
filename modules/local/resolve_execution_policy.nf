def executionPolicyForCache(String document) {
    def policy = new groovy.json.JsonSlurperClassic().parseText(document)
    // Keep host observations and queue capacity in the retained policy file,
    // not in a worker's cache key. Identical work can resume on another node.
    return groovy.json.JsonOutput.toJson([render: policy.render, normalization: policy.normalization])
}


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
    def bootstrap = params.bootstrap_execution_policy?.toString()
    if (!bootstrap) error('Execution policy was not resolved before scheduler startup')
    def encoded = java.util.Base64.getUrlEncoder().encodeToString(bootstrap.getBytes('UTF-8'))
    """
    export PYTHONDONTWRITEBYTECODE=1
    '${params.python}' 'software/resolve_execution_policy.py' \
        --output execution_policy.json \
        --from-json-b64 '${encoded}' \
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
