nextflow.enable.dsl = 2

include { PLAN_RUN } from './workflows/plan_run'
include { RUN_SHARD } from './workflows/run_shard'
include { AGGREGATE_RUN } from './workflows/aggregate_run'
include { PUBLISH_RUN } from './workflows/publish_run'
include { PORTABLE_RUN } from './workflows/portable_run'
include { SSQTL_NORMALIZE } from './workflows/ssqtl_normalize'
include { PROJECT_RUN } from './workflows/project_run'


workflow {
    PROJECT_RUN()
}
