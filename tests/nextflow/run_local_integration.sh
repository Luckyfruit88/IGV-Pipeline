#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
python_bin="${project_root}/.venv/bin/python"
nextflow_bin="${NEXTFLOW_BIN:-nextflow}"
java_home="${JAVA_HOME:-}"

if [[ ! -x "${python_bin}" ]]; then
    echo "missing test Python environment: ${python_bin}" >&2
    exit 2
fi
if [[ -z "${java_home}" || ! -x "${java_home}/bin/java" ]]; then
    echo "JAVA_HOME must point to a Java 21 runtime" >&2
    exit 2
fi
if ! command -v "${nextflow_bin}" >/dev/null 2>&1 && [[ ! -x "${nextflow_bin}" ]]; then
    echo "NEXTFLOW_BIN is not executable: ${nextflow_bin}" >&2
    exit 2
fi
if ! command -v Rscript >/dev/null 2>&1; then
    echo "Rscript is required for the raw-input integration contract" >&2
    exit 2
fi

export NXF_VER="${NXF_VER:-25.04.7}"
export NXF_ANSI_LOG="${NXF_ANSI_LOG:-false}"

created_temp=0
if [[ -n "${IGV_NF_TEST_ROOT:-}" ]]; then
    run_root="${IGV_NF_TEST_ROOT}"
    mkdir -p "${run_root}"
    run_root="$(cd "${run_root}" && pwd -P)"
else
    run_root="$(mktemp -d "${TMPDIR:-/tmp}/igv-nextflow-e3.XXXXXX")"
    created_temp=1
fi

java_link_root="$(mktemp -d "${TMPDIR:-/tmp}/igv-nextflow-java.XXXXXX")"
ln -s -- "${java_home}" "${java_link_root}/home"
export JAVA_HOME="${java_link_root}/home"
export JAVA_CMD="${JAVA_HOME}/bin/java"
export PATH="${JAVA_HOME}/bin:${PATH}"

cleanup() {
    status=$?
    if [[ ${status} -ne 0 || "${KEEP_TEST_OUTPUT:-0}" == "1" ]]; then
        echo "integration artifacts: ${run_root}" >&2
    elif [[ "${created_temp}" == "1" && "${run_root}" == */igv-nextflow-e3.* ]]; then
        rm -rf -- "${run_root}"
    fi
    if [[ "${java_link_root}" == */igv-nextflow-java.* ]]; then
        rm -rf -- "${java_link_root}"
    fi
    exit "${status}"
}
trap cleanup EXIT

output_root="${run_root}/output"
work_root="${run_root}/work"
export NXF_HOME="${run_root}/nextflow-home"
mkdir -p "${output_root}/sessions" "${work_root}" "${NXF_HOME}"
"${python_bin}" "${project_root}/tests/nextflow/build_fixture.py" \
    "${run_root}" --project-root "${project_root}"

fixture="${run_root}/fixture"
params_file="${fixture}/params.json"
pipeline_commit="$(git -C "${project_root}" rev-parse HEAD)"
Rscript "${project_root}/tests/nextflow/build_r_fixture.R" \
    "${fixture}/inputs/rds/AGratio_SNPgeno_pos_chrA_list.rds"

cd "${project_root}"

if "${nextflow_bin}" run . -entry PLAN_RUN -profile scc \
    --pipeline_commit "${pipeline_commit}" \
    >"${run_root}/scc-missing-sif.log" 2>&1; then
    echo "SCC profile unexpectedly accepted a missing helper SIF identity" >&2
    exit 1
fi
if ! grep -Fq 'this profile requires helper_sif and its lowercase SHA-256 identity' \
    "${run_root}/scc-missing-sif.log"; then
    echo "SCC profile did not fail at the expected helper SIF identity gate" >&2
    exit 1
fi

"${nextflow_bin}" run . -entry PLAN_RUN -profile test \
    --run_id test_raw_nextflow \
    --generation_id test_raw_generation_001 \
    --params_file "${params_file}" \
    --associations "${fixture}/inputs/associations.csv" \
    --rds_dir "${fixture}/inputs/rds" \
    --bam_lookup "${fixture}/inputs/bam_lookup.csv" \
    --violin_dir "${fixture}/inputs/violin" \
    --genome_definition "${fixture}/inputs/genome.json" \
    --fasta "${fixture}/inputs/genome.fa" \
    --fai "${fixture}/inputs/genome.fa.fai" \
    --cytoband "${fixture}/inputs/cytoband.txt.gz" \
    --annotation "${fixture}/inputs/annotation.gff.gz" \
    --r_prepare_wrapper "${project_root}/bin/prepare_cases.R" \
    --r_prepare_implementation "${fixture}/r_prepare_implementation.R" \
    --expected_case_count 1 \
    --plan_output "${output_root}/raw-plan" \
    --session_output "${output_root}/sessions/raw-plan-first" \
    --enable_reports true \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/raw-plan"

"${nextflow_bin}" run . -entry PLAN_RUN -profile test -resume \
    --run_id test_raw_nextflow \
    --generation_id test_raw_generation_001 \
    --params_file "${params_file}" \
    --associations "${fixture}/inputs/associations.csv" \
    --rds_dir "${fixture}/inputs/rds" \
    --bam_lookup "${fixture}/inputs/bam_lookup.csv" \
    --violin_dir "${fixture}/inputs/violin" \
    --genome_definition "${fixture}/inputs/genome.json" \
    --fasta "${fixture}/inputs/genome.fa" \
    --fai "${fixture}/inputs/genome.fa.fai" \
    --cytoband "${fixture}/inputs/cytoband.txt.gz" \
    --annotation "${fixture}/inputs/annotation.gff.gz" \
    --r_prepare_wrapper "${project_root}/bin/prepare_cases.R" \
    --r_prepare_implementation "${fixture}/r_prepare_implementation.R" \
    --expected_case_count 1 \
    --plan_output "${output_root}/raw-plan" \
    --session_output "${output_root}/sessions/raw-plan-resume" \
    --enable_reports true \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/raw-plan"

mv -- "${output_root}/raw-plan" "${output_root}/raw-plan-before-implementation-change"
printf '\n# Nextflow path-input cache invalidation probe.\n' >> \
    "${fixture}/r_prepare_implementation.R"

"${nextflow_bin}" run . -entry PLAN_RUN -profile test -resume \
    --run_id test_raw_nextflow \
    --generation_id test_raw_generation_001 \
    --params_file "${params_file}" \
    --associations "${fixture}/inputs/associations.csv" \
    --rds_dir "${fixture}/inputs/rds" \
    --bam_lookup "${fixture}/inputs/bam_lookup.csv" \
    --violin_dir "${fixture}/inputs/violin" \
    --genome_definition "${fixture}/inputs/genome.json" \
    --fasta "${fixture}/inputs/genome.fa" \
    --fai "${fixture}/inputs/genome.fa.fai" \
    --cytoband "${fixture}/inputs/cytoband.txt.gz" \
    --annotation "${fixture}/inputs/annotation.gff.gz" \
    --r_prepare_wrapper "${project_root}/bin/prepare_cases.R" \
    --r_prepare_implementation "${fixture}/r_prepare_implementation.R" \
    --expected_case_count 1 \
    --plan_output "${output_root}/raw-plan" \
    --session_output "${output_root}/sessions/raw-plan-changed" \
    --enable_reports true \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/raw-plan"

"${nextflow_bin}" run . -entry PLAN_RUN -profile test \
    --run_id test_local_nextflow \
    --generation_id test_generation_001 \
    --params_file "${params_file}" \
    --associations "${fixture}/inputs/associations.csv" \
    --rds_dir "${fixture}/inputs/rds" \
    --bam_lookup "${fixture}/inputs/bam_lookup.csv" \
    --violin_dir "${fixture}/inputs/violin" \
    --genome_definition "${fixture}/inputs/genome.json" \
    --fasta "${fixture}/inputs/genome.fa" \
    --fai "${fixture}/inputs/genome.fa.fai" \
    --cytoband "${fixture}/inputs/cytoband.txt.gz" \
    --annotation "${fixture}/inputs/annotation.gff.gz" \
    --prepared_cases "${fixture}/prepared_cases.tsv" \
    --prepared_samples "${fixture}/prepared_samples.tsv" \
    --expected_case_count 1 \
    --plan_output "${output_root}/plan" \
    --session_output "${output_root}/sessions/plan" \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/plan"

shard_manifest="${output_root}/plan/shard_bundle/shards/shard_001.jsonl"
invalid_shard_manifest="${run_root}/invalid/shard_001.jsonl"
"${python_bin}" "${project_root}/tests/nextflow/make_invalid_task.py" \
    "${output_root}/plan/normalization_bundle/tasks.jsonl" \
    "${invalid_shard_manifest}"

"${nextflow_bin}" run . -entry RUN_SHARD -profile test \
    --params_file "${params_file}" \
    --shard_manifest "${invalid_shard_manifest}" \
    --shard_id shard_001 \
    --session_id local_domain_failure \
    --run_output "${output_root}/domain-failure-run" \
    --session_output "${output_root}/sessions/domain-failure-run" \
    --fake_runtime true \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/domain-failure-run"

"${nextflow_bin}" run . -entry RUN_SHARD -profile test \
    --params_file "${params_file}" \
    --shard_manifest "${shard_manifest}" \
    --shard_id shard_001 \
    --session_id local_fake_runtime \
    --run_output "${output_root}/run" \
    --session_output "${output_root}/sessions/run" \
    --enable_reports true \
    --fake_runtime true \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/run"

"${nextflow_bin}" run . -entry RUN_SHARD -profile test -resume \
    --params_file "${params_file}" \
    --shard_manifest "${shard_manifest}" \
    --shard_id shard_001 \
    --session_id local_fake_runtime \
    --run_output "${output_root}/run" \
    --session_output "${output_root}/sessions/run-resume" \
    --enable_reports true \
    --fake_runtime true \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/run"

"${nextflow_bin}" run . -entry AGGREGATE_RUN -profile test \
    --canonical_tasks "${output_root}/plan/normalization_bundle/tasks.jsonl" \
    --shard_plan "${output_root}/plan/shard_bundle/shard_plan.json" \
    --shard_summaries "${output_root}/run/summary/*" \
    --compose_bundles "${output_root}/run/stages/compose_case/*" \
    --qc_bundles "${output_root}/run/stages/qc_case/*" \
    --trace_files "${output_root}/sessions/run/trace.txt" \
    --aggregate_output "${output_root}/aggregate" \
    --review_output "${output_root}/review" \
    --session_output "${output_root}/sessions/aggregate" \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/aggregate"

"${nextflow_bin}" run . -entry PUBLISH_RUN -profile test \
    --review_package "${output_root}/review/review_package" \
    --review_records "${fixture}/reviews.jsonl" \
    --aggregate_case_results "${output_root}/aggregate/aggregate_bundle/case_results.jsonl" \
    --validated_review_output "${output_root}/validated" \
    --publication_destination "${output_root}/published" \
    --session_output "${output_root}/sessions/publish" \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/publish"

"${nextflow_bin}" run . -entry PLAN_RUN -profile test -stub-run \
    --run_id test_stub \
    --generation_id test_stub_generation \
    --params_file "${params_file}" \
    --associations "${fixture}/inputs/associations.csv" \
    --rds_dir "${fixture}/inputs/rds" \
    --bam_lookup "${fixture}/inputs/bam_lookup.csv" \
    --violin_dir "${fixture}/inputs/violin" \
    --genome_definition "${fixture}/inputs/genome.json" \
    --fasta "${fixture}/inputs/genome.fa" \
    --fai "${fixture}/inputs/genome.fa.fai" \
    --cytoband "${fixture}/inputs/cytoband.txt.gz" \
    --annotation "${fixture}/inputs/annotation.gff.gz" \
    --prepared_cases "${fixture}/prepared_cases.tsv" \
    --prepared_samples "${fixture}/prepared_samples.tsv" \
    --plan_output "${output_root}/stub/plan" \
    --session_output "${output_root}/sessions/stub-plan" \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/stub-plan"

"${nextflow_bin}" run . -entry RUN_SHARD -profile test -stub-run \
    --params_file "${params_file}" \
    --shard_manifest "${shard_manifest}" \
    --shard_id shard_001 \
    --session_id stub_runtime \
    --run_output "${output_root}/stub/run" \
    --session_output "${output_root}/sessions/stub-run" \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/stub-run"

"${nextflow_bin}" run . -entry AGGREGATE_RUN -profile test -stub-run \
    --canonical_tasks "${output_root}/plan/normalization_bundle/tasks.jsonl" \
    --shard_plan "${output_root}/plan/shard_bundle/shard_plan.json" \
    --shard_summaries "${output_root}/run/summary/*" \
    --compose_bundles "${output_root}/run/stages/compose_case/*" \
    --qc_bundles "${output_root}/run/stages/qc_case/*" \
    --trace_files "${output_root}/sessions/run/trace.txt" \
    --aggregate_output "${output_root}/stub/aggregate" \
    --review_output "${output_root}/stub/review" \
    --session_output "${output_root}/sessions/stub-aggregate" \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/stub-aggregate"

"${nextflow_bin}" run . -entry PUBLISH_RUN -profile test -stub-run \
    --review_package "${output_root}/review/review_package" \
    --review_records "${fixture}/reviews.jsonl" \
    --aggregate_case_results "${output_root}/aggregate/aggregate_bundle/case_results.jsonl" \
    --validated_review_output "${output_root}/stub/validated" \
    --publication_destination "${output_root}/stub/published" \
    --session_output "${output_root}/sessions/stub-publish" \
    --pipeline_commit "${pipeline_commit}" \
    -work-dir "${work_root}/stub-publish"

"${python_bin}" "${project_root}/tests/nextflow/verify_outputs.py" "${run_root}"
