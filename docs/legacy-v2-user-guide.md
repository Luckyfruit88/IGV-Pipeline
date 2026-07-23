# IGV Snapshot Workflow 2.x legacy user guide

[Current v3 English](../README.md) | [当前 v3 中文](../README.zh-CN.md)

> Historical reference only. In a v3 installation, invoke this frozen 2.x interface as `ssqtl-igv`; the `igv-snapshot` and `igv-snapshot-workflow` commands use the new schema-3.0 interface.

The frozen `ssqtl-igv` interface is a configurable Python/R pipeline for turning association
records and sample-level sequencing data into reviewable, native IGV evidence
figures. This guide is written for first-time users as well as operators running
large jobs on a Linux cluster.

The package does not include private scientific data, reference resources, IGV,
or cluster credentials. You provide those through a YAML configuration file.

The only v2 command is `ssqtl-igv`. The names `igv-snapshot` and
`igv-snapshot-workflow` both invoke v3 and must not be used with this guide.

## 1. What the full workflow does

For every association row, the workflow:

1. identifies the chromosome and strand;
2. opens the corresponding RDS object and extracts the target AG-site table;
3. keeps the target SNP genotype and ratio values;
4. selects representative samples from available genotype groups;
5. maps sample IDs to local BAM and BAI files;
6. starts an isolated Xvfb desktop and native IGV client;
7. loads the local genome, annotation, event, BAM, coverage, sequence, and
   alignment tracks;
8. waits for the IGV window, toolbar/locus region, and rendered pixels to become
   stable;
9. captures the complete native IGV client window;
10. locates the matching violin plot and places it to the right of the untouched
    IGV image;
11. runs mechanical and scientific QC;
12. stops at `REVIEW_PENDING` for explicit human approval;
13. publishes approved cases or creates a rerun manifest for rejected/failed
    cases.

The per-case state flow is:

```text
PREPARED
  -> IGV_STARTING
  -> WINDOW_CAPTURED
  -> GUI_SETTLED
  -> RENDER_STABLE
  -> COMPOSED
  -> QC_PASS
  -> REVIEW_PENDING
  -> PUBLISHED

Any execution/QC failure -> FAILED -> RERUN
Any manual rejection     -> RERUN
```

This is a closed-loop workflow: execution alone does not declare a scientific
result valid. Automated QC supplies evidence; a reviewer makes the final visual
decision.

## 2. Decide where and how you will run

The supported platform is POSIX/Linux with Python 3.10 or newer.

Choose one execution mode:

- `local`: process cases serially in the current session. This is useful for a
  small validation run or a workstation with all native dependencies.
- `grid_engine`: create bounded Grid Engine arrays, submit with `qsub`, and
  validate accounting with `qacct`. This is recommended for a large full run.

The wheel installs only the Python package plus Pillow and PyYAML. The following
native tools must already be available:

- R and `Rscript`;
- IGV and Java;
- Xvfb, `xwininfo`, and `xprop`;
- ImageMagick `import`;
- Poppler `pdftotext` and `pdftoppm`;
- Tesseract and the configured OCR language;
- `qsub` and `qacct` for Grid Engine mode.

A graphical workstation is not required. IGV runs inside Xvfb on the Linux
compute node.

## 3. Install and verify the package

### 3.1 Create a clean Python environment

Do not use a Python 3.6/3.7 environment. Verify the interpreter before creating
the venv:

```bash
python3 --version
python3 -m venv "$HOME/venvs/igv-snapshot-workflow-1.0.1"
source "$HOME/venvs/igv-snapshot-workflow-1.0.1/bin/activate"
python --version
```

### 3.2 Locate and install the wheel

Paths such as `/path/to/file.whl` are placeholders. Replace them with the real,
absolute path returned by `find` or supplied by your administrator:

```bash
find "$HOME" /your/shared/project/root \
  -name 'igv_snapshot_workflow-1.0.1-py3-none-any.whl' \
  -type f 2>/dev/null
```

Then install the exact file:

```bash
python -m pip install \
  /absolute/path/to/igv_snapshot_workflow-1.0.1-py3-none-any.whl
```

For an offline installation, place the application wheel plus compatible Pillow
and PyYAML wheels in one wheelhouse:

```bash
python -m pip install --no-index --find-links /absolute/path/to/wheelhouse \
  /absolute/path/to/wheelhouse/igv_snapshot_workflow-1.0.1-py3-none-any.whl
```

### 3.3 Verify the installation

```bash
python -m pip check
python -m pip show igv-snapshot-workflow
command -v ssqtl-igv
ssqtl-igv --help
ssqtl-igv --help
```

`pip check` must report no broken requirements, and both CLI commands must show
help successfully.

## 4. Prepare your scientific inputs

A BAM file by itself is not sufficient. A complete run needs all of the
following.

### 4.1 Association table

The default CSV columns are:

```csv
AG_site,SNP,strand
chr13:112881814-112881815,chr13.112881818_T.C,+
chr13:45340822-45340821,chr13.45340822_A.G,-
```

Column names are configurable under `inputs.association_columns`. Every
normalized AG-site/SNP pair must be unique.

### 4.2 RDS directory

By default, one RDS file is selected from chromosome and strand using:

```text
AGratio_SNPgeno_{strand_token}_{chrom}_list.rds
```

Each RDS must contain a named list in which:

- the AG site matches exactly one list entry;
- that entry contains the target SNP column;
- a configured sample-ID column exists;
- the configured ratio column exists.

Adjust `inputs.rds_filename_template`, `locus_sample_columns`, and
`ratio_column` if your data uses different names.

### 4.3 BAM lookup and BAI files

Create a CSV mapping each sample ID to one BAM file or BAM directory. For
example:

```csv
sample_id,bam_path
sample_001,/absolute/path/to/sample_001.bam
sample_002,/absolute/path/to/sample_002.bam
```

The accepted ID/path column names and BAM suffixes are configurable. Requirements:

- sample IDs must match the RDS sample IDs;
- a sample ID must not map to multiple paths;
- every selected BAM must exist and be readable;
- every selected BAM must have a readable `.bai` or alternate supported BAI;
- decide whether an older BAI is a warning or failure with
  `inputs.stale_bai_policy`.

### 4.4 Violin PDFs

The configured violin directory must contain PDFs whose names follow
`paths.violin_pdf_template`. The PDF text must allow the workflow to identify
the exact AG-site/SNP pair. A similarly named plot is not accepted as an exact
match.

### 4.5 Local genome and annotation resources

Provide local paths for:

- the IGV genome definition;
- reference FASTA and FAI;
- cytoband file;
- annotation GFF/GTF resource.

The genome definition must not redirect IGV to remote genome/annotation
resources. Optional SHA-256 fields can pin every immutable resource.

## 5. Create and edit the configuration

Generate the packaged template:

```bash
ssqtl-igv init-config --output workflow.yaml
```

Do not use `--force` unless you intentionally want to replace an existing
configuration.

At minimum, replace every `/path/to/...` value in these sections:

```yaml
paths:
  associations: /absolute/path/to/associations.csv
  associations_sha256: null
  rds_dir: /absolute/path/to/rds
  bam_lookup: /absolute/path/to/bam_lookup.csv
  violin_dir: /absolute/path/to/violin_pdfs
  output_root: /absolute/path/to/workflow_runs
  publish_root: /absolute/path/to/published_results

genome:
  id: hg38
  display_name: Human (GRCh38/hg38)
  definition: /absolute/path/to/local_genome.json
  fasta: /absolute/path/to/reference.fa
  fai: /absolute/path/to/reference.fa.fai
  cytoband: /absolute/path/to/cytoband.txt.gz
  annotation: /absolute/path/to/annotation.gff.gz
  annotation_version: your-annotation-release
```

For a local run:

```yaml
execution:
  mode: local
```

For Grid Engine:

```yaml
execution:
  mode: grid_engine

environment:
  module_purge: false
  modules: []
  venv: /absolute/path/to/the/shared/venv

scheduler:
  project: your-grid-engine-project
  runtime: "12:00:00"
  memory_per_core: 8G
  memory_gb: 8
  total_parallel_memory_gb: 64
  max_parallel: 8
  max_tasks_per_array: 8
  cases_per_task: 1
```

The `environment.venv` path must be visible from every compute node. Scheduler
values are site-specific; do not copy memory, runtime, project, or concurrency
settings without checking your cluster policy.

Useful fail-closed bindings include:

```yaml
inputs:
  expected_case_count: 100

paths:
  associations_sha256: your-observed-sha256

genome:
  definition_sha256: your-observed-sha256
  fasta_sha256: your-observed-sha256
  fai_sha256: your-observed-sha256
  cytoband_sha256: your-observed-sha256
  annotation_sha256: your-observed-sha256
```

Use `sha256sum /absolute/path/to/file` to obtain a digest. Leaving a SHA field
as `null` disables that identity check.

## 6. Check the environment before any run

Run the standalone preflight first:

```bash
ssqtl-igv preflight --config workflow.yaml
```

Preflight checks:

- required executables;
- Pillow and PyYAML;
- configured files/directories and optional hashes;
- local-only genome definitions;
- writable output/publish parents;
- storage capacity and inode requirements;
- Grid Engine tools when `execution.mode: grid_engine`.

Do not continue after a fatal issue. Fix the configuration or environment and
repeat preflight until it passes.

## 7. Run an isolated smoke test

The CLI intentionally has no special `smoke` subcommand. A smoke test is an
ordinary run with a small, representative association table and completely
separate output locations.

### 7.1 Choose representative cases

Create `smoke_associations.csv` with the original header and approximately 4–10
rows. Include, when available:

- at least one positive-strand case;
- at least one negative-strand case;
- a case with all genotype groups;
- a case with fewer samples or an empty genotype group;
- a higher-coverage case.

Do not edit or truncate the production association file in place.

### 7.2 Create an isolated smoke configuration

```bash
cp workflow.yaml workflow.smoke.yaml
```

Change only the smoke-specific inputs/outputs:

```yaml
paths:
  associations: /absolute/path/to/smoke_associations.csv
  associations_sha256: null
  output_root: /absolute/path/to/smoke_runs
  publish_root: /absolute/path/to/smoke_review

inputs:
  expected_case_count: 4
```

Never reuse a production run root or publish root for smoke evidence.

### 7.3 Plan and execute the smoke test

For Grid Engine, first create an audited plan without calling `qsub`:

```bash
SMOKE_RUN_ROOT=/absolute/path/to/smoke_runs/smoke_001

ssqtl-igv run \
  --config workflow.smoke.yaml \
  --run-root "$SMOKE_RUN_ROOT" \
  --dry-run
```

Confirm the expected case count, task map, resources, and bounded array plan.
Then submit the same configuration and run root:

```bash
ssqtl-igv run \
  --config workflow.smoke.yaml \
  --run-root "$SMOKE_RUN_ROOT" \
  --submit
```

For local mode, run serially without `--dry-run` or `--submit`:

```bash
ssqtl-igv run \
  --config workflow.smoke.yaml \
  --run-root /absolute/path/to/smoke_runs/smoke_001
```

### 7.4 Close the smoke test

For Grid Engine, wait until the submitted jobs are absent from `qstat`, then:

```bash
ssqtl-igv collect-qacct \
  --config workflow.smoke.yaml \
  --run-root "$SMOKE_RUN_ROOT"
```

Create the review bundle/status files:

```bash
ssqtl-igv summarize \
  --config workflow.smoke.yaml \
  --run-root "$SMOKE_RUN_ROOT"
```

Successful cases should be `REVIEW_PENDING`. Inspect every image using the
acceptance checklist in section 10. Do not start a large run if any unresolved
IGV startup, capture, locus, annotation, sample, or plot-matching failure remains.

## 8. Plan and launch the full run

Use a new production run root. Do not reuse the smoke run root.

```bash
RUN_ROOT=/absolute/path/to/workflow_runs/full_run_001
```

### 8.1 Grid Engine dry-run

`--dry-run` prepares the immutable input snapshot, builds the manifest and
shards, performs preflight, and creates an audited scheduler plan. It does not
call `qsub` or run IGV cases.

```bash
ssqtl-igv run \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --dry-run
```

Before submission, verify:

- manifest count equals `inputs.expected_case_count` when configured;
- no duplicate/missing case IDs;
- no preparation failures;
- task ranges cover the intended cases exactly once;
- each array contains no more than `scheduler.max_tasks_per_array`;
- the requested memory/runtime/concurrency follow cluster policy;
- the plan points to the intended wheel runtime and configuration.

### 8.2 Submit the audited plan

Use the same config and run root:

```bash
ssqtl-igv run \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --submit
```

The scheduler uses bounded arrays, applies `-tc` as defense in depth, chains
arrays with `-hold_jid`, and holds the summary job on the final array.

Do not submit again merely because a `qsub` command timed out or returned an
ambiguous response. Inspect Grid Engine and the generated `jobs.json` first to
avoid duplicate work.

### 8.3 Local full run

Local mode processes incomplete cases serially:

```bash
ssqtl-igv run \
  --config workflow.yaml \
  --run-root "$RUN_ROOT"
```

## 9. Collect scheduler evidence and summarize

Grid Engine users must wait for all recorded arrays to finish, then freeze
accounting evidence:

```bash
ssqtl-igv collect-qacct \
  --config workflow.yaml \
  --run-root "$RUN_ROOT"
```

The command binds qacct records to recorded job IDs, owner, job name, project,
task ranges, exit status, and configured concurrency.

Then summarize:

```bash
ssqtl-igv summarize \
  --config workflow.yaml \
  --run-root "$RUN_ROOT"
```

The run root receives auditable files including:

- `final_status.tsv`;
- `failure_report.tsv`;
- `rerun_manifest.tsv`;
- `provenance.json`;
- `telemetry.json`;
- checksum/identity metadata.

Reviewable figures and sample tables are delivered under the configured
`paths.publish_root`, organized by chromosome. A first summary normally reports:

- `REVIEW_PENDING` when automated QC passed but human review is incomplete;
- `RERUN_REQUIRED` when one or more cases failed or were rejected;
- `PUBLISHED` only after all required approvals are recorded.

## 10. Review and accept the results

Automated PASS does not replace visual review. For every `REVIEW_PENDING` case,
confirm all of the following:

- the left side is the complete native IGV client window;
- menus, toolbar, genome/chromosome/locus controls, ideogram, ruler, tracks,
  sequence, and annotation are visible and readable;
- the displayed genome and locus are correct;
- BAM/BAI sample identity and genotype-group ordering match the sample table;
- native coverage, read/alignment, and junction structure are judgeable;
- the annotation/transcript and strand are scientifically appropriate;
- the AG-site reference context is visible;
- splice/junction evidence near the AG site, or its absence, can be judged;
- the violin plot names the exact same AG-site/SNP pair;
- no crop, overlay, external label, divider, or resampling altered the IGV client.

Approve one or more cases only after visual inspection:

```bash
ssqtl-igv review \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --case-id CASE_ID \
  --decision approve \
  --reviewer REVIEWER_ID \
  --confirm-native-igv \
  --confirm-annotation-visible \
  --confirm-strand-transcript \
  --confirm-ag-site-context \
  --confirm-splice-junction-judgeable \
  --confirm-violin-pair
```

Reject an unacceptable case with a reason:

```bash
ssqtl-igv review \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --case-id CASE_ID \
  --decision reject \
  --reviewer REVIEWER_ID \
  --notes 'Describe the visible problem and required correction'
```

A rejection means “rerun required”; it is not negative biological evidence.

After recording reviews, run `summarize` again:

```bash
ssqtl-igv summarize \
  --config workflow.yaml \
  --run-root "$RUN_ROOT"
```

Closure is complete only when no case remains in `REVIEW_PENDING` or `RERUN`.

## 11. Rerun failed or rejected cases

Use the generated rerun manifest. Do not manually edit case state files.

Grid Engine dry-run:

```bash
ssqtl-igv resume \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --rerun-manifest "$RUN_ROOT/rerun_manifest.tsv" \
  --dry-run \
  --generation 1
```

Grid Engine submit:

```bash
ssqtl-igv resume \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --rerun-manifest "$RUN_ROOT/rerun_manifest.tsv" \
  --submit \
  --generation 1
```

Local rerun:

```bash
ssqtl-igv resume \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --rerun-manifest "$RUN_ROOT/rerun_manifest.tsv" \
  --local \
  --generation 1
```

After a Grid Engine rerun, collect qacct again, summarize, inspect the new
evidence, and record a new review decision. Increment `--generation` for each
audited resubmission generation.

## 12. Common problems

| Symptom | Meaning | Correct action |
|---|---|---|
| `No such file or directory: /path/to/...whl` | A documentation placeholder was copied literally | Find the real wheel and use its complete absolute path |
| venv reports Python 3.6/3.7 | Unsupported interpreter | Exit it, load/install Python 3.10+, and create a new venv |
| `TOOL_MISSING` | A native executable is absent from the job environment | Load the required module or configure the absolute executable under `binaries` |
| `RESOURCE_MISSING` | A configured input/reference path is wrong or unreadable | Correct the YAML path and permissions; rerun preflight |
| `RESOURCE_SHA256_MISMATCH` | A pinned file changed | Restore the intended resource or deliberately update the pinned digest after review |
| `REMOTE_GENOME_RESOURCE` | The genome definition points to online content | Create/use a fully local genome definition |
| `R_PREPARE_FAILED` with missing RDS/AG/SNP | The association row does not match the configured RDS structure | Check filename template, strand token, AG key, SNP column, sample-ID and ratio columns |
| R preparation reaches `TimeoutExpired` | `timeouts.r_prepare_seconds` is shorter than cohort preparation | Increase it for the full cohort; the packaged default is 129600 seconds (36 hours) |
| sample maps to multiple BAM paths | BAM lookup is ambiguous | Keep one authoritative path per sample ID |
| BAI missing/stale | Index is absent or older than BAM | Create/refresh the index or apply the documented warning policy |
| Grid Engine mode requires explicit action | Submission is intentionally fail-closed | Use exactly one of `--dry-run` or `--submit` |
| case remains `REVIEW_PENDING` | Automated QC passed; human review is incomplete | Inspect the figure and record approve/reject |
| case is `RERUN` | Execution/QC failed or a reviewer rejected it | Use `rerun_manifest.tsv` with `resume` |

When diagnosing a case, start with its state JSON and latest attempt directory
under the run root. Preserve logs and provenance; do not overwrite evidence to
make a failure disappear.

## 13. Reproducibility checklist

For another user to reproduce the same or closely equivalent results, record:

- package version, source commit, and wheel SHA-256;
- complete YAML configuration and its SHA-256;
- association/input/resource SHA-256 values;
- Python/R/IGV/Java/native-tool versions;
- Grid Engine job IDs and frozen qacct evidence;
- case manifest and task-map identities;
- final artifact checksums;
- reviewer identity and explicit review decisions.

Exact pixel reproduction additionally requires the same IGV version, fonts,
screen geometry, desktop settings, OCR settings, and local resources.

## 14. Advanced: artifact-first shared runtime

Cluster administrators may stage an immutable shared runtime with the packaged
script. It accepts a prebuilt wheel, refuses to overwrite an existing runtime,
and writes `READY` only after package/CLI validation.

Online staging:

```bash
export IGV_PACKAGE_ARTIFACT=/absolute/path/to/igv_snapshot_workflow-1.0.1-py3-none-any.whl
export IGV_RUNTIME_ROOT=/absolute/path/to/new_runtime
bash scripts/01_stage_runtime.sh
```

Offline staging:

```bash
export IGV_PACKAGE_ARTIFACT=/absolute/path/to/wheelhouse/igv_snapshot_workflow-1.0.1-py3-none-any.whl
export IGV_WHEELHOUSE=/absolute/path/to/wheelhouse
export IGV_RUNTIME_ROOT=/absolute/path/to/new_runtime
bash scripts/01_stage_runtime.sh
```

The runtime path must be final because console-script shebangs embed the venv's
absolute path.

## 15. Advanced: build and test from source

Ordinary users should install a released wheel. Developers building from source
must use the pinned build environment:

```bash
python3 -m venv .build-venv
source .build-venv/bin/activate
python -m pip install -r requirements-build.lock
python -m build --no-isolation
```

Run the checks:

```bash
python -m unittest discover -s tests -p 'test_*.py'
bash -n scripts/*.sh
```

The source distribution includes lock files, configuration, scripts, tests, and
package resources. The wheel contains only import-package code and declared
resources. No BAM/BAI/RDS data, screenshots, annotation assets, credentials, or
site-specific scheduler policy are bundled.

## License

MIT License. See `LICENSE`.
