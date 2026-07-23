# Control model and sources of truth

IGV Pipeline 3.0 controls one primary transformation:

```text
project metadata -> canonical tasks -> IGV screenshots + mechanical QC
```

The controlled object is the immutable canonical task set for one run. The
desired terminal state is exact task coverage with a checksum-bound case result
for every expected task. A fully successful snapshot run reports
`SNAPSHOTS_READY`; human review and publication are optional downstream loops.

## State and authority

Each fact has one authoritative source. Projections may summarize those facts,
but they cannot replace them.

| Fact | Authoritative source | Non-authoritative projection |
|---|---|---|
| Expected task set | Frozen canonical tasks and run contract | `run_summary.json` counts |
| Task execution and cache | Nextflow trace/cache | Controller console messages |
| Scheduler resources and exit | Raw qacct, when an operator requests it | Accounting summary |
| Case artifact and QC | Nextflow terminal bundle and `case_result.json` | `snapshots.tsv` |
| Scientific or human decision | Campaign ledger, only when optional review is used | Finalized review projection |
| Publication | Verified publication-completion receipt and checksum tree | Staging directory |

`run_summary.json` is explicitly `authoritative:false`. The diagnostic control
journal does not carry task state, failed-case sets, scheduler state, or a
campaign phase. The controller reconstructs status from the authoritative
sources instead of maintaining a competing workflow state machine.

## Observability and control

The observable state is:

- the project and canonical task digests;
- the embedded runtime manifest and derived runtime fingerprint;
- Nextflow trace/cache and terminal bundles;
- raw and combined screenshot checksums plus QC fields;
- optional raw qacct, review ledger, and publication checksum tree.

The available control actions are input rejection, deterministic
normalization, bounded sharding, Nextflow concurrency/cache control, clean
rerun with a new generation, optional human decisions, and atomic publication.
The controller never edits a Nextflow task outcome or chooses trace over a
conflicting scheduler record.

## Feedback and correction

Input or runtime self-test failures stop before rendering. A per-case failure
retains successful cases, emits `failed_cases.tsv`, and exits `2`; correction
creates a new attempt or generation without rewriting prior evidence. Runtime
and render-policy changes alter the frozen fingerprint. Small control files
use content hashes, while large scientific inputs use the standard Nextflow
file identity of path, size, and nanosecond mtime for resume. Reference
resources and selected RDS/PDF inputs are additionally content-hashed in
canonical tasks. Replacing a large input while deliberately preserving both
size and mtime is outside that identity and requires a new output/work
directory (or an advanced mtime) before resume.

The system is stable under interruption because Nextflow owns execution and
resume, output promotion is atomic, and optional campaign locks protect only
short scientific-authorization transactions. Locks never span Nextflow,
scheduler waits, review sessions, or file transfer.

## Optional SCC and campaign loops

The pull-only BU SCC mode submits one outer compute-node allocation and runs the
same container with Nextflow's local executor inside it. Raw qacct describes
that outer allocation and is optional accounting evidence; it does not block
snapshot availability.

Campaign commands remain an advanced scientific batching layer. They freeze a
selection and authorize a next batch, but they do not record Nextflow process
attempts, cache state, native IDs, qacct exits, resources, or copied task
status. The 100-case BU SCC pilot is maintainer QA, not a user runtime state or
qualification receipt.
