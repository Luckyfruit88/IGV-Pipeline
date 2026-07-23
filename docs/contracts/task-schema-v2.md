# Canonical Contracts v2

Schema version `2.0` is a breaking contract for the Nextflow migration. The
canonical task JSONL is the internal scientific source of truth after
normalization. Downstream processes do not reread the association table, RDS
inputs, BAM lookup table, or user parameter file.

## Schemas

| File | Contract |
|---|---|
| `schema/task.schema.json` | one canonical task and all result-changing input identities |
| `schema/stage-result.schema.json` | one always-emitted case-stage bundle result |
| `schema/case-result.schema.json` | independent workflow/evidence/human/publication dimensions |
| `schema/review.schema.json` | append-only human artifact and scientific decision |
| `schema/shard-ledger.schema.json` | immutable shard control event |
| `schema/run-provenance.schema.json` | run software/reference/session/accounting identity |

All schemas use JSON Schema Draft 2020-12. Python cross-field validators in
`ssqtl_igv.contracts` add invariants that JSON Schema alone cannot express,
including staged-name uniqueness, exact genotype track counts, contiguous
manifest order, and publication/review state coupling.

## Path and cache rule

A source path in task JSON is provenance metadata. For a `READY` task, the
`RUN_SHARD` channel materializer must also convert every BAM, BAI, genome
definition component, annotation, and violin PDF into an actual Nextflow
`path` input. The process creates a task-local genome definition that refers to
the staged resources. JSON paths alone never satisfy the dependency contract.

`CASE_INPUT_INVALID` retains the canonical task ID and error evidence but is
routed to a structured terminal case result without asking Nextflow to stage a
known-missing source file. If a source disappears after a `READY` task was
frozen, staging failure is infrastructure drift and fails the shard.

## Independent case states

| Dimension | Values |
|---|---|
| Render | `PENDING`, `SUCCEEDED`, `FAILED` |
| Evidence | `COMPLETE`, `EVIDENCE_INCOMPLETE`, `UNAVAILABLE` |
| Artifact review | `REVIEW_PENDING`, `APPROVE`, `REJECT` |
| Scientific interpretation | `PENDING`, `SUPPORTED`, `NOT_SUPPORTED`, `INDETERMINATE` |
| Publication | `NOT_READY`, `READY`, `PUBLISHED`, `WITHHELD` |

An empty genotype group remains a `READY` task when its other inputs are valid.
It produces `EVIDENCE_INCOMPLETE`, is not automatically rerun, and may only
remain `PENDING` or receive `INDETERMINATE` scientific interpretation under the
frozen policy. Artifact approval remains an independent human judgment.

`READY` or `PUBLISHED` requires artifact `APPROVE` and a non-pending scientific
interpretation. These states mean a complete reviewed evidence package; they
do not mean the biological claim is supported.

## Always-emitted bundles

Each case-stage process declares one fixed bundle directory. The directory
always contains a schema-valid stage result; success-only artifacts live
inside it. A domain failure writes `DOMAIN_FAILED` plus failure records and
exits zero. Missing optional Nextflow outputs are not used as state signals.
Infrastructure failure may prevent a valid bundle and exits non-zero.

## Ordering and exact-set rules

- `task_id` is immutable and unique.
- `manifest_order` is unique and contiguous from 1 through the expected count.
- Shard union equals the canonical task set exactly.
- Final tables are sorted by `manifest_order`, never filesystem enumeration or
  scheduler completion order.
- A mixed schema version, duplicate ID/order, missing task, or unexpected task
  fails closed.
