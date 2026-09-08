# Recovery, resource admission, and snapshot generations

The controller records execution attempts separately from product state. A failed
Nextflow launch cannot overwrite a completed product summary. `reconcile` derives
status from canonical tasks, terminal bundles, the snapshot index, checksums, and
trace lineage; stale or missing summaries are archived and rebuilt.

```bash
igv-snapshot reconcile --output /runs/batch-0002
python -m ssqtl_igv.benchmark_cli campaign reconcile \
  --campaign-dir /campaign --runs-dir /runs --output /collection
```

Campaign reconciliation reads the batch workspaces without rewriting them. It
retains the frozen master task count, maps local batch order back to master order,
and receives completed batches even when their old controller summary says
`INFRASTRUCTURE_FATAL`. Overlapping batch requests and output/input directories
are rejected. Producers using the launcher hold a lease; reconciliation defers
an active producer. Legacy workspaces should be quiescent before import.
When a batch has undergone an authorized failed-only rerun, the collection checks
the frozen rerun receipt, registered generation, and projected case/bundle hashes
before replacing its old failed entry. Already successful entries remain immutable.

The large master manifest is verified once per reconciliation and reduced to an
identity index. Accepted batches have checksum-bound receipts, so unchanged
batches do not repeatedly read/render their payloads. Lost receipts can be rebuilt
from retained evidence. The complete snapshot tree is verified before a newly
completed collection is committed.

JSON Schema definitions are compiled once per distinct content in a bounded cache.
Every task and terminal document is still validated. The schema file is read on
each call, so replacing a schema at the same path immediately changes validation.

| Exit | Status | Meaning |
|---|---|---|
| 0 | `SNAPSHOTS_READY` | All expected tasks have eligible images. |
| 2 | `CASE_FAILURES` | Every expected task is accounted for, with terminal failures. |
| 1 | `INCOMPLETE` or `INFRASTRUCTURE_FATAL` | Work/evidence is missing or a control operation failed. |

`PASS_WITH_INCOMPLETE_EVIDENCE` and scientific review remain separate from image
generation. Reconciliation does not change those evidence labels.

A batch controller can call the same reconciler automatically after its run:

```bash
python -m ssqtl_igv.benchmark_cli campaign run-batch \
  --batch-request /campaign/batches/batch-0002/batch-request.json \
  --output /runs/batch-0002 \
  --campaign-runs /runs --campaign-output /collection
```

The paths must be mounted in the runtime. A pending sibling batch does not turn
a successfully executed batch into a scheduler failure. An interrupted collection
can be reconciled again without starting IGV.

## Resource admission

`conf/execution.config` calls the shared policy resolver before the executor is
constructed. SCC's allocation envelope determines the executor CPU/memory pool
and concurrency limit. The DAG materializes that exact policy instead of making a
second, potentially different resource decision. Retry memory is capped at the
allocation's usable memory, and Nextflow admits concurrent attempts by their
individual memory requirements. JVM and numerical-library thread limits follow
the worker CPU request.

Host observations are retained in `execution_policy.json`; only render and
normalization decisions enter worker cache identities. Moving to another node
with the same effective render policy does not invalidate completed worker output.
SGE remains responsible for allocating separate chunks; local executor requests
do not replace the site's operating-system resource isolation.

## Native locus text probe

The runtime build compiles `LocusClipboardProbe.java` with a checksum-pinned,
build-only JDK. The compiler is removed from the image layer. Production calls a
precompiled, checksummed class through the pinned IGV JRE, with bounded heap and
CPU use. No runtime `javac` or PATH-based Java lookup is needed.

The image self-test starts an isolated Xvfb and reads the complete value of a
clipped Swing field. Runtime verification still requires the exact normalized
chromosome and coordinates, while retaining the original screenshot and OCR
observations. OCR disagreement is not repaired by guessing a chromosome or
discarding coordinate digits.

## Snapshot storage protocol 3.1

Initial batch products remain readable as compact 3.0 products. Collections and
failed-only publication use `3.1-snapshot-store` under
`.igv-pipeline/snapshot-store/`:

- `objects/` contains immutable PNGs, copied and verified once.
- `generations/` contains complete views made with hard links, versioned tables,
  summaries, and metadata receipts.
- `CURRENT` is the sole atomic commit pointer. Earlier generations remain readable.

Root `snapshots/`, `snapshots.tsv`, `failed_cases.tsv`, and `run_summary.json` are
compatibility aliases. Code needing a consistent multi-file read must pin a view:

```python
from ssqtl_igv.snapshot_store import snapshot_view

generation = snapshot_view(output_root)
# Read both the index and its images under this same generation.
```

Legacy flat products are imported once. A durable bootstrap record lets the next
transaction finish an interrupted alias migration. No later commit moves the old
public image tree away. A process exit before or after the pointer update exposes
a complete old or new generation to version-aware readers. Uncommitted objects
and generations are safe to reuse after validation.

For tools requiring ordinary directories/files, export a pinned view into a new
destination. Execution caches and recovery controls remain in the source run.

```bash
igv-snapshot export-snapshots --output /collection --destination /delivery
```

This export performs one full payload copy and checksum verification, publishes
the directory atomically, and refuses to overwrite an existing destination.
