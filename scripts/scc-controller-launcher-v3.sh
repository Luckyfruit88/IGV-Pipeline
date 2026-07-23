#!/usr/bin/env bash
set -euo pipefail

cat >&2 <<'EOF'
The distributed host-Nextflow/SGE controller mode is deferred and is not
available in the IGV Pipeline 3.0 pull-only release.

Use scripts/submit-bu-scc-pull-run.sh to submit one outer SGE allocation. The
OCI-derived SIF then runs Nextflow's local executor inside that allocation.
EOF
exit 64
