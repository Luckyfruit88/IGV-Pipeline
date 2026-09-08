# Historical SCC pilot

This directory preserves the original v3.0.0 maintainer experiment. It is not
part of installation, ordinary execution, or the release gate. The archived
workflow is documentation, not a registered GitHub Actions workflow.

Existing campaign data remains readable. To operate a historical campaign use
`python -m ssqtl_igv.benchmark_cli campaign ...` from a source checkout or an
explicit container Python entrypoint. Do not run this workflow before ordinary
`igv-snapshot run`. The frozen 8,973/100-case policy is deliberately not presented
as a general-purpose batching interface. Existing release assets are unchanged.

## Original protocol (historical, not current requirements)

#### Maintainer 100-case QA

The first release requires one formal BU SCC 100-case pilot using the same SIF
and single-node execution model. Selection spans the fixed chromosome × strand
strata and task complexity vector `(track_count, track_input_bytes,
overview_span_bp)`. The pilot checks exact 100-task Nextflow coverage, 100
screenshots and QC records, no silent loss, and the outer SGE job accounting.

The maintainer flow keeps execution and scientific state separate:

```text
campaign prepare-master  # Nextflow normalizes the 8,973-task master set
campaign run-batch       # Nextflow executes only pilot-001
```

`prepare-master` writes the immutable campaign contract and deterministic
100-task `batch-request`. The one-job helper accepts that request with
`--batch-request /absolute/campaign/batches/pilot-001/batch-request.json`.
The pilot SIF must be pulled from the immutable digest produced by the
`pilot-candidate-oci` workflow. After the pilot passes, the release workflow
promotes that same digest to `3.0.0`, `3.0`, and `latest` without rebuilding.

This pilot is a maintainer release check. It is not a normal user command, does
not add runtime parameters, and does not create a custom trust or key workflow.
Apple Silicon through amd64 emulation is usable but not yet tested as an
official platform; native ARM and Windows validation are deferred.


## 原始协议（历史记录，不是当前要求）

#### 维护者 100-case QA

首次发布前，在 BU SCC 用同一个 SIF 和单节点执行模式完成正式 100-case pilot。
任务选择覆盖固定 chromosome × strand strata，并使用复杂度向量
`(track_count, track_input_bytes, overview_span_bp)`。检查内容包括 Nextflow 精确
覆盖 100 个 task、100 份截图与 QC、零静默丢失，以及外层 SGE job accounting。

维护者流程保持 execution state 与 scientific state 分离：

```text
campaign prepare-master  # 由 Nextflow 生成 8,973-task master set
campaign run-batch       # Nextflow 只执行 pilot-001
```

`prepare-master` 冻结 campaign contract 和确定性的 100-task `batch-request`。
单 job helper 通过
`--batch-request /absolute/campaign/batches/pilot-001/batch-request.json`
执行该批次。pilot SIF 必须从 `pilot-candidate-oci` workflow 产出的 immutable
digest 转换；pilot 通过后，release workflow 只把同一个 digest 提升为
`3.0.0`、`3.0` 和 `latest`，不重新构建。

这是维护者 release check，不是普通用户命令，不增加 runtime 参数，也不创建
自定义密钥或信任流程。Apple Silicon 可通过 amd64 仿真使用，但暂未作为官方
验证平台；native ARM 和 Windows 验证延期。
