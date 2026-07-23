# IGV Pipeline 3.0

IGV Pipeline turns local metadata and BAM/BAI files into reproducible IGV
Desktop screenshots and mechanical QC. Nextflow provides execution, cache, and
resume; the container provides IGV 2.16.2, Xvfb, fonts, OCR, image tools, and
both required Java runtimes.

> Release status: `v3.0.0` and its public GHCR image are published only after
> the maintainer BU SCC 100-case QA run passes. The commands below define the
> release interface; before that release, use a locally built image tag.

## English

### Quick Start

#### 1. Create a project

After the image is released:

```bash
mkdir igv-demo
cd igv-demo
mkdir output

docker pull ghcr.io/luckyfruit88/igv-pipeline:3.0.0
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  --network none \
  --mount type=bind,src="$PWD",dst=/output \
  ghcr.io/luckyfruit88/igv-pipeline:3.0.0 \
  init --adapter generic --output /output/project
```

`init` also accepts `--adapter ssqtl`. It creates a complete project template;
replace the placeholder reference resources and metadata with your local data.

The generic entry file is `project/project.yaml`:

```yaml
schema_version: "3.0"
adapter: generic
inputs:
  cases: cases.tsv
reference: reference.yaml
```

`cases.tsv` is UTF-8, tab-delimited, and has one BAM track per row:

```tsv
schema_version	case_id	locus	strand	bam	bai	track_label	group	aux_path	aux_page
3.0	case-001	chr1:100001-101000	+	data/case-001.bam	data/case-001.bam.bai	Sample 1	case	aux/plot.png	1
```

The ssQTL entry file is:

```yaml
schema_version: "3.0"
adapter: ssqtl
inputs:
  associations: associations.csv
  rds_dir: rds
  bam_lookup: bam_lookup.csv
  violin_dir: violin
  config: ssqtl.yaml  # optional
reference: reference.yaml
```

All project paths are relative to `project.yaml`. Absolute paths, URIs, globs,
backslashes, `..`, and symlink escapes are rejected. Runtime network access and
remote genome resources are not supported.

#### 2. Run with Docker

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  --network none \
  --mount type=bind,src="$PWD/project",dst=/project,readonly \
  --mount type=bind,src="$PWD/output",dst=/output \
  ghcr.io/luckyfruit88/igv-pipeline:3.0.0 run
```

No key, runtime sidecar, approval file, Docker socket, or nested container is
needed. The outer container runs Nextflow's local executor.

#### 3. Run with Apptainer or Singularity

```bash
apptainer pull igv-pipeline_3.0.0.sif \
  docker://ghcr.io/luckyfruit88/igv-pipeline:3.0.0

apptainer run --cleanenv --containall --no-home \
  --bind "$PWD/project:/project:ro,$PWD/output:/output" \
  igv-pipeline_3.0.0.sif run
```

For SingularityCE, replace `apptainer` with `singularity`.

#### 4. Read the results

```text
output/
├── results/cases/<task_id>/review.png
├── results/cases/<task_id>/raw/igv.png
├── results/cases/<task_id>/case_result.json
├── snapshots.tsv
├── failed_cases.tsv
├── run_summary.json
└── reports/trace.txt
```

Exit codes:

- `0`: every expected case is covered; screenshots and QC are ready and the
  run status is `SNAPSHOTS_READY`.
- `1`: project validation, input integrity, runtime self-test, or infrastructure
  failed before a trustworthy result set could be produced.
- `2`: one or more cases failed; successful screenshots are retained and
  `failed_cases.tsv` identifies rerun candidates.

`run_summary.json` is a convenience projection with `authoritative:false`.
Nextflow trace/cache and checksum-bound terminal case bundles remain the
execution evidence.

### Production Usage

The public CLI consists of:

```text
igv-snapshot init
igv-snapshot doctor
igv-snapshot run
igv-snapshot review
igv-snapshot publish
igv-snapshot import-v2
igv-snapshot campaign ...
```

Inside the OCI/SIF, the entrypoint accepts the command after `igv-snapshot`
directly, which is why the container examples use `run` and `init`.

The public `run` controls are intentionally small:

```text
--project /project/project.yaml
--output /output
--work /output/.work
--resume
--max-parallel auto|1..8
--max-cases-per-shard 1..256
```

`--max-parallel auto` resolves to the minimum of eight, available CPUs, and
available memory divided by 8 GiB. If memory cannot be observed reliably, it
uses one worker. Every render task requests one CPU, 8 GB, and 30 minutes.

To resume an interrupted run with the same project and runtime:

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  --network none \
  --mount type=bind,src="$PWD/project",dst=/project,readonly \
  --mount type=bind,src="$PWD/output",dst=/output \
  ghcr.io/luckyfruit88/igv-pipeline:3.0.0 run --resume
```

The embedded runtime manifest is validated automatically. Its fingerprint is a
real Nextflow input. Changes to metadata, reference files, render policy, or the
runtime prevent silent reuse of an incompatible cache. Outputs from an older
unreleased v3 candidate must use a new output directory.

For a read-only root filesystem, add:

```text
--read-only
--cap-drop=ALL
--security-opt=no-new-privileges
--tmpfs /tmp:rw,nosuid,nodev,size=1g
```

The project mount remains read-only, output is the only required writable bind,
and the entrypoint keeps HOME and Nextflow state below `output/.runtime`.

#### Optional review and publication

Snapshot generation does not require human review. To annotate cases in the
review UI, bind it only to `127.0.0.1`:

```bash
igv-snapshot review --output /output --reviewer USER_NAME
igv-snapshot review --output /output --finalize
```

Publication is optional and requires a finalized review. It verifies exact task
coverage, review decisions, case checksums, and the frozen runtime fingerprint,
then promotes a staging tree atomically without overwriting an existing
destination:

```bash
igv-snapshot publish --output /output --destination /path/to/new-destination
```

Campaign commands are an advanced scientific batching layer. They authorize
selections and later batches but never copy Nextflow task state.

### SCC Pilot Qualification

#### BU SCC single-job execution

The default SCC pattern is one outer SGE compute job. Inside that allocation,
the same SIF runs Nextflow's local executor with at most eight concurrent cases.
The compute node does not need registry access.

Pull the SIF once from a network-capable login node, place the project and
output on SCC storage. The repository includes a validated one-job helper and
an example site file:

```bash
scripts/submit-bu-scc-pull-run.sh \
  --site-config config/bu-scc-site.example.json \
  --sif /absolute/software/igv-pipeline_3.0.0.sif \
  --project-dir /absolute/project \
  --output-dir /absolute/output \
  --dry-run
```

Remove `--dry-run` after inspecting the generated `qsub` command, or submit a
job similar to:

```bash
#!/bin/bash -l
#$ -P YOUR_PROJECT
#$ -pe omp 8
#$ -l mem_per_core=8G
#$ -l h_rt=04:00:00
#$ -j y

apptainer run --cleanenv --containall --no-home \
  --bind "/absolute/project:/project:ro,/absolute/output:/output" \
  /absolute/software/igv-pipeline_3.0.0.sif \
  run --max-parallel 8
```

Add the site queue directive only when your project requires one. `qacct` is
optional operator accounting for the outer job; it does not gate screenshot
output. Distributed host-Nextflow/SGE execution is deferred from v3.0.0; its
legacy launcher fails closed instead of invoking removed CLI options.

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

### Developer Architecture

The source-of-truth boundary is deliberately narrow:

| Fact | Source of truth | Convenience output |
|---|---|---|
| Expected task set | Frozen canonical tasks and run contract | Summary counts |
| Task execution/cache | Nextflow trace/cache | Controller messages |
| Scheduler resource/exit | Raw qacct, when requested | Accounting summary |
| Case artifact/QC | Terminal bundle and `case_result.json` | `snapshots.tsv` |
| Scientific decision | Optional campaign ledger | Finalized review projection |
| Publication | Completion receipt and checksum tree | Staging directory |

The controlled object is the canonical task set. The goal is exact coverage
with reproducible screenshots and no silent loss. Observations are task/input
digests, the runtime fingerprint, Nextflow trace, terminal bundles, image/QC
hashes, and optional qacct/review/publication records. Control actions are
fail-closed validation, bounded concurrency, Nextflow resume, new-generation
rerun, optional human decisions, and atomic publication.

See [the control model](docs/architecture/control-model.md), [the runtime
contract](docs/runtime/portable-runtime-v3.md), and [the legacy v2 guide](docs/legacy-v2-user-guide.md).

## 中文 / Chinese

### 快速开始

#### 1. 创建项目

镜像发布后运行：

```bash
mkdir igv-demo
cd igv-demo
mkdir output

docker pull ghcr.io/luckyfruit88/igv-pipeline:3.0.0
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  --network none \
  --mount type=bind,src="$PWD",dst=/output \
  ghcr.io/luckyfruit88/igv-pipeline:3.0.0 \
  init --adapter generic --output /output/project
```

`init` 也支持 `--adapter ssqtl`。命令会生成完整项目模板；请用本地数据替换
其中的参考资源和 metadata。

Generic 的 `project/project.yaml`：

```yaml
schema_version: "3.0"
adapter: generic
inputs:
  cases: cases.tsv
reference: reference.yaml
```

`cases.tsv` 使用 UTF-8、制表符分隔，每行一个 BAM track：

```tsv
schema_version	case_id	locus	strand	bam	bai	track_label	group	aux_path	aux_page
3.0	case-001	chr1:100001-101000	+	data/case-001.bam	data/case-001.bam.bai	Sample 1	case	aux/plot.png	1
```

ssQTL 的项目入口：

```yaml
schema_version: "3.0"
adapter: ssqtl
inputs:
  associations: associations.csv
  rds_dir: rds
  bam_lookup: bam_lookup.csv
  violin_dir: violin
  config: ssqtl.yaml  # 可省略
reference: reference.yaml
```

所有路径均相对于 `project.yaml`。绝对路径、URI、glob、反斜杠、`..` 和
symlink 越界都会被拒绝。运行时不访问网络，也不支持远程 genome resource。

#### 2. 使用 Docker

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  --network none \
  --mount type=bind,src="$PWD/project",dst=/project,readonly \
  --mount type=bind,src="$PWD/output",dst=/output \
  ghcr.io/luckyfruit88/igv-pipeline:3.0.0 run
```

用户不需要密钥、runtime sidecar、批准文件、Docker socket 或嵌套容器。外层
容器直接使用 Nextflow local executor。

#### 3. 使用 Apptainer 或 Singularity

```bash
apptainer pull igv-pipeline_3.0.0.sif \
  docker://ghcr.io/luckyfruit88/igv-pipeline:3.0.0

apptainer run --cleanenv --containall --no-home \
  --bind "$PWD/project:/project:ro,$PWD/output:/output" \
  igv-pipeline_3.0.0.sif run
```

SingularityCE 用户只需把命令名 `apptainer` 替换为 `singularity`。

#### 4. 查看结果

```text
output/
├── results/cases/<task_id>/review.png
├── results/cases/<task_id>/raw/igv.png
├── results/cases/<task_id>/case_result.json
├── snapshots.tsv
├── failed_cases.tsv
├── run_summary.json
└── reports/trace.txt
```

退出码：

- `0`：全部预期 case 精确覆盖，截图与 QC 已完成，状态为
  `SNAPSHOTS_READY`。
- `1`：project、输入完整性、runtime self-test 或基础设施出现 fatal。
- `2`：部分 case 失败；成功截图会保留，`failed_cases.tsv` 给出 rerun 对象。

`run_summary.json` 是标记为 `authoritative:false` 的便利投影。执行事实仍来自
Nextflow trace/cache 和带 checksum 的 terminal case bundle。

### 生产使用

公共 CLI 包含：

```text
igv-snapshot init
igv-snapshot doctor
igv-snapshot run
igv-snapshot review
igv-snapshot publish
igv-snapshot import-v2
igv-snapshot campaign ...
```

OCI/SIF 的 entrypoint 可以直接接收 `igv-snapshot` 后面的命令，所以容器示例写作
`run` 和 `init`。

公开的 `run` 参数只保留：

```text
--project /project/project.yaml
--output /output
--work /output/.work
--resume
--max-parallel auto|1..8
--max-cases-per-shard 1..256
```

`--max-parallel auto` 取 8、可用 CPU 数和可用内存除以 8 GiB 三者的最小值；
无法可靠探测内存时使用 1。每个 render task 的初始资源为 1 CPU、8 GB、30 分钟。

相同项目和 runtime 的中断恢复：

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  --network none \
  --mount type=bind,src="$PWD/project",dst=/project,readonly \
  --mount type=bind,src="$PWD/output",dst=/output \
  ghcr.io/luckyfruit88/igv-pipeline:3.0.0 run --resume
```

镜像内置的 runtime manifest 会自动验证，其 fingerprint 是真实 Nextflow input。
metadata、reference、render policy 或 runtime 任一变化都不能静默复用不兼容
cache。旧的未发布 v3 candidate 输出必须改用新的 output 目录。

如需只读 rootfs，可额外加入：

```text
--read-only
--cap-drop=ALL
--security-opt=no-new-privileges
--tmpfs /tmp:rw,nosuid,nodev,size=1g
```

project mount 保持只读，output 是唯一必需的可写 bind；HOME 和 Nextflow 状态会
自动保存在 `output/.runtime`。

#### 可选审核与发布

生成截图不要求人工审核。如需科学标注，审核界面只监听 `127.0.0.1`：

```bash
igv-snapshot review --output /output --reviewer USER_NAME
igv-snapshot review --output /output --finalize
```

`publish` 是可选流程，要求已 finalized review。它会核验 task 精确覆盖、人工
决定、case checksum 和冻结的 runtime fingerprint，再以原子方式发布 staging
tree，且禁止覆盖已有 destination：

```bash
igv-snapshot publish --output /output --destination /path/to/new-destination
```

Campaign 命令只用于高级科学分批授权，不复制 Nextflow task state。

### SCC Pilot 验证

#### BU SCC 单 job 运行

BU SCC 默认只提交一个外层 SGE compute job；在 allocation 内，同一个 SIF 使用
Nextflow local executor，最多并发 8 个 case。compute node 不需要访问 registry。

先在可联网的 login node 拉取一次 SIF，把 project 和 output 放到 SCC storage，
仓库提供一个经过参数校验的单 job helper 和 site 配置示例：

```bash
scripts/submit-bu-scc-pull-run.sh \
  --site-config config/bu-scc-site.example.json \
  --sif /absolute/software/igv-pipeline_3.0.0.sif \
  --project-dir /absolute/project \
  --output-dir /absolute/output \
  --dry-run
```

检查生成的 `qsub` 命令后移除 `--dry-run`，也可以直接提交类似下面的脚本：

```bash
#!/bin/bash -l
#$ -P YOUR_PROJECT
#$ -pe omp 8
#$ -l mem_per_core=8G
#$ -l h_rt=04:00:00
#$ -j y

apptainer run --cleanenv --containall --no-home \
  --bind "/absolute/project:/project:ro,/absolute/output:/output" \
  /absolute/software/igv-pipeline_3.0.0.sif \
  run --max-parallel 8
```

只有 site 明确要求时才增加 queue。`qacct` 仅作为外层 job 的可选 operator
accounting，不阻塞截图输出。分布式宿主 Nextflow/SGE 执行在 v3.0.0 中延期；
旧 launcher 会明确 fail closed，不会调用已经删除的 CLI 参数。

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

### 开发者架构

唯一事实源边界如下：

| 内容 | 唯一事实源 | 便利输出 |
|---|---|---|
| 预期 task set | 冻结 canonical tasks 与 run contract | summary 计数 |
| Task execution/cache | Nextflow trace/cache | controller message |
| Scheduler resource/exit | 可选 raw qacct | accounting summary |
| Case artifact/QC | terminal bundle 与 `case_result.json` | `snapshots.tsv` |
| 科学决定 | 可选 campaign ledger | finalized review projection |
| Publication | completion receipt 与 checksum tree | staging directory |

受控对象是 canonical task set；目标是截图可复现、任务精确覆盖、零静默丢失。
可观测量包括 task/input digest、runtime fingerprint、Nextflow trace、terminal
bundle、图像/QC hash，以及可选 qacct/review/publication record。控制动作包括
fail-closed validation、有界并发、Nextflow resume、新 generation rerun、可选人工
决定和原子发布。

更多信息见[控制模型](docs/architecture/control-model.md)、[runtime contract](docs/runtime/portable-runtime-v3.md)和[旧 v2 指南](docs/legacy-v2-user-guide.zh-CN.md)。
