# IGV 快照工作流 2.x 旧版用户指南

[Current v3 English](../README.md) | [当前 v3 中文](../README.zh-CN.md)

> 仅供历史参考。在 v3 安装中，这套冻结的 2.x 接口应通过 `ssqtl-igv` 调用；`igv-snapshot` 与 `igv-snapshot-workflow` 已使用新的 schema 3.0 接口。

冻结的 `ssqtl-igv` 接口是一个可配置的 Python/R pipeline，用于将关联分析记录和样本级测序数据转换为可审阅的原生 IGV 证据图。本指南同时面向首次使用者和在 Linux 集群上运行大规模任务的操作人员。

本 package 不包含私有科学数据、参考资源、IGV 或集群凭证。使用者需要通过 YAML 配置文件提供这些内容。

v2 唯一命令为 `ssqtl-igv`。`igv-snapshot` 与 `igv-snapshot-workflow` 都调用 v3，不能用于本指南。

## 1. Full workflow 的作用

对于关联表中的每一行，工作流会：

1. 识别染色体和链方向；
2. 打开相应的 RDS 对象并提取目标 AG site 表；
3. 保留目标 SNP genotype 和 ratio 值；
4. 从已有 genotype group 中选择代表性样本；
5. 将 sample ID 映射到本地 BAM 和 BAI 文件；
6. 启动隔离的 Xvfb desktop 和原生 IGV client；
7. 加载本地 genome、annotation、event、BAM、coverage、sequence 和 alignment tracks；
8. 等待 IGV 窗口、toolbar/locus 区域和渲染像素稳定；
9. 捕获完整的原生 IGV client 窗口；
10. 定位匹配的 violin plot，并将其放在未改动的 IGV 图像右侧；
11. 执行机械 QC 和科学 QC；
12. 停在 `REVIEW_PENDING`，等待明确的人工批准；
13. 发布已批准的 case，或为被拒绝/失败的 case 创建 rerun manifest。

每个 case 的状态流如下：

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

任何执行/QC 失败 -> FAILED -> RERUN
任何人工拒绝    -> RERUN
```

这是一个闭环工作流：仅仅完成执行并不能证明科学结果有效。自动 QC 提供证据，最终视觉判断由审阅者作出。

## 2. 确定运行位置和执行模式

支持的平台是 POSIX/Linux，Python 版本必须为 3.10 或更高。

请选择一种执行模式：

- `local`：在当前 session 中串行处理 case。适合小规模验证，或已经具备全部原生依赖的 workstation。
- `grid_engine`：创建有界 Grid Engine arrays，通过 `qsub` 提交，并使用 `qacct` 验证 accounting。推荐用于大规模 full run。

Wheel 只安装 Python package、Pillow 和 PyYAML。以下原生工具必须预先可用：

- R 和 `Rscript`；
- IGV 和 Java；
- Xvfb、`xwininfo` 和 `xprop`；
- ImageMagick `import`；
- Poppler `pdftotext` 和 `pdftoppm`；
- Tesseract 及配置指定的 OCR 语言；
- Grid Engine 模式需要 `qsub` 和 `qacct`。

不需要图形 workstation。IGV 会在 Linux compute node 的 Xvfb 中运行。

## 3. 安装并验证 package

### 3.1 创建干净的 Python 环境

不要使用 Python 3.6/3.7 环境。创建 venv 前先确认解释器版本：

```bash
python3 --version
python3 -m venv "$HOME/venvs/igv-snapshot-workflow-1.0.1"
source "$HOME/venvs/igv-snapshot-workflow-1.0.1/bin/activate"
python --version
```

### 3.2 定位并安装 wheel

`/path/to/file.whl` 之类的路径只是占位符。必须将其替换为 `find` 返回或管理员提供的真实绝对路径：

```bash
find "$HOME" /your/shared/project/root \
  -name 'igv_snapshot_workflow-1.0.1-py3-none-any.whl' \
  -type f 2>/dev/null
```

然后安装该文件：

```bash
python -m pip install \
  /absolute/path/to/igv_snapshot_workflow-1.0.1-py3-none-any.whl
```

离线安装时，把应用 wheel 以及兼容的 Pillow、PyYAML wheels 放入同一个 wheelhouse：

```bash
python -m pip install --no-index --find-links /absolute/path/to/wheelhouse \
  /absolute/path/to/wheelhouse/igv_snapshot_workflow-1.0.1-py3-none-any.whl
```

### 3.3 验证安装

```bash
python -m pip check
python -m pip show igv-snapshot-workflow
command -v ssqtl-igv
ssqtl-igv --help
ssqtl-igv --help
```

`pip check` 必须报告没有损坏的依赖关系，并且两个 CLI 命令都必须能正常显示帮助。

## 4. 准备科学输入

只有 BAM 文件并不足以完成运行。一次完整运行需要以下全部输入。

### 4.1 Association table

默认 CSV 列为：

```csv
AG_site,SNP,strand
chr13:112881814-112881815,chr13.112881818_T.C,+
chr13:45340822-45340821,chr13.45340822_A.G,-
```

列名可以通过 `inputs.association_columns` 配置。标准化后的每个 AG-site/SNP pair 必须唯一。

### 4.2 RDS 目录

默认情况下，工作流使用染色体和链方向按以下模板选择一个 RDS 文件：

```text
AGratio_SNPgeno_{strand_token}_{chrom}_list.rds
```

每个 RDS 必须包含一个 named list，并满足：

- AG site 恰好匹配一个 list entry；
- 该 entry 包含目标 SNP 列；
- 存在配置指定的 sample-ID 列；
- 存在配置指定的 ratio 列。

如果数据使用不同名称，请调整 `inputs.rds_filename_template`、`locus_sample_columns` 和 `ratio_column`。

### 4.3 BAM lookup 和 BAI 文件

创建一个 CSV，将每个 sample ID 映射到一个 BAM 文件或 BAM 目录。例如：

```csv
sample_id,bam_path
sample_001,/absolute/path/to/sample_001.bam
sample_002,/absolute/path/to/sample_002.bam
```

允许的 ID/path 列名和 BAM 后缀均可配置。要求如下：

- sample ID 必须与 RDS 中的 sample ID 一致；
- 一个 sample ID 不能映射到多个路径；
- 每个被选择的 BAM 必须存在且可读；
- 每个被选择的 BAM 必须有可读的 `.bai` 或其他受支持的 BAI；
- 使用 `inputs.stale_bai_policy` 决定比 BAM 更旧的 BAI 是 warning 还是 failure。

### 4.4 Violin PDFs

配置的 violin 目录必须包含文件名符合 `paths.violin_pdf_template` 的 PDF。PDF 文本必须能让工作流识别准确的 AG-site/SNP pair；名称相似的图不能视为精确匹配。

### 4.5 本地 genome 和 annotation 资源

请提供以下本地路径：

- IGV genome definition；
- reference FASTA 和 FAI；
- cytoband 文件；
- annotation GFF/GTF 资源。

Genome definition 不得将 IGV 重定向到远程 genome/annotation 资源。可选 SHA-256 字段可以固定每个不可变资源的身份。

## 5. 创建并编辑配置

生成 package 自带的模板：

```bash
ssqtl-igv init-config --output workflow.yaml
```

除非确实要替换已有配置，否则不要使用 `--force`。

至少要替换以下部分中的每个 `/path/to/...`：

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

本地运行：

```yaml
execution:
  mode: local
```

Grid Engine 运行：

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

`environment.venv` 路径必须对所有 compute node 可见。Scheduler 参数由站点决定；在确认集群政策前，不要照搬 memory、runtime、project 或 concurrency 设置。

可以使用以下 fail-closed 绑定：

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

使用 `sha256sum /absolute/path/to/file` 获取 digest。将 SHA 字段保留为 `null` 会停用相应身份检查。

## 6. 在任何运行前检查环境

先运行独立 preflight：

```bash
ssqtl-igv preflight --config workflow.yaml
```

Preflight 会检查：

- 必需的 executable；
- Pillow 和 PyYAML；
- 配置的文件/目录及可选 hash；
- local-only genome definition；
- output/publish 上级目录是否可写；
- 存储容量和 inode 要求；
- `execution.mode: grid_engine` 下的 Grid Engine 工具。

出现 fatal issue 后不要继续。修复配置或环境，并重复运行 preflight，直到通过。

## 7. 运行隔离的 smoke test

CLI 有意不提供专用 `smoke` 子命令。Smoke test 是使用小型、有代表性的 association table 和完全独立输出位置执行的一次普通运行。

### 7.1 选择代表性 case

创建 `smoke_associations.csv`，保留原始 header，并加入约 4–10 行。在数据允许时，应包含：

- 至少一个正链 case；
- 至少一个负链 case；
- 一个包含全部 genotype group 的 case；
- 一个样本较少或某个 genotype group 为空的 case；
- 一个 coverage 较高的 case。

不要直接编辑或截短 production association 文件。

### 7.2 创建隔离的 smoke 配置

```bash
cp workflow.yaml workflow.smoke.yaml
```

只修改 smoke 专用输入和输出：

```yaml
paths:
  associations: /absolute/path/to/smoke_associations.csv
  associations_sha256: null
  output_root: /absolute/path/to/smoke_runs
  publish_root: /absolute/path/to/smoke_review

inputs:
  expected_case_count: 4
```

Smoke evidence 绝不能复用 production run root 或 publish root。

### 7.3 规划并执行 smoke test

Grid Engine 模式下，先创建 audited plan，不调用 `qsub`：

```bash
SMOKE_RUN_ROOT=/absolute/path/to/smoke_runs/smoke_001

ssqtl-igv run \
  --config workflow.smoke.yaml \
  --run-root "$SMOKE_RUN_ROOT" \
  --dry-run
```

确认预期 case 数、task map、资源和有界 array plan。然后使用相同配置和 run root 提交：

```bash
ssqtl-igv run \
  --config workflow.smoke.yaml \
  --run-root "$SMOKE_RUN_ROOT" \
  --submit
```

Local 模式下串行运行，不加 `--dry-run` 或 `--submit`：

```bash
ssqtl-igv run \
  --config workflow.smoke.yaml \
  --run-root /absolute/path/to/smoke_runs/smoke_001
```

### 7.4 闭合 smoke test

Grid Engine 模式下，等待已提交 jobs 从 `qstat` 消失，然后运行：

```bash
ssqtl-igv collect-qacct \
  --config workflow.smoke.yaml \
  --run-root "$SMOKE_RUN_ROOT"
```

生成 review bundle 和状态文件：

```bash
ssqtl-igv summarize \
  --config workflow.smoke.yaml \
  --run-root "$SMOKE_RUN_ROOT"
```

成功 case 应处于 `REVIEW_PENDING`。按照第 10 节的验收清单检查每张图。如果仍有未解决的 IGV 启动、截图、locus、annotation、sample 或 plot matching 失败，不要启动大规模运行。

## 8. 规划并启动 full run

使用新的 production run root，不要复用 smoke run root。

```bash
RUN_ROOT=/absolute/path/to/workflow_runs/full_run_001
```

### 8.1 Grid Engine dry-run

`--dry-run` 会准备不可变输入 snapshot、构建 manifest 和 shards、执行 preflight，并创建 audited scheduler plan。它不会调用 `qsub`，也不会运行 IGV case。

```bash
ssqtl-igv run \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --dry-run
```

提交前验证：

- 配置了 `inputs.expected_case_count` 时，manifest 数量与其一致；
- 不存在重复或缺失的 case ID；
- 不存在 preparation failure；
- task ranges 对目标 case 恰好覆盖一次；
- 每个 array 不超过 `scheduler.max_tasks_per_array`；
- 请求的 memory/runtime/concurrency 符合集群政策；
- plan 指向预期的 wheel runtime 和配置。

### 8.2 提交已审计的 plan

使用相同的配置和 run root：

```bash
ssqtl-igv run \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --submit
```

Scheduler 使用有界 arrays，以 `-tc` 作为纵深保护，通过 `-hold_jid` 串联 arrays，并使 summary job hold 在最后一个 array 上。

不要仅因为 `qsub` 命令超时或返回不明确响应就再次提交。先检查 Grid Engine 和生成的 `jobs.json`，避免重复计算。

### 8.3 Local full run

Local 模式会串行处理尚未完成的 case：

```bash
ssqtl-igv run \
  --config workflow.yaml \
  --run-root "$RUN_ROOT"
```

## 9. 收集 scheduler 证据并汇总

Grid Engine 使用者必须等待所有记录的 arrays 完成，然后冻结 accounting 证据：

```bash
ssqtl-igv collect-qacct \
  --config workflow.yaml \
  --run-root "$RUN_ROOT"
```

该命令将 qacct 记录与已记录的 job ID、owner、job name、project、task ranges、exit status 和配置的 concurrency 绑定。

然后执行汇总：

```bash
ssqtl-igv summarize \
  --config workflow.yaml \
  --run-root "$RUN_ROOT"
```

Run root 会获得以下可审计文件：

- `final_status.tsv`；
- `failure_report.tsv`；
- `rerun_manifest.tsv`；
- `provenance.json`；
- `telemetry.json`；
- checksum/identity metadata。

可审阅图像和样本表会按染色体组织，输出到配置的 `paths.publish_root`。第一次 summarize 通常会报告：

- 自动 QC 已通过但人工审阅未完成时为 `REVIEW_PENDING`；
- 一个或多个 case 失败或被拒绝时为 `RERUN_REQUIRED`；
- 只有记录所有必需批准后才为 `PUBLISHED`。

## 10. 审阅并验收结果

自动 PASS 不能取代视觉审阅。对于每个 `REVIEW_PENDING` case，必须确认：

- 左侧是完整的原生 IGV client 窗口；
- menu、toolbar、genome/chromosome/locus controls、ideogram、ruler、tracks、sequence 和 annotation 均可见且可读；
- 显示的 genome 和 locus 正确；
- BAM/BAI sample identity 和 genotype-group 顺序与样本表一致；
- 原生 coverage、read/alignment 和 junction structure 可判断；
- annotation/transcript 和链方向在科学上适当；
- AG-site reference context 可见；
- 可以判断 AG site 附近的 splice/junction 证据或其缺失；
- violin plot 标注的是完全相同的 AG-site/SNP pair；
- 不存在改变 IGV client 的 crop、overlay、外部 label、divider 或 resampling。

只有完成视觉检查后才批准 case：

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

拒绝不合格 case 时必须给出原因：

```bash
ssqtl-igv review \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --case-id CASE_ID \
  --decision reject \
  --reviewer REVIEWER_ID \
  --notes 'Describe the visible problem and required correction'
```

拒绝表示“需要 rerun”，不代表负面的生物学证据。

记录 review 后，再次运行 `summarize`：

```bash
ssqtl-igv summarize \
  --config workflow.yaml \
  --run-root "$RUN_ROOT"
```

只有当没有 case 停留在 `REVIEW_PENDING` 或 `RERUN` 时，闭环才完成。

## 11. Rerun 失败或被拒绝的 case

使用生成的 rerun manifest，不要手工编辑 case state 文件。

Grid Engine dry-run：

```bash
ssqtl-igv resume \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --rerun-manifest "$RUN_ROOT/rerun_manifest.tsv" \
  --dry-run \
  --generation 1
```

Grid Engine submit：

```bash
ssqtl-igv resume \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --rerun-manifest "$RUN_ROOT/rerun_manifest.tsv" \
  --submit \
  --generation 1
```

Local rerun：

```bash
ssqtl-igv resume \
  --config workflow.yaml \
  --run-root "$RUN_ROOT" \
  --rerun-manifest "$RUN_ROOT/rerun_manifest.tsv" \
  --local \
  --generation 1
```

Grid Engine rerun 后，再次收集 qacct、执行 summarize、检查新证据并记录新的 review decision。每一代 audited resubmission 都要递增 `--generation`。

## 12. 常见问题

| 现象 | 含义 | 正确处理方法 |
|---|---|---|
| `No such file or directory: /path/to/...whl` | 原样复制了文档占位符 | 找到真实 wheel，并使用完整绝对路径 |
| venv 显示 Python 3.6/3.7 | 不受支持的解释器 | 退出该环境，加载/安装 Python 3.10+，并创建新 venv |
| `TOOL_MISSING` | Job 环境缺少原生 executable | 加载所需 module，或在 `binaries` 下配置绝对 executable 路径 |
| `RESOURCE_MISSING` | 配置的输入/参考路径错误或不可读 | 修正 YAML 路径和权限，并重新运行 preflight |
| `RESOURCE_SHA256_MISMATCH` | 被固定身份的文件发生变化 | 恢复预期资源，或在审阅后有意更新 digest |
| `REMOTE_GENOME_RESOURCE` | Genome definition 指向在线内容 | 创建或使用完全本地的 genome definition |
| `R_PREPARE_FAILED` 且缺少 RDS/AG/SNP | Association row 与配置的 RDS 结构不匹配 | 检查文件名模板、strand token、AG key、SNP 列、sample-ID 和 ratio 列 |
| R preparation 出现 `TimeoutExpired` | `timeouts.r_prepare_seconds` 短于完整 cohort 的准备时间 | 按完整 cohort 调高；package 默认值为 129600 秒（36 小时） |
| Sample 映射到多个 BAM 路径 | BAM lookup 存在歧义 | 每个 sample ID 只保留一个权威路径 |
| BAI 缺失或过旧 | Index 不存在或比 BAM 更旧 | 创建/更新 index，或采用文档规定的 warning policy |
| Grid Engine 模式要求显式 action | 提交被设计为 fail-closed | `--dry-run` 和 `--submit` 必须且只能选择一个 |
| Case 保持 `REVIEW_PENDING` | 自动 QC 已通过，人工审阅尚未完成 | 检查图像并记录 approve/reject |
| Case 为 `RERUN` | 执行/QC 失败，或审阅者将其拒绝 | 将 `rerun_manifest.tsv` 与 `resume` 一起使用 |

诊断 case 时，先检查其 state JSON 和 run root 下最新的 attempt 目录。保留 log 和 provenance；不要为了让失败消失而覆盖证据。

## 13. 可复现性清单

为了让其他使用者复现相同或高度相似的结果，请记录：

- package version、source commit 和 wheel SHA-256；
- 完整 YAML 配置及其 SHA-256；
- association/input/resource SHA-256；
- Python/R/IGV/Java/native-tool 版本；
- Grid Engine job ID 和冻结的 qacct 证据；
- case manifest 和 task-map identity；
- 最终 artifact checksum；
- reviewer identity 和明确的 review decision。

如果要求像素级完全复现，还必须使用相同的 IGV version、font、screen geometry、desktop setting、OCR setting 和本地资源。

## 14. 高级用法：artifact-first shared runtime

集群管理员可以使用 package 自带的脚本部署不可变 shared runtime。该脚本接收预构建 wheel，拒绝覆盖已有 runtime，并且只有在 package/CLI 验证通过后才写入 `READY`。

在线部署：

```bash
export IGV_PACKAGE_ARTIFACT=/absolute/path/to/igv_snapshot_workflow-1.0.1-py3-none-any.whl
export IGV_RUNTIME_ROOT=/absolute/path/to/new_runtime
bash scripts/01_stage_runtime.sh
```

离线部署：

```bash
export IGV_PACKAGE_ARTIFACT=/absolute/path/to/wheelhouse/igv_snapshot_workflow-1.0.1-py3-none-any.whl
export IGV_WHEELHOUSE=/absolute/path/to/wheelhouse
export IGV_RUNTIME_ROOT=/absolute/path/to/new_runtime
bash scripts/01_stage_runtime.sh
```

Runtime path 必须是最终路径，因为 console-script shebang 会嵌入 venv 的绝对路径。

## 15. 高级用法：从源码构建和测试

普通使用者应安装正式发布的 wheel。从源码构建的开发者必须使用固定版本的 build environment：

```bash
python3 -m venv .build-venv
source .build-venv/bin/activate
python -m pip install -r requirements-build.lock
python -m build --no-isolation
```

运行检查：

```bash
python -m unittest discover -s tests -p 'test_*.py'
bash -n scripts/*.sh
```

Source distribution 包含 lock files、configuration、scripts、tests 和 package resources。Wheel 只包含 import-package code 和声明的资源。本项目不打包 BAM/BAI/RDS 数据、截图、annotation assets、凭证或站点专用 scheduler policy。

## 许可证

MIT License。详见 `LICENSE`。
