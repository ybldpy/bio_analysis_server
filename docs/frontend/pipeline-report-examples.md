# 前端报告解析样例和 WSL 文件访问说明

更新日期：2026-10-21。

供前端 P4 报告解析与结果展示联调使用，包含短读／长读 QC JSON、AMR、VF、MLST、ECTyper 血清分型，以及空结果和未分型样例。样例来自既有真实工具运行，不是手写 mock，不需要重新执行流水线。

接口和结果字段约定见：[病毒和细菌流水线结果展示接入说明](./pipeline-results-visualization-guide.md)。

## 1 文件访问方式

### WSL 内访问

后端项目位于 WSL2，Linux 项目根目录为：

```text
/home/jcy/bio_analysis_server
```

下文所有样例路径均相对于这个项目根目录。

### Windows 同机访问

如果前端 agent 在同一台电脑的 Windows 环境运行，可以尝试通过以下 UNC 路径访问项目：

```text
\\wsl.localhost\<发行版名称>\home\jcy\bio_analysis_server
```

在 Windows PowerShell 中查询已安装的发行版：

```powershell
wsl --list --quiet
```

用后端项目实际所在的发行版名称替换 `<发行版名称>`，不要把占位符直接作为目录使用。存在多个发行版时，应确认哪个包含上述项目，不能默认任选一个。

下面示例演示如何得到短读 QC 文件的 Windows 路径：

```powershell
$reportProjectRoot = '\\wsl.localhost\<发行版名称>\home\jcy\bio_analysis_server'
$reportSampleRelative = 'target\final-pipeline-regression-YhRGovlP\rerun-SKvX7IkL\bacteria-attempt2\run-5473726166793917219\ecoli_mg1655_SRR1030394\storage\stageOutput\102\cleaned.json'
$reportSamplePath = Join-Path $reportProjectRoot $reportSampleRelative
Test-Path -LiteralPath $reportSamplePath
```

UNC 路径能否读取还取决于前端 agent 的文件访问权限。如果其运行环境不能访问 WSL，需要另行将样例复制到前端项目目录；如果不在同一台电脑，则需要打包传递。目前这些原始样例尚未复制到 Windows 目录。

这些文件位于 `target` 测试产物目录，可能被构建清理删除。前端正式依赖样例进行回归测试前，应另存到其测试 fixtures 目录；不要依赖测试运行目录长期存在。

## 2 主要报告样例

### 2.1 短读 QC JSON

工具：fastp **1.0.1**。输入为双端短读。

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/ecoli_mg1655_SRR1030394/storage/stageOutput/102/cleaned.json
```

主要结构：

```text
summary.fastp_version
summary.before_filtering
summary.after_filtering
filtering_result
duplication
insert_size
adapter_cutting
read1_before_filtering
read2_before_filtering
read1_after_filtering
read2_after_filtering
```

### 2.2 长读 QC JSON

工具：fastplong **0.4.1**。该报告由官方长读测试数据经过真实工具运行生成。

```text
target/qc-public-data-regression/run-14386567059686433537/bacteria_flye_ecoli_corrected/storage/stageOutput/2/qc.json
```

主要结构：

```text
summary.fastplong_version
summary.before_filtering
summary.after_filtering
filtering_result
adapter_cutting
read_before_filtering
read_after_filtering
```

短读与长读的解析差异：

| 内容 | 短读样例 | 长读样例 |
| --- | --- | --- |
| 工具版本字段 | `summary.fastp_version` | `summary.fastplong_version` |
| Reads 详细统计 | `read1_*`、`read2_*` | `read_*` |
| 过滤前后平均长度字段 | `read1_mean_length`、`read2_mean_length` | `read_mean_length` |
| 双端插入片段等字段 | 此双端样例包含 | 不应作为必填字段 |

`q20_rate`、`q30_rate`、`gc_content` 是 0–1 比例，展示百分比时乘以 100。字段缺失不表示数值为零。短读单端任务也不应被要求具有 R2 统计。

### 2.3 AMR 报告

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/ecoli_mg1655_SRR1030394/storage/stageOutput/105/amr.tsv
```

- 有表头的 TSV，22 列、1 条数据记录。
- 来自真实 AMRFinder 工具运行。
- 阶段结果中的文件字段是 `amrResult`。
- 按表头名称读取列，不按空格拆分表头或内容。

### 2.4 VF 报告

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/ecoli_mg1655_SRR1030394/storage/stageOutput/106/vf.tsv
```

- 有表头的 TSV，15 列、49 条数据记录。
- 来自真实 ABRicate 工具运行。
- 阶段结果中的文件字段是 `vfTsv`。
- 第一列表头 **`#FILE` 是有效字段名，不是注释**，不能丢弃整行。
- 文件末尾有空行，解析时忽略空行，但保留数据行内部的空字段。

### 2.5 MLST 报告

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/ecoli_mg1655_SRR1030394/storage/stageOutput/107/mlst.tsv
```

- **无表头**的 TSV，此样例有 10 列、1 条数据记录。
- 阶段结果中的文件字段是小写 `mlst`，该值是文件对象键，不是 ST 值。
- 不能把第一行当表头，也不能固定要求每份报告有 10 列；第 3 节提供了 3 列的未分型样例。
- 首列含本地输入路径，默认界面可只显示适当的文件名或样本标识。

### 2.6 ECTyper 血清分型报告

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/ecoli_mg1655_SRR1030394/storage/stageOutput/108/serotypeResult.tsv
```

- 有表头的 TSV，40 列、1 条数据记录。
- 来自真实 ECTyper 工具运行，阶段结果中的文件字段是 `serotypeResultUrl`。
- 此次不是 Kaptive 分支，`kaptiveKResultUrl`、`kaptiveOResultUrl` 均为空。

可核实的展示字段包括：

```text
Name
Species
O-type
H-type
Serotype
QC
Evidence
Warnings
DatabaseVer
```

根据实际表头名称定位列，不写死列号。这个样例只覆盖 ECTyper，不能据此推断其他血清分型工具的报告结构。

## 3 空结果和未分型样例

以下报告对应的工具执行成功，适合核实空态，不应显示成运行失败。

### AMR 只有表头

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/cglutamicum_DRR119686/storage/stageOutput/205/amr.tsv
```

22 列，仅有表头，没有数据记录。

### VF 只有表头

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/cglutamicum_DRR119686/storage/stageOutput/206/vf.tsv
```

15 列，仅有表头和尾部空行，没有数据记录。

### MLST 未分型

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/cglutamicum_DRR119686/storage/stageOutput/207/mlst.tsv
```

无表头，3 列、1 条数据记录。需要与正常分型结果分别处理，不能因为缺少后续等位基因列而直接报文件格式错误。

## 4 运行来源和样例边界

短读 QC、AMR、VF、MLST 和 ECTyper 的来源运行记录：

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/ecoli_mg1655_SRR1030394/report.json
```

空结果与未分型样例的来源运行记录：

```text
target/final-pipeline-regression-YhRGovlP/rerun-SKvX7IkL/bacteria-attempt2/run-5473726166793917219/cglutamicum_DRR119686/report.json
```

长读 QC 的来源运行记录：

```text
target/qc-public-data-regression/run-14386567059686433537/bacteria_flye_ecoli_corrected/report.json
```

这些 `report.json` 用于追溯测试来源和真实工具执行记录，**不是前端正式接口返回的统一报告格式**。前端应解析第 2、3 节列出的实际产物，不要拿测试汇总替代工具报告。

样例来自公开数据或官方测试数据，但部分文件包含本机路径和 `command` 字符串。目前原文件未脱敏；这些字段不应在页面默认展示，向其他环境传递时可只脱敏路径等环境信息并保留报告结构。

这些样例用于核实格式，不保证覆盖所有工具版本和所有业务分支。

## 5 尚未提供真实样例的分支

在已检查的既有测试产物范围内，尚未找到以下分支的真实输出样例：

- Kaptive K/O 原始及合并报告。
- SeqSero2 报告。
- SeroBA 报告。
- pFaster 报告。

这些分支先保留下载入口，不套用 ECTyper 解析器，也不把手写测试样例当作真实工具格式的证明。其他已核实模块可以继续开发，不需要等待这些分支。

## 6 前端解析和接入要求

1. TSV 按制表符解析，兼容换行格式，忽略尾部空行；保留数据行中的空字段。
2. 有表头的报告按列名取值，兼容新增列。MLST 按无表头格式单独处理。
3. “只有表头”“未分型”“下载失败”“阶段未执行”显示不同状态。
4. QC 按实际工具和字段适配，不给缺失字段补零。
5. 上述本地路径只用于开发核实。正式页面仍从阶段结果读取 `reportJSON`、`amrResult`、`vfTsv`、`mlst`、`serotypeResultUrl`，然后通过 `/stageOutput/download?outputUrl=<编码后的对象键>` 获取内容。
6. 不把这些 `target/...` 本地相对路径直接提交给生产文件接口，也不把它们写死到前端业务代码。
7. 报告文本作为文本渲染，不直接拼接成 HTML；本地路径和命令默认隐藏。
8. 暂未核实格式的报告保留下载入口，已核实的格式可以逐步增加表格和统计图。
