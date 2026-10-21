# 病毒和细菌流水线结果展示接入说明

更新日期：2026-10-21。

面向前端开发，覆盖病毒流水线（`pipelineType=100/101`）、常规细菌流水线（`pipelineType=200`）以及新冠污水独立入口（`pipelineType=102`，见第 11 节）的结果页面。页面需要根据实际阶段列表，读取内联结果或报告文件，展示运行状态、统计指标、分类结果和下载入口。

当前已有一部分结构化结果，可以直接制作图表；另一部分只有工具报告文件，需要增加相应格式的解析。本文分别说明，不把文件路径当成已经解析好的结果数据。

病毒任务创建与目标选择另见：[病毒流水线创建页面调整说明](./virus-pipeline-creation-guide.md)。

用于核实报告格式的真实样例、WSL/Windows 访问方式及空结果样例见：[前端报告解析样例和 WSL 文件访问说明](./pipeline-report-examples.md)。

## 1 页面范围和模块布局

结果页面建议包含四部分：任务总览、阶段结果、需要用户处理的事项、产物下载。

| 流水线 | 输入类型 | 当前阶段范围 |
| --- | --- | --- |
| 病毒 | Reads | 输入检查、QC、Assembly、参考选择、Mapping、Variant Call、Consensus、SNP 注释 |
| 病毒 | Assembly | 参考选择、参考比较 |
| 细菌 | Reads | 输入检查、QC、Assembly、物种分类、AMR、VF、MLST、血清分型 |
| 细菌 | Assembly | 物种分类、AMR、VF、MLST、血清分型 |

以上是结果模块范围，不表示所有阶段严格串行，也不表示每个阶段一定执行。细菌的分类、AMR、VF、MLST 可以并行；病毒的 Consensus 和 SNP 注释是变异检测之后的两个下游分支。

- 以接口实际返回的阶段列表为准；某些输入或条件下会省略或跳过阶段。
- 没有创建的阶段不显示为失败，也不能填充零值。
- 病毒 Assembly 输入没有 reads Mapping、Variant Call、Consensus 结果，不应显示为这些阶段异常缺失。
- 可选文件、旧版本指标和不适用阶段需要有空态。
- 当前 `stageIndex` 常用于标记入口，值可能为 `0/-1`，不是完整的展示顺序；按 `stageType` 和对应流程组织页面，不直接依赖接口返回顺序或 `stageIndex` 排序。

## 2 结果读取和文件获取

### 2.1 现有接口

| 接口 | 参数 | 用途 |
| --- | --- | --- |
| `GET /pipeline/getPipeline` | Query：`pipelineId` | 获取流水线名称、类型、整体状态等信息 |
| `GET /pipeline/getPipelineStages` | Query：`pipelineId` | 获取完整阶段列表及结果字段，结果详情页使用此接口 |
| `POST /pipeline/getPipelineStates` | JSON 数字数组，例如 `[123,124]` | 获取多条流水线的状态，适合列表或状态刷新 |
| `GET /stageOutput/download` | Query：`outputUrl`，值为单个文件对象键 | 下载产物，或读取 JSON、TSV 等报告内容 |
| `POST /pipeline/stage/taxonomy/confirm` | JSON：`stageId`、`taxId`、可选 `reason` | 对等待人工处理的分类结果进行确认 |

`getPipelineStages` 会加载 `outputInline` 等长文本字段。`getPipelineStates` 当前使用不带 BLOB 列的阶段查询，不加载 `outputInline`、`parameters`，不能用其结果判断“参考选择或物种分类没有产物”。

获取阶段详情的成功响应采用下面的包装；示例只保留理解数据层级所需的字段：

```json
{
  "status": 0,
  "data": [
    {
      "stageId": 901,
      "stageType": 20,
      "stageName": "有参比对 (Mapping)",
      "status": 4,
      "outputInline": null,
      "outputUrl": "{\"bamUrl\":\"stageOutput/901/result.bam\",\"bamIndexUrl\":\"stageOutput/901/result.bam.bai\",\"coverageDepthUrl\":\"stageOutput/901/coverage_depth.tsv\",\"metricsUrl\":\"stageOutput/901/mapping.metrics.json\"}"
    }
  ],
  "failMsg": null
}
```

外层 `status=0` 表示接口业务成功，不是阶段完成状态。HTTP 成功后仍需检查包装中的 `status`；HTTP 失败或参数校验失败时，也不能假定响应一定是此 JSON 结构。

### 2.2 outputInline 和 outputUrl 的区别

两个字段的 Java 类型都是字符串，内容为序列化 JSON，前端需要先解析。`outputUrl` 这个名字不代表其值可以直接作为下载地址。

| 阶段 | stageType | 当前正常结果存放字段 | 解析后获得什么 |
| --- | --- | --- | --- |
| 输入检查 | `80` | `outputUrl` | 输入元信息和文件对象键 |
| QC | `0` | `outputUrl` | 清洗文件与报告的对象键 |
| Assembly | `30` | `outputUrl` | Contigs 和可选 Scaffolds 对象键 |
| 参考选择 | `44` | `outputInline` | 选中参考、候选分数、未匹配节段等结构化结果 |
| Mapping | `20` | `outputUrl` | BAM、深度文件、指标 JSON 的对象键 |
| Variant Call | `40` | `outputUrl` | VCF、索引、指标 JSON 的对象键 |
| Consensus | `41` | `outputUrl` | FASTA、遮罩 BED、指标 JSON 的对象键 |
| 参考比较 | `43` | `outputUrl` | PAF 与差异 TSV 的对象键 |
| SNP 注释 | `72` | `outputUrl` | 注释 VCF 对象键 |
| 物种分类 | `10` | `outputInline` | 分类、候选、分箱指标以及部分报告对象键 |
| AMR | `60` | `outputUrl` | AMR 报告对象键 |
| VF | `61` | `outputUrl` | VF 报告对象键 |
| MLST | `62` | `outputUrl` | MLST 报告对象键 |
| 血清分型 | `64` | `outputUrl` | 分型报告对象键 |
| 新冠污水混合分析 | `92` | `outputUrl` | 报告、指标、来源记录、执行日志及可选 BAM 对象键 |

典型读取过程：

```text
参考选择 / 物种分类
stage.outputInline → 解析 JSON → 使用结构化结果展示

Mapping / Variant Call / Consensus
stage.outputUrl → 解析 JSON → 取得 metricsUrl
→ 请求文件接口 → 解析指标 JSON → 展示图表

AMR / VF / MLST / 血清分型
stage.outputUrl → 解析 JSON → 取得报告文件对象键
→ 提供下载；需要表格时，再按报告格式解析文件
```

补充规则：

- 参考选择存在历史记录兼容：`outputInline` 为空时，可以读取旧的 `outputUrl`。后端编排也有这一兼容逻辑。
- 物种分类当前以 `outputInline` 为正式读取来源；不要假设后端已经提供相同的 `outputUrl` 回退。
- 血清分型被跳过时，`outputInline` 可能是 `{"skipReason":"..."}`，不是正常分型报告。
- Taxonomy 超出分类范围时，`outputInline` 仍是分类结果，其中分类 `status=3`；不能把所有 `NOT_APPLICABLE` 的 inline 内容都当成 `skipReason` 对象。
- SNP 注释被跳过时不保证提供 `skipReason`。原因缺失时显示“不适用”，不要自行写成“没有变异”。
- 解析前处理 `null`、空字符串；坏 JSON 显示“结果解析失败”，不要回填成空结果或零命中。
- 新结果优先使用 `metricsUrl`。历史记录可能缺少这个字段，或使用 `qcUrl`、内联 `metrics/qcMetrics`；需要展示旧任务时单独做兼容，不依赖服务端自动改写数据库中的历史 JSON 字符串。

### 2.3 文件下载和指标 JSON

文件字段的值通常是下面这样的存储对象键：

```text
stageOutput/901/mapping.metrics.json
```

它不是浏览器可直接访问的 HTTP URL，也不是要求前端访问服务器本地路径。下载地址应由文件接口生成：

```javascript
function stageOutputUrl(fileKey) {
  if (typeof fileKey !== "string" || fileKey.trim() === "") {
    throw new Error("缺少文件对象键");
  }
  return `/stageOutput/download?outputUrl=${encodeURIComponent(fileKey)}`;
}

async function readMetricsJson(fileKey) {
  const response = await fetch(stageOutputUrl(fileKey));
  if (!response.ok) {
    throw new Error(`读取指标失败，HTTP ${response.status}`);
  }
  return await response.json();
}
```

示例使用同源相对地址；接入时沿用项目的 API 前缀、认证和请求封装。页面应捕获网络和 JSON 解析错误，提供重试。

- 下载成功返回文件内容，没有 `{status,data,failMsg}` 包装。
- 当前响应为 `application/octet-stream`，并带 `Content-Disposition: attachment`；JSON 文件仍可通过程序请求并解析。
- 参数传单个文件字段，例如 `metricsUrl`、`bamUrl`、`amrResult` 的值，不能传完整的 `stage.outputUrl` JSON。
- 文件字段命名并不统一以 `Url` 结尾，例如 `annotatedVCF`、`differenceTsv`、`mlst` 也保存对象键，应按本文的字段表读取。
- HTML 报告目前是下载附件；在线预览需另外适配，不能直接认为下载接口已经支持 iframe 预览。展示原始报告文本时也不要把内容直接拼接成页面 HTML。
- 当前缺文件错误未统一成明确的 404 响应，前端需处理网络错误、5xx 或非 JSON 错误体，不把下载失败显示成“未检出”。

## 3 可视化能力分层

### 3.1 现有 JSON 可以支持的展示

| 模块 | 可展示内容 | 取数方式 |
| --- | --- | --- |
| 阶段状态 | 进度、完成、失败、等待确认、跳过 | 阶段列表 |
| 输入信息 | 测序平台、长短读、质量编码、输入布局 | 输入检查结果的 `readMeta` 和 R1/R2 字段 |
| QC | 过滤前后 reads、保留比例、质量统计、过滤原因 | 获取 `reportJSON`，适配实际报告字段 |
| 病毒参考选择 | 选中参考表、候选比较、未匹配节段提示 | `outputInline` |
| 病毒 Mapping | 比对率、覆盖率、深度指标卡、分参考统计图 | 获取 `metricsUrl` |
| 病毒变异检测 | 变异类型、过滤状态、分参考数量 | 获取 `metricsUrl` |
| 病毒 Consensus | 深度遮罩与未遮罩比例、分参考统计 | 获取 `metricsUrl` |
| 细菌物种分类 | 分类卡片、候选比较、证据来源、人工确认记录 | `outputInline` |
| 细菌分箱 | Contig 数、碱基数、分箱组成图 | 分类结果内的 `binningMetrics` |

### 3.2 有文件但需要增加解析或统计的展示

| 展示目标 | 现有文件 | 额外处理 |
| --- | --- | --- |
| 逐位置深度曲线 | `coverageDepthUrl` | 解析深度 TSV，按参考序列分组；大文件需分段或降采样 |
| 变异明细表 | `vcfGzUrl` | 解压并解析 VCF；当前无逐条查询和分页接口 |
| 功能注释明细 | `annotatedVCF` | 解析 VCF 注释字段 |
| AMR / VF 命中表 | `amrResult` / `vfTsv` | 按报告表头解析 TSV |
| MLST 结果卡片 | `mlst` | 提取 scheme、ST、等位基因结果 |
| 血清分型结果卡片 | 分型 TXT/TSV | 按实际工具格式解析，不能共用固定列定义 |
| Assembly 数量和长度指标 | `contigsUrl` | 统计 FASTA；当前没有现成的 contig 数、N50 等汇总字段 |
| 参考比较图和差异表 | `pafPath` / `differenceTsv` | 解析并聚合 PAF / TSV |
| 最终 Consensus 的实际长度和 N 数 | `consensusUrl` | 扫描最终 FASTA；不能直接替代为遮罩 metrics |

第一版可先实现上一节的结构化展示，并为所有已发布文件提供下载。解析较大报告或需要统一格式时，宜由后端增加解析和分页能力；当前接口尚未提供这些能力。

## 4 公共阶段结果

### 4.1 输入检查

`stageType=80`，结果取自 `outputUrl`。

字段：`readMeta`、`r1Url`、`r2Url`。

| readMeta 字段 | 含义 |
| --- | --- |
| `qualityEncoding` | `0=Phred+33`，`1=Phred+64` |
| `readLenType` | `0=短读`，`1=长读` |
| `sequencingPlatform` | 平台编码；常用 `0=未知`、`1001=Illumina`、`1002=BGI/MGI`、`2001=ONT`、`2002=PacBio` |
| `sequenceLevel` | `-1=未知`、`0=Reads`、`1=Assembly`、`2=Reference` |

完整平台编码以 `Constants.SequencingPlatform` 为准；遇到未适配编码显示“未知平台”并保留原值，不根据长短读自行改写平台。元信息不提供 reads 数量、Q30 等质量统计，这些属于 QC 报告。

`r2Url` 为空时不要显示 R2 下载入口。布局标签按规范化后的 R1/R2 结果展示，不仅根据原上传文件数量判断。

### 4.2 QC

`stageType=0`，结果取自 `outputUrl`。

| 字段 | 文件用途 |
| --- | --- |
| `cleanedR1` | 清洗后的 R1 或单端 reads |
| `cleanedR2` | 清洗后的 R2，可为空 |
| `reportJSON` | 统计和图表数据来源 |
| `reportHTML` | 完整质量报告附件 |

可展示过滤前后 reads 数、碱基数、保留比例、Q20/Q30、长度及过滤原因。具体图表字段取决于实际工具报告；短读和长读分支使用的工具报告应分别适配，不假定每份报告都有全部字段。

以常见 fastp 报告为例，数据来自 `summary.before_filtering`、`summary.after_filtering`、`filtering_result`；这些是下载的报告内部字段，不是 `QcResult` 的顶层字段。缺少某项时显示“未提供”，不要填零。

### 4.3 Assembly

`stageType=30`，结果取自 `outputUrl`。

- `contigsUrl`：Contigs FASTA。
- `scaffoldUrl`：可选的 Scaffolds FASTA，字段名是单数 `scaffoldUrl`。

当前可以直接展示阶段状态和下载入口。没有 Scaffolds 不代表失败。Contig 数量、总长度、长度分布、N50、GC% 等需要另外统计，后端尚未通过该阶段结果提供这些字段。

## 5 病毒阶段结果

### 5.1 参考基因组选择

`stageType=44`，结果主要取自 `outputInline`。

| 顶层字段 | 展示用途 |
| --- | --- |
| `selectedReference` | 本次选择的参考集合 |
| `candidateScores` | 候选比较表和分数图 |
| `unmatchedSegmentKeys` | 本次候选范围内未选出参考的节段 |
| `partial` | 是否存在上述未匹配节段 |
| `readFallbackMetrics` | Reads 补充检查的状态和已使用配置，可折叠展示 |

选中参考使用 `selectedReference.taxId`、`organismName` 及 `sequences[]`。每条序列可展示 `accession`、`sourceDb`、`genomeLength`、`segment`、`segmentOrdinal`、`segmentKey`、`completeness`、`isAnnotated`。原始数据库元信息 `rawMetadata` 不作为默认展示内容。

候选比较字段：

| 字段 | 含义和单位 |
| --- | --- |
| `referenceAccession` | 与选中序列 `accession` 关联 |
| `segmentKey` | 用于节段分组 |
| `evidenceSource` | `CONTIGS` 或 `READS`，匹配证据来源 |
| `sequenceIdentity` | 已经是 0–100 的百分数，如 `97.2` 显示 `97.2%` |
| `referenceCoverage` | 0–1 比例，如 `0.95` 显示 `95%` |
| `alignedReferenceBases` / `referenceBases` | 已对齐参考位置数 / 参考长度，单位 bp |
| `matchingBases` / `alignmentBlockBases` | 匹配碱基数 / 比对块长度，单位 bp |
| `alignmentCount` | 比对记录数量 |
| `score` | 排序分数，不是统计置信概率 |
| `rejectionReason` | 若提供则展示未接受原因 |

候选没有独立的 `selected` 字段。用 `selectedReference.sequences[].accession` 构建集合，与 `candidateScores[].referenceAccession` 关联，渲染“已选中”标记。节段展示优先使用 `segmentOrdinal` 数值顺序，未知顺序保留名称，不对字符串做 `1,10,2` 式排序。

`readFallbackMetrics.status` 支持 `NO_READS`、`DISABLED`、`NOT_NEEDED`、`COMPLETED`、`UNSUPPORTED_READ_TYPE`。这些表示补充检查状态，不等于阶段失败；原始配置字段可在详情中按需展开。

重要解释边界：

- 参考名称表示使用的比对参考，不等于确定样本就是该参考菌株或分离株。
- `partial=false` 只表示本次候选分组中没有未匹配项，不是独立的全基因组完整性证明。
- `selectedReference.complete` 不是样本完整度；当前多序列聚合会得到 `false`，不能据此显示“缺节段”。
- `compositionStatus=UNKNOWN` 不表示已发现混样；当前多序列结果未据此确认是否来自同一分离株。
- 参考选择执行器的比较 TSV 当前没有通过结果 handler 发布下载对象键，不应生成不存在的下载入口。

### 5.2 Mapping

`stageType=20`，解析 `outputUrl` 后获取以下文件：

| 字段 | 产物 |
| --- | --- |
| `bamUrl` | BAM 比对文件 |
| `bamIndexUrl` | BAM 索引 |
| `coverageDepthUrl` | 逐位置深度 TSV |
| `metricsUrl` | Mapping 指标 JSON |

指标 JSON 的总体字段：

| 字段 | 建议标签 |
| --- | --- |
| `mappingFraction` | 比对率，0–1 比例 |
| `coverageFractionAt1x` | 至少 1× 覆盖率，0–1 比例 |
| `meanDepth` / `maxDepth` | 平均深度 / 最高深度，单位 × |
| `totalReferenceBases` | 参考总长度，bp |
| `coveredBases` / `zeroDepthBases` | 有覆盖 / 零深度位置数，bp |
| `mappedReadSegments` / `unmappedReadSegments` / `totalReadSegments` | 已比对 / 未比对 / 总 read segment 数 |
| `references` | 每条参考序列的独立统计 |

`references[]` 包含 `referenceName`、`referenceLength`、`coveredBases`、`zeroDepthBases`、`coverageFractionAt1x`、`depthSum`、`meanDepth`、`maxDepth`、`mappedReadSegments`、`unmappedReadSegments`。使用 `referenceName` 作为指标分组键，展示分参考表格和柱状图。该名称来自实际 FASTA/BAM 序列标识，不保证始终等于数据库 accession；只有确认对应关系后再补充节段标签，不能凭数组顺序关联。

平均深度的分母包含零深度位置。总体统计使用顶层指标，不对各参考的平均值再简单求平均。read segment 数来自 BAM 统计，双端两端分别计数，不命名为“read pair 数”；比对计数和经过质量筛选的深度也不是同一统计口径。

逐位置深度曲线需要另外解析 `coverageDepthUrl`，指标 JSON 本身不包含曲线点。BAM 浏览器集成属于另外的功能，当前下载接口不等于已经提供在线基因组浏览器。

### 5.3 Variant Call

`stageType=40`，结果取自 `outputUrl`。

- `rawVcfGzUrl`、`rawVcfTbiUrl`：原始 VCF 及索引，可为空。
- `vcfGzUrl`、`vcfTbiUrl`：处理后的 VCF 及索引。
- `metricsUrl`：指标 JSON。

| 指标 | 建议标签 |
| --- | --- |
| `variantRecords` | 变异记录总数 |
| `snpCount` / `indelCount` / `otherCount` | SNP / INDEL / 其他变异记录数 |
| `explicitPassCount` | 明确 PASS 的变异记录数 |
| `filteredCount` | 未通过过滤、已标记的变异记录数 |
| `unfilteredCount` | FILTER 为 `.`、未明确标记过滤状态的变异记录数 |
| `totalVcfRecords` | 全部 VCF 数据记录数，可能含非变异参考记录 |
| `references` | 各参考序列的上述统计，另有 `referenceName`、可空的 `referenceLength` |

按两种维度分别作图：变异类型分布、过滤状态分布。当前满足以下计数关系，不能把六个字段放进同一个互斥饼图：

```text
snpCount + indelCount + otherCount = variantRecords
explicitPassCount + filteredCount + unfilteredCount = variantRecords
```

指标按处理后 VCF 的记录统计，不是逐个替代等位基因计数。`totalVcfRecords` 不能显示成“变异总数”。当前软过滤会保留未通过的记录并附加过滤标记，所以 `filteredCount` 不能叫“已删除数量”。

逐条位置、替换、过滤原因等明细需要解析 VCF，当前没有明细分页接口。不要把“检测到变异”自动解释成某种临床或功能结论。

### 5.4 Consensus

`stageType=41`，结果取自 `outputUrl`。

- `consensusUrl`：最终 Consensus FASTA。
- `maskBedUrl`：深度遮罩区域 BED。
- `metricsUrl`：遮罩统计 JSON。

指标包括 `minimumDepth`、`totalBases`、`unmaskedBases`、`maskedBases`、`unmaskedFraction`、`maskedFraction`。`sequences[]` 包含每条参考的 `sequenceName` 及对应的长度、遮罩和比例字段。

建议显示“深度遮罩阈值”“未遮罩位置数”“低深度遮罩位置数”，并以堆叠条展示遮罩比例。

这些指标按参考坐标和深度计算，`totalBases` 不是重新扫描最终 FASTA 的长度，`maskedBases` 也不是最终 FASTA 中所有 N 的计数。不要直接命名为“基因组完整率”“最终序列 N 比例”。若需要这些文件层面的统计，应另行解析 `consensusUrl`。

### 5.5 参考比较

`stageType=43`，病毒 Assembly 输入使用此阶段，结果取自 `outputUrl`。

- `pafPath`：比对 PAF 对象键，虽然字段名带 Path，仍通过文件接口获取。
- `differenceTsv`：差异 TSV 对象键。

当前可直接展示阶段状态和文件下载。比对图、差异数量、类型统计需要解析文件；没有独立 metrics JSON。不要把参考选择阶段的候选分数当成参考比较阶段自己的汇总指标。

### 5.6 SNP 注释

`stageType=72`，正常结果取自 `outputUrl`，文件字段为 `annotatedVCF`，注意大小写。

当前可展示注释文件下载。功能影响、基因和注释类别等汇总需要额外解析。

当前编排要求选中参考正好只有一条序列且配置了 `annotationFile`。多参考序列或没有注释文件时，阶段标为 `NOT_APPLICABLE`。这表示本次未执行注释，不等于没有变异或工具失败。

## 6 细菌阶段结果

### 6.1 物种分类

`stageType=10`，结果取自 `outputInline`。

| 字段 | 用途 |
| --- | --- |
| `status` | 分类结论状态，独立于外层阶段状态 |
| `best` | 已确定的分类，未确定时可为空 |
| `candidates` | 候选分类列表，可为空 |
| `evidenceResource` | `FASTANI`、`KRAKEN2`、`USER_CONFIRMED` 等证据来源，不是 URL |
| `manualConfirmation` | 人工确认审计信息，可为空 |
| `binningMetrics` | 组装序列分箱指标，可为空 |
| `mainBinFastaUrl` | 原始主分箱 FASTA，可为空 |
| `contigAssignmentReportUrl` | Contig 分类分配 TSV，可为空 |

`best` 和 `candidates[]` 的字段是 `taxid`、`name`、`rank`、`score`。注意这里是小写 `taxid`，不是请求参数使用的 `taxId`。

| 分类结果 status | 展示文案 | 当前常见阶段状态 |
| --- | --- | --- |
| `0` | 分类已确定 | `4`，已完成 |
| `1` | 分类存在歧义 | `5`，等待人工处理 |
| `2` | 分类证据不足 | `5`，等待人工处理 |
| `3` | 超出当前分类范围 | `6`，不适用 |

候选分数的标签必须结合证据来源：

- `FASTANI`：ANI 分值。
- `KRAKEN2` 的 Contig 分箱分支：按组装碱基数加权的百分比。
- `KRAKEN2` 的 Reads 分支：报告中的分类百分比。
- `USER_CONFIRMED`：表示人工确定分类，原始候选分数仍保留；原来源见 `manualConfirmation.originalEvidenceResource`。

这些不是统一的“置信度概率”，不要在不同来源间直接比较高低，也不要强行给空 `best` 填充第一名。

`manualConfirmation` 可展示 `speciesTaxId`、`confirmedAt`、`confirmedBy`、`reason`；另有 `originalStatus`、`originalEvidenceResource`。确认时间在内联快照中可能是 epoch 毫秒，应与外层日期字段分别适配，不能假设两者格式一致。

### 6.2 Contig 分箱

`binningMetrics` 包含：

| 指标组 | 字段 |
| --- | --- |
| 总规模 | `totalContigs`、`totalAssemblyBases` |
| 已归属到物种 | `speciesAssignedContigs`、`speciesAssignedBases` |
| 已分类但未解析到物种 | `unresolvedContigs`、`unresolvedBases` |
| 未分类 | `unclassifiedContigs`、`unclassifiedBases` |
| 原始优势物种分箱 | `dominantSpeciesTaxId`、`dominantSpeciesName`、`dominantSpeciesAssemblyFraction` |
| 分箱列表 | `bins[]` |

每个 `bins[]` 项包含 `taxId`、`name`、`rankCode`、`contigCount`、`totalBases`、`assemblyFraction`。

可制作分箱表格、按碱基长度加权的组成图，以及“已归属物种／未解析到物种／未分类”的概览。比例字段为 0–1。`bins[]` 是互斥的最终分配分箱，每条 contig 只计入其中一项；列表可能同时包含物种级、未解析到物种的其他层级和未分类项。整体组成图应保留层级标签；如果只展示物种级条目，应保留其余未解析和未分类的占比，不自动归一化成全部样本的组成。

这些反映组装序列的分类组成，不直接命名为“样本物种丰度”。人工确认不会重算原分箱或主分箱 FASTA，原始优势分箱可能与人工确认后的 `best` 不同，应分别展示。

### 6.3 AMR

`stageType=60`，解析 `outputUrl` 后读取 `amrResult`，它是 AMR TSV 报告的对象键，不是内联列表。

当前可直接提供报告下载。命中数量、注释列表和类别图需要按报告表头解析；阶段结果尚无对应的统一 JSON 字段。展示名称使用“AMR 注释／命中结果”，不自动转换成药敏或临床耐药结论。

### 6.4 VF

`stageType=61`，解析 `outputUrl` 后读取 `vfTsv`，对应 VF TSV 报告。

当前可直接提供报告下载；命中表及统计图需要解析。旧快照可能使用 `amrTsv` 键，若需要兼容历史任务，应在 VF 模块内读取旧键，不把它误归为 AMR 报告。注释命中不自动等同于致病性结论。

### 6.5 MLST

`stageType=62`，解析 `outputUrl` 后读取 `mlst`，JSON 键是小写。

该值是 MLST TSV 报告对象键，不是 ST 值。提取 scheme、ST、各位点等位基因结果后才能制作卡片或表格。MLST 结果与物种分类结果分开显示，不使用 scheme 名称替代 Taxonomy 结论。

### 6.6 血清分型

`stageType=64`，正常结果取自 `outputUrl`。

| 字段 | 用途 |
| --- | --- |
| `serotypeResultUrl` | 主分型报告，可能是 TXT 或 TSV |
| `kaptiveKResultUrl` | Kaptive K 位点报告，仅适用分支提供 |
| `kaptiveOResultUrl` | Kaptive O 位点报告，仅适用分支提供 |

不同工具的报告格式不同，不能根据统一的 `.tsv` 后缀假设列相同。当前可能来自 SeqSero2、ECTyper、Kaptive、SeroBA 或 pFaster；结果对象尚无统一的 `tool`、`serotype`、`confidence` 字段。

Kaptive 分支可分别展示 K/O 报告，其合并主报告通过 `locus_type` 列区分 `K` 和 `O`。其他分支的两个 K/O 文件字段为空是正常情况。

没有兼容输入或不支持目标分类时，阶段可以跳过。此时显示“不适用”及已提供的 `skipReason`，不能展示成“未检出血清型”。

## 7 状态和人工确认交互

### 7.1 区分三类状态

| 状态对象 | 数值含义 |
| --- | --- |
| 接口 Result.status | `0=业务成功`、`1=业务失败`、`2=内部失败`、`3=重复操作`、`4=参数不合法` |
| Pipeline.status | `1=运行中`、`2=已完成`、`3=待启动` |
| Stage.status | 见下表 |

| Stage.status | 展示文案 | 页面行为 |
| --- | --- | --- |
| `0` | 待执行 | 显示等待，不用空结果填零 |
| `1` | 排队中 | 显示排队状态 |
| `2` | 运行中 | 显示运行状态，不把暂缺文件当成失败 |
| `3` | 执行失败 | 标记失败；有历史结果时须区分，不能冒充本次成功产物 |
| `4` | 已完成 | 加载并展示当前结果 |
| `5` | 等待人工处理 | 分类阶段展示候选选择与确认入口 |
| `6` | 不适用／已跳过 | 展示适用性和可用原因，仍可保留分类等已有证据 |

Taxonomy 结果内部 `status` 是第 6.1 节的另一套分类编号，不复用 Stage 状态标签。流水线在某个阶段失败后可能仍保持运行中以等待重试，因此总览也需要检查阶段状态。

`FINISHED` 表示程序执行完成，不代表统一的分析质量 PASS。当前没有通用的质量 PASS/WARN/FAIL 结论，前端不自行用任意阈值生成此类结论。

### 7.2 人工确认分类

对于流水线仍处于运行中（`pipeline.status=1`）、阶段处于等待处理（`stage.status=5`）的分类任务，允许从已保存的 `candidates` 选择一个结果进行确认。提交时后端仍会重新校验状态：

```http
POST /pipeline/stage/taxonomy/confirm
Content-Type: application/json
```

```json
{
  "stageId": 123,
  "taxId": 562,
  "reason": "已人工核对分类证据"
}
```

示例中的 ID 仅用于说明请求格式。实际 `taxId` 来自所选候选的 `taxid` 字段。

- `stageId`、`taxId` 必填且大于 0，`reason` 可选且最多 2000 字符。
- 不传 `version` 或 `confirmedBy`，版本判断及确认人由后端处理。
- 不允许任意输入候选列表之外的 TaxID。
- 新产生的 OUT_PANEL 阶段是 `status=6`，不显示常规确认按钮；旧的待确认记录可能存在兼容处理。

响应 `data` 包含 `stageId`、`taxId`、`version`、`downstreamScheduled`、`alreadyConfirmed`。提交后刷新完整阶段详情。

确认可能已经保存，但下游调度失败；此时响应可能是 HTTP 500，仍携带确认数据且 `downstreamScheduled=false`。前端应提示“确认已保存，下游启动失败”，允许重复相同确认请求重试调度，不能一概显示“确认未保存”，也不能由此允许改成另一候选。

重复已保存的同一确认请求属于调度恢复，不是首次确认：阶段可以已经完成，后端允许所属流水线处于运行中或已完成。恢复按钮与首次确认按钮应区分，实际是否可恢复仍以服务端响应为准。

## 8 通用展示和异常处理要求

### 8.1 单位与空值

| 数据类型 | 展示规则 |
| --- | --- |
| Fraction 比例 | 0–1，乘 100 后显示百分比，例如 `0.95 → 95%` |
| 参考选择 sequenceIdentity | 已是 0–100 百分数，不再次乘 100 |
| 深度 | 显示单位 ×，例如 `82.60×` |
| 长度和碱基数 | 显示 bp，可辅助换算 kb/Mb，不改变原始数值 |
| 记录数 | 整数，注意是 reads、read segments、contigs 还是 VCF records |
| null / 缺字段 | 显示“未提供”或隐藏，不转成数值 0 |

百分比格式化示例：

```javascript
function formatFraction(value) {
  return typeof value === "number" && Number.isFinite(value)
    ? `${(value * 100).toFixed(2)}%`
    : "—";
}
```

`startTime`、`endTime` 可能为空，只有两者有效时才计算耗时。显示时间统一处理时区，避免把 epoch 毫秒当秒，或给已经带时区的时间重复转换。

### 8.2 结果缺失和报告为空

- 阶段没有结果字段：显示“暂无结果”，结合阶段状态解释。
- JSON 无法解析：显示读取错误并保留重试入口，不显示“零命中”。
- 文件对象键为空：隐藏下载入口，不生成空 URL 请求。
- 文件下载失败：显示获取失败，不能解释为阴性或未检出。
- 报告有表头但无数据行：按该报告规范确认后，才能显示“未发现匹配记录”。
- `FAIL`、`ACTION_REQUIRED`、`NOT_APPLICABLE`、成功但没有匹配记录是不同的页面状态。
- 大型 BAM、VCF、FASTA 或深度文件不应在打开总览时全部加载；指标 JSON 与文件明细分开、按需加载。
- JSON/文件读取失败不应阻塞其他已有模块的展示。

后端当前有 `bio_pipeline_log` 失败记录，但尚无日志查询 HTTP 接口。完整失败日志面板需要补充接口；阶段列表本身不保证包含失败详细原因。

## 9 前端联调检查清单

- [ ] 病毒 Reads 和 Assembly 两种输入显示不同的实际模块。
- [ ] 细菌分类、AMR、VF、MLST 等结果独立展示，不依赖全部阶段同时完成。
- [ ] 详情使用 `getPipelineStages`，参考选择与分类的 inline 结果能读取。
- [ ] `outputInline` 和 `outputUrl` 均按 JSON 字符串解析，没有直接作为浏览器地址。
- [ ] 下载参数为单个对象键，经过 URL 编码；指标 JSON 能单独获取。
- [ ] Mapping、Variant、Consensus 新结果从 `metricsUrl` 取数。
- [ ] 两套分类和阶段状态不混淆，跳过与失败有不同展示。
- [ ] 百分数和 Fraction 没有重复乘 100，空值不会显示成 0。
- [ ] Variant 图表没有把全部 VCF 记录数当成变异总数。
- [ ] Consensus 遮罩比例没有命名成样本完整率或最终 N 比例。
- [ ] 分箱组成图使用互斥最终分箱的碱基加权比例，标明层级并保留未解析部分，不标成真实样本物种丰度。
- [ ] 多节段参考不会因 `complete=false` 被错误标成缺节段或失败。
- [ ] 可选 Scaffolds、R2、注释、K/O 报告缺失时页面正常。
- [ ] AMR、VF、MLST、血清分型只有在完成对应格式解析后才显示明细和统计。
- [ ] 人工确认只提交已有候选，能区分确认保存与下游调度状态。
- [ ] 文件不存在、下载失败、坏 JSON、旧结果缺指标均有独立空态或错误态。

## 10 后端实现位置

- [流水线及阶段查询接口](../../src/main/java/com/xjtlu/bio/controller/PipelineController.java)
- [阶段产物下载接口](../../src/main/java/com/xjtlu/bio/controller/StageOutputController.java)
- [流水线阶段构建](../../src/main/java/com/xjtlu/bio/analysisPipeline/workflow/AnalysisPipelineStagesBuilder.java)
- [阶段编排和跳过逻辑](../../src/main/java/com/xjtlu/bio/analysisPipeline/workflow/StageOrchestrator.java)
- [状态及类型常量](../../src/main/java/com/xjtlu/bio/analysisPipeline/Constants.java)
- [阶段结果对象](../../src/main/java/com/xjtlu/bio/analysisPipeline/stageResult)
- [结构化指标对象](../../src/main/java/com/xjtlu/bio/analysisPipeline/stageMetrics)
- [参考集合和候选分数](../../src/main/java/com/xjtlu/bio/analysisPipeline/referenceGenome)
- [结果保存处理](../../src/main/java/com/xjtlu/bio/analysisPipeline/stageDoneHandler)

## 11 新冠污水混合分析（stageType=92）

`pipelineType=102` 是独立的单 Stage 流水线。入口提醒、上传约定及部署前提见 [创建页面说明](./virus-pipeline-creation-guide.md) 第 14 节。本轮完成软件适配和模拟测试，不表示已有真实 Freyja 报告或真实数据端到端验收记录。

`stage.outputUrl` 是 JSON 字符串，解析后的字段如下：

| 字段 | 文件 | 前端用途 |
| --- | --- | --- |
| `reportUrl` | `freyja.demix.tsv` | 原始分析报告下载 |
| `metricsUrl` | `covid-wastewater.metrics.json` | 读取结构化指标，展示谱系比例及报告统计 |
| `provenanceUrl` | `provenance.json` | 方案版本、配置声明的工具版本、数据版本和文件 SHA-256 下载 |
| `executionLogUrl` | `execution.log` | 成功运行的执行日志下载 |
| `bamUrl` | `aligned.bam` | 可选比对产物下载 |
| `bamIndexUrl` | `aligned.bam.bai` | 可选索引下载；与 BAM 成对提供 |

仍使用 `GET /stageOutput/download?outputUrl=<编码后的单个对象键>` 获取文件。前端不要推测服务器目录或固定拼接文件键。

以下指标 JSON 仅用于说明结构，**不是实测样本结果**：

```json
{
  "lineages": [
    { "lineage": "example-lineage-A", "abundance": 0.7 },
    { "lineage": "example-lineage-B", "abundance": 0.25 }
  ],
  "coveragePercent": 95.8,
  "residual": 3.14,
  "summarizedRaw": "[('example-group', 0.95)]"
}
```

- `lineages[].abundance` 是 0–1 的比例，展示为百分数时乘 100；不要自动把所有条目重新归一化到 100%。
- `coveragePercent` 已经是百分数。它对应工具报告的覆盖统计，不是“结果准确率”或“已确定谱系比例”。
- `residual` 是工具报告的拟合残差，不是错误率；不要据此自行输出 PASS/FAIL 或诊断结论。
- `summarizedRaw` 保留原报告的汇总文本，只展示纯文本或提供下载，不调用 `eval`。
- 程序校验数据格式、有限数值和范围；空谱系、缺字段、长度不一致及工具的求解失败标志不会转成“成功但全零”。这不等于科学质量门槛已经验收。
- `FINISHED` 表示程序执行和报告保存完成；不要自动显示“检测准确”或“样本合格”。
- 失败时通常没有可下载的正常结果；错误摘要与有限长度日志尾会按既有失败记录机制保存。现有 API 尚未提供日志查询接口，不能保证失败页面能读取全部内容。

报告格式同时兼容官方文档展示的旧数组形式以及当前 CLI 写出的空格分隔形式。字段含义依据 [Freyja 报告说明](https://andersen-lab.github.io/Freyja/src/usage/demix.html)，序列化格式核对自 [官方 CLI 源码](https://github.com/andersen-lab/Freyja/blob/main/freyja/_cli.py)。
