# 前端增量交接：创建流水线时配置本次参考范围

更新日期：2026-10-25。

本文可直接交给前端 agent，集中说明创建页面需要接入的 `enabledReferenceIds`，以及第 9 节的 FASTA 单阶段全候选比较。接口和字段已经接入当前后端源码；本文不代表目标联调环境已经部署，也不代表前端已经实现。原有参考库管理、文件上传和启动接口不在本次修改范围内。

## 1. 背景与本次目标

目前用户创建病毒任务时选择的是分析目标（`taxId`），后端负责展开该目标对应的参考分类成员，再加载参考候选，不需要前端维护 `memberTaxIds`。

本次在此基础上增加一个可选交互：用户可以缩小“本次任务允许使用的参考候选范围”。默认行为不变，不要求所有用户都逐条选择参考。

需要明确区分：

| 配置 | 作用范围 | 前端操作 |
| --- | --- | --- |
| 参考库记录的 `enabled` | 全局配置，影响后续任务的候选加载 | 仅在参考库管理页面调用启停接口 |
| 创建参数的 `enabledReferenceIds` | 仅限制本次任务的候选集合 | 在创建请求中提交参考记录 ID 数组 |
| Reads 流水线最终选中的参考 | 参考选择阶段产生的结果 | 不是创建页面的勾选结果；FASTA 路线不产生最优参考选择 |

本次只适用于普通病毒 `pipelineType=100` 和新冠 `pipelineType=101`。其他类型，包括污水 `102`，不展示此配置，也不提交非 `null` 的 `enabledReferenceIds`。

## 2. 页面交互

在现有分析目标选择控件下增加“本次任务参考范围”：

1. 默认选择“使用全局启用范围”，提交时省略 `enabledReferenceIds` 或传 `null`，不必加载全部参考。
2. 用户开启“自定义参考范围”后，查询所选目标下全局启用的参考，加载完整候选后默认全部勾选，允许取消本次不使用的项。
3. 列表使用 `referenceId` 作为行标识和勾选值。建议展示 accession、名称、实际 taxId、segment、来源和序列长度；允许空字段显示为 `—`。
4. 显示“已选 X / 共 Y”，跨页保留完整勾选集合。未加载完整时显示加载状态，不声称已经全选全部候选。
5. 自定义模式至少保留一条参考；全部取消时阻止提交，不能自动切回默认范围。
6. 切换分析目标或流水线类型后，清空旧候选和勾选，重新加载；丢弃旧请求晚到的响应，避免不同目标的参考混用。

推荐提示文案：

> 默认使用该目标下全局启用的参考序列。开启自定义后，可限制本次任务的候选范围，不会改变参考库全局启用状态。Reads 输入由后端选择后续参考；FASTA 输入逐条比较全部候选，不选择最优参考。

任务页面勾选时，不调用 `/referenceGenome/library/{referenceId}/enabled`；该接口修改的是全局开关。

## 3. 获取分析目标与候选参考

### 3.1 分析目标

沿用 `GET /pipeline/virusTargets`。成功响应的 `data` 是选项数组，每项包含：

- `label`：展示名称。
- `taxId`：创建任务的分析目标。
- `pipelineType`：该选项对应的流水线类型。

前端使用同一选项的 `taxId + pipelineType`，不维护静态目标或成员清单。该接口不返回参考列表或 `memberTaxIds`。

新冠选项使用 `pipelineType=101`、`taxId=2697049`。创建接口会固定新冠目标，但候选查询接口不会自动纠正错误组合，前端应始终提交正确组合。

### 3.2 候选查询

复用现有接口，不新增接口：

```http
POST /referenceGenome/library/query
Content-Type: application/json
```

```json
{
  "targetTaxId": 11320,
  "pipelineType": 100,
  "enabled": true,
  "start": 0,
  "pageSize": 100
}
```

`targetTaxId` 取所选目标的 `taxId`，由后端展开参考成员。`targetTaxId` 与 `pipelineType` 必须成对提供且匹配已登记选项；本接口仅支持 `100`／`101` 的专项上下文。

分页规则：

- `start` 是从 0 开始的记录偏移量，不是页码；默认 0。
- `pageSize` 默认 20，范围 1–100。
- 读取成功响应的 `data.items` 和 `data.total`，遍历全部分页，按 `referenceId` 维护选择集合。
- 初始化完整候选时，只传专项上下文、`enabled=true` 和分页参数。不要附加实际 `taxId`、`orgType` 或 accession 条件；这些条件会进一步缩小范围。
- 特别注意：参考管理页面的固定分类筛选不能直接复制到任务候选查询中。`taxId` 精确筛选也不能替代 `targetTaxId` 的成员展开。

响应结构示例（记录内容仅用于说明字段，不是可直接使用的测试参考）：

```json
{
  "status": 0,
  "data": {
    "total": 1,
    "start": 0,
    "pageSize": 100,
    "items": [
      {
        "referenceId": 501,
        "accession": "LOCAL_DEMO_001",
        "sourceDb": "USER_UPLOAD",
        "taxId": 11320,
        "orgType": 0,
        "organismName": "示例参考名称",
        "genomeLength": 1000,
        "segment": null,
        "completeness": null,
        "isAnnotated": false,
        "enabled": true,
        "fastaObjectName": "refseq/custom/example/reference.fna"
      }
    ]
  },
  "failMsg": null
}
```

查询失败不能当成空列表，也不能改成全库查询或仅查询目标 taxId 自身作为替代。自定义模式下，加载失败或分页未完成时禁止提交；用户可主动切回默认模式，但前端不能自动丢弃其自定义选择。

完整查询返回零条时，提示“该目标暂无全局启用的参考，请先维护参考库”，不要把它表示为“已选择全部”。创建本身不等于保证后续能够启动。

## 4. 参数语义与校验

字段名固定为 `enabledReferenceIds`，类型为 JSON 整数数组，对应后端 `List<Long>`。

| 请求值 | 行为 |
| --- | --- |
| 省略或 `null` | 在启动时使用该目标下当时全局启用的参考，不在创建时冻结默认候选 |
| `[]` | 参数错误，不表示默认、全部或不使用参考 |
| 非空数组 | 本任务仅使用这些候选：Reads 参与选择，FASTA 全部逐条比较；所有 ID 必须有效 |

显式列表的每个 ID 必须是正整数，不能重复、不能为 `null`、字符串或小数。提交的是参考记录 `referenceId`，不是 taxId、accession 或成员 ID。

每条记录必须存在、全局启用，并属于当前分析目标解析出的成员范围。任何一条不满足时，后端拒绝整份列表，不取有效项交集，也不回退全局默认。

不要把勾选顺序解释为参考优先级，也不要把勾选解释为手动指定最终参考。

“默认范围”和“显式提交全部当前 ID”不同：前者会继承启动时的全局范围；后者始终受提交的 ID 列表限制，且启动时这些记录仍须有效。

## 5. 单个创建请求

```http
POST /pipeline/create
Content-Type: application/json
```

```json
{
  "projectId": 123,
  "pipelineType": 100,
  "analysisName": "病毒分析任务（自定义参考范围）",
  "pipelineStageParameters": {
    "taxId": 11320,
    "enabledReferenceIds": [501, 503],
    "sequencingPlatform": 0,
    "extraParams": {}
  }
}
```

`501`、`503` 只是字段示例，必须替换为候选接口实际返回的有效参考 ID。默认模式删除该字段或设为 `null`，其余字段和原流程一致。

准确路径是 `pipelineStageParameters.enabledReferenceIds`，与 `taxId` 同级。不要放在请求根对象、`extraParams` 或 `extraParams.stageParams` 中。

成功响应示例：

```json
{
  "status": 0,
  "data": 456,
  "failMsg": null
}
```

`data` 为新任务的 `pipelineId`。创建不会自动上传输入，也不会自动启动分析，后续沿用现有流程。

## 6. 批量创建请求

```http
POST /pipeline/batchCreatePipelines
Content-Type: application/json
```

```json
{
  "projectId": 123,
  "pipelineType": 100,
  "pipelines": [
    {
      "pipelineName": "任务01（自定义）",
      "pipelineParameters": {
        "taxId": 11320,
        "enabledReferenceIds": [501, 503],
        "sequencingPlatform": 0,
        "extraParams": {}
      }
    },
    {
      "pipelineName": "任务02（默认）",
      "pipelineParameters": {
        "taxId": 11520,
        "enabledReferenceIds": null,
        "sequencingPlatform": 0,
        "extraParams": {}
      }
    }
  ]
}
```

批量的字段路径是 `pipelines[i].pipelineParameters.enabledReferenceIds`，每项独立；不要误用单个创建的 `pipelineStageParameters`。

顶层 `pipelineType` 为整批共用，普通病毒 `100` 与新冠 `101` 分批提交。不同目标的任务不能直接复用同一组参考 ID。示例 ID 同样必须替换为实际有效值。

## 7. 错误与配置变化

- 同时检查 HTTP 状态和业务 `status`，仅 `status=0` 表示业务成功；兼容非统一 JSON 的 HTTP 错误响应，不只判断 HTTP 200。
- 创建失败时保留表单和勾选，展示后端 `failMsg`；不要自动删除失效 ID 后重试，也不要自动传 `null` 扩大范围。
- 创建和启动都会校验显式列表。创建之后，若参考被全局禁用、删除或移出目标范围，启动可能失败，应提示刷新候选并重新确认。
- 用户已自定义后刷新候选，不应静默恢复全选或删除失效项；有变化时明确提示用户复核。
- 完整参考快照在启动构建 Stage 时生成。后续全局启停或成员配置变化不会重写已保存的 Stage 快照。
- 本次不新增成员级 `enabled`，不要求前端提交 `memberTaxIds`，也不改参考上传接口。

## 8. 前端验收清单

- [ ] 默认模式省略字段或传 `null`，原创建、上传、启动流程不变。
- [ ] 自定义模式使用所选目标的 `targetTaxId + pipelineType + enabled=true` 查询完整候选。
- [ ] 超过一页时正确加载全部记录、维护跨页勾选，不把第一页当作全部。
- [ ] 请求只提交勾选的 `referenceId`，单个和批量字段位置正确。
- [ ] 全部取消、加载失败、加载未完成时不能提交自定义列表。
- [ ] 切换目标时清理旧选择，旧请求不能覆盖新目标的列表。
- [ ] 无效或失效参考错误可展示；不静默过滤、不自动回退默认。
- [ ] 不修改全局启停；其他任务不受本次勾选影响。
- [ ] 非 `100`／`101` 类型不提交该字段；新冠使用固定目标组合。
- [ ] FASTA 新任务只展示参考比较阶段，保留全部 `MATCHED` / `NO_MATCH` 候选，不标记最优参考；Reads 路线不变。

## 9. FASTA 全候选比较与结果版本

本节是可独立交付的完整接入契约，包含字段表和完整 JSON 样例，不依赖前端工作区中其他文档是否已更新。如果前端本地“结果展示说明第 5.5 节”仍只有 `pafPath` / `differenceTsv`，那是历史格式说明；新版按本节接入，旧字段仅用于兼容历史任务。

### 9.1 适用路线

普通病毒 `100` 和新冠 `101` 的 Assembly / FASTA 输入（上传 `fileType=11`）现在只创建 `ReferenceComparison`（`stageType=43`），不再先运行 `ReferenceSelection`（`44`）。有效候选范围仍由目标、全局启用状态及可选 `enabledReferenceIds` 决定；成功报告包含该范围中的每条参考，不因同 accession、同节段或没有匹配而只保留部分候选。Reads 路线的参考选择和下游分析保持原样。

后端保存的阶段参数使用 `candidateReferences` 快照；前端不要提交这个内部字段。历史阶段若该字段缺失或为 `null`，后端可读取旧 `referenceGenome`；显式 `[]` 无效，不回退到旧参考。此参数兼容规则不等于结果格式兼容规则。

### 9.2 第一步：解析阶段结果索引

从 `GET /pipeline/getPipelineStages?pipelineId=...` 的成功响应 `data` 数组中取得实际阶段。`stage.outputUrl` 的类型仍是 **string**，先执行 `JSON.parse(stage.outputUrl)`，解析后的对象如下：

```json
{
  "schemaVersion": 2,
  "candidateCount": 3,
  "matchedCandidateCount": 2,
  "unmatchedCandidateCount": 1,
  "comparisonJsonUrl": "stageOutput/588/reference_comparison.json",
  "comparisonTsvUrl": "stageOutput/588/reference_comparison.tsv"
}
```

| 索引字段 | 类型 | 含义 |
| --- | --- | --- |
| `schemaVersion` | number | 本版固定为 `2` |
| `candidateCount` | number | 本次比较的候选总数 |
| `matchedCandidateCount` | number | 状态为 `MATCHED` 的候选数 |
| `unmatchedCandidateCount` | number | 状态为 `NO_MATCH` 的候选数 |
| `comparisonJsonUrl` | string | 完整 JSON 报告的存储对象键 |
| `comparisonTsvUrl` | string | 全部候选汇总 TSV 的存储对象键 |

`candidateCount = matchedCandidateCount + unmatchedCandidateCount`。这里没有 `comparisons[]`，也没有 `metricsUrl`；完整明细保存在 JSON 报告文件，不在 `outputUrl` 或 `outputInline` 内嵌。

### 9.3 第二步：下载并解析 JSON 报告

使用现有文件接口，不新增接口：

```http
GET /stageOutput/download?outputUrl=<encodeURIComponent(comparisonJsonUrl)>
```

- 参数是索引中的**单个对象键**，不是完整的 `stage.outputUrl` JSON，不是服务器路径。
- 成功响应是原始文件内容，没有 `{status,data,failMsg}` 包装。
- 响应的 Content-Type 是 `application/octet-stream`，带附件下载头；程序仍可按文本/JSON解析。若项目请求封装自动取 `response.data.data`，此接口应走文件请求分支。
- 沿用项目配置的 API 地址、认证和请求封装；不要把下方样例路径或某个 WSL 地址写死。
- 网络、HTTP、JSON解析或版本错误应显示加载失败并允许重试，不能转成空列表或 `NO_MATCH`。

最小读取示意：

```javascript
const index = JSON.parse(stage.outputUrl);
if (index.schemaVersion !== 2) {
  // 按第 9.6 节处理历史格式或未知版本，不继续按 v2 解析。
  throw new Error("需要切换到对应的结果格式处理分支");
}

const response = await fetch(
  `/stageOutput/download?outputUrl=${encodeURIComponent(index.comparisonJsonUrl)}`
);
if (!response.ok) throw new Error(`读取比较报告失败，HTTP ${response.status}`);
const report = await response.json();
if (report.schemaVersion !== 2 || !Array.isArray(report.comparisons)) {
  throw new Error("比较报告格式不符合 schema-v2");
}
// 表格数据：report.comparisons；不是 report.data，也不是 report.metrics。
```

### 9.4 JSON 报告字段表

报告顶层有 `schemaVersion`、三个计数（与索引同名同义）以及 `comparisons` 数组。成功报告中 `comparisons.length = candidateCount`。每个候选的字段是**平铺结构**，没有嵌套的 `metrics`：

| 候选字段 | 类型 | 含义和展示规则 |
| --- | --- | --- |
| `referenceId` | number / null | 参考记录 ID；有值时可关联参考库记录 |
| `referenceAccession` | string | accession；不能假定唯一，不能据此合并候选 |
| `sourceDb` | string / null | 来源，例如 `USER_UPLOAD`、`RefSeq`；显示实际返回值 |
| `taxId` | number / null | 该参考的实际分类 ID，不用创建任务的目标 taxId 覆盖 |
| `organismName` | string / null | 参考名称；未知显示 `—` |
| `segment` | string / null | 原始节段标签；未知显示 `—` |
| `segmentKey` | string / null | 规范化分组键，例如 `SEGMENT_1`；可分组，但不合并或隐藏同组候选 |
| `status` | string | `MATCHED` 或 `NO_MATCH`，独立于外层 Stage 的数值状态 |
| `sequenceIdentity` | number | 0–100 百分数；`99.775` 显示为 `99.775%`，不要再次乘 100 |
| `referenceCoverage` | number | 0–1 比例；`0.5` 显示为 `50%` |
| `alignedReferenceBases` | number | 对齐到的参考位置数，重叠区间取并集，单位 bp |
| `referenceBases` | number | 该候选 FASTA 中所有序列记录的总长度，单位 bp |
| `matchingBases` | number | 比对记录累计的匹配碱基数，不是去重覆盖位置数 |
| `alignmentBlockBases` | number | 比对记录累计的比对块长度，单位 bp |
| `alignmentCount` | number | 有效比对记录数；不是变异数、reads 数或节段数 |
| `alignmentPafUrl` | string | 该候选独立 PAF 文件的存储对象键 |
| `differenceTsvUrl` | string | 该候选独立差异 TSV 的存储对象键 |

覆盖率使用 `alignedReferenceBases / referenceBases`；一致性使用 `100 × matchingBases / alignmentBlockBases`，无对齐时为 0。前端优先展示后端返回值，不把两组不同口径的计数混算。

### 9.5 完整新版 JSON 报告样例

下面来自本轮隔离 HTTP 集成测试的实际下载结果，仅整理缩进。测试数据为固定种子生成的软件测试数据，`taxId=2000000000` 是隔离测试路由值，不是真实分类。记录 ID、名称和对象键均只用于示例；临时测试服务已关闭，不能拿这些键向当前联调服务索取文件。

同一报告保留三个同节段候选：完全匹配、部分覆盖匹配、无匹配。数值可以作为前端渲染验收样例，但不代表真实样本分析结论。

```json
{
  "schemaVersion": 2,
  "candidateCount": 3,
  "matchedCandidateCount": 2,
  "unmatchedCandidateCount": 1,
  "comparisons": [
    {
      "referenceId": 37180,
      "referenceAccession": "CUSTOM_865f8f6fd14b48daba995b53c046b377",
      "sourceDb": "USER_UPLOAD",
      "taxId": 2000000000,
      "organismName": "COMPARISON_TEST_ONLY_f1a3e01562874e16b7fa018a74741550_exact",
      "segment": "1",
      "segmentKey": "SEGMENT_1",
      "status": "MATCHED",
      "sequenceIdentity": 100,
      "referenceCoverage": 1,
      "alignedReferenceBases": 6000,
      "referenceBases": 6000,
      "matchingBases": 6000,
      "alignmentBlockBases": 6000,
      "alignmentCount": 1,
      "alignmentPafUrl": "stageOutput/588/candidates/0001/alignment.paf",
      "differenceTsvUrl": "stageOutput/588/candidates/0001/reference_difference.tsv"
    },
    {
      "referenceId": 37181,
      "referenceAccession": "CUSTOM_5a8113926f25419d9b365196947b119f",
      "sourceDb": "USER_UPLOAD",
      "taxId": 2000000000,
      "organismName": "COMPARISON_TEST_ONLY_f1a3e01562874e16b7fa018a74741550_partial",
      "segment": "1",
      "segmentKey": "SEGMENT_1",
      "status": "MATCHED",
      "sequenceIdentity": 99.775,
      "referenceCoverage": 0.5,
      "alignedReferenceBases": 4000,
      "referenceBases": 8000,
      "matchingBases": 3991,
      "alignmentBlockBases": 4000,
      "alignmentCount": 1,
      "alignmentPafUrl": "stageOutput/588/candidates/0002/alignment.paf",
      "differenceTsvUrl": "stageOutput/588/candidates/0002/reference_difference.tsv"
    },
    {
      "referenceId": 37182,
      "referenceAccession": "CUSTOM_3ff7eb2486494d67baa01cf100a5e59a",
      "sourceDb": "USER_UPLOAD",
      "taxId": 2000000000,
      "organismName": "COMPARISON_TEST_ONLY_f1a3e01562874e16b7fa018a74741550_unrelated",
      "segment": "1",
      "segmentKey": "SEGMENT_1",
      "status": "NO_MATCH",
      "sequenceIdentity": 0,
      "referenceCoverage": 0,
      "alignedReferenceBases": 0,
      "referenceBases": 6000,
      "matchingBases": 0,
      "alignmentBlockBases": 0,
      "alignmentCount": 0,
      "alignmentPafUrl": "stageOutput/588/candidates/0003/alignment.paf",
      "differenceTsvUrl": "stageOutput/588/candidates/0003/reference_difference.tsv"
    }
  ]
}
```

样例预期展示：

| 候选 | 匹配状态 | 一致性 | 参考覆盖率 |
| --- | --- | --- | --- |
| exact | MATCHED | 100% | 100% |
| partial | MATCHED | 99.775% | 50% |
| unrelated | NO_MATCH | 0% | 0% |

### 9.6 展示、下载与历史兼容

- 页面提供候选总数/匹配数/未匹配数、全部候选表格、JSON/TSV汇总下载和逐候选 PAF/差异 TSV 下载。所有文件对象键都走第 9.3 节的同一下载接口。
- `comparisons[]` 按候选快照顺序返回，不是质量排名。同 accession、同节段仍可能有多行；行标识可使用独立 `alignmentPafUrl`，不要仅用 accession 或 segment。
- `MATCHED` 表示有可用对齐，不代表质量达标或“最优”。不添加“已选中”“最终参考”标签。
- `NO_MATCH` 是正常比较结果，不隐藏该行；PAF 可以为 0 字节，差异 TSV 可以只有表头。这不同于 HTTP 下载失败、工具执行失败。
- 全部候选均 `NO_MATCH` 时，报告仍包含全部候选，`matchedCandidateCount=0`，`unmatchedCandidateCount=candidateCount`。Stage/Pipeline 可以正常完成，页面显示“比较完成，未获得匹配”，不表示样本质量合格。
- 新版 JSON 已包含结构化比较指标，但没有 `score`、`selected`、`variantCount` 或深度指标。逐位置比对图、差异数量和类型统计仍需解析每个候选的 PAF / 差异 TSV，不能从 `alignmentCount` 或参考选择分数代替推导。
- 历史结果缺少 `schemaVersion`，且带 `pafPath` / `differenceTsv` 时，使用旧下载分支。这两个值仍是对象键；旧数据没有的指标显示“历史结果未提供”，不要补 0 或伪造候选列表。
- 未知的显式版本（如未来的 `schemaVersion=3`）不自动当成历史格式，应提示暂不支持的结果版本。坏 JSON、缺失必要字段也不是合法历史结果。
- 新建 FASTA 任务只展示 Comparison；历史任务列表可能仍包含 Selection，按接口实际返回的阶段展示，不改写旧任务。

历史索引样例（同样是解析 `stage.outputUrl` 后的对象）：

```json
{
  "pafPath": "stageOutput/901/alignment.paf",
  "differenceTsv": "stageOutput/901/reference_difference.tsv"
}
```

汇总 TSV 的真实分隔符为 Tab，字段顺序如下：

```text
reference_id  reference_accession  source_db  tax_id  organism_name  segment  segment_key  status  sequence_identity_percent  reference_coverage  aligned_reference_bases  reference_bases  matching_bases  alignment_block_bases  alignment_count
```

前端本地旧版第 5.5 节中的“只有单份 PAF/TSV、没有独立指标 JSON”不再描述新版结果；请用上述契约替换或补齐该节，保留历史兼容说明。

补充资料（并非本节接入的前置依赖）：[完整结果展示说明](./pipeline-results-visualization-guide.md#55-参考比较)、[病毒创建页面说明](./virus-pipeline-creation-guide.md)、[参考库前端交接](./reference-library-frontend-handoff.md)。本文说明当前源码和已执行测试的结果，不保证前端联调环境已经部署该版本。
