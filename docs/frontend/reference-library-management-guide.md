# 参考序列库管理：前端接口说明

更新日期：2026-10-24。本文对应当前源码中的 `ReferenceLibraryController`、`ReferenceLibraryService` 和管理 DTO，包含专项范围查询、上传归属确认；不代表已部署或完成线上联调。实施范围见[参考序列库追加与启停计划](../reference-library-management-plan.md)，专项页面交互见[前端交接说明](./reference-library-frontend-handoff.md)。

2026-10-24 的本地 Spring Boot 随机端口真实 HTTP 验证共 23 项通过：3 项明文／gzip／ZIP 成功，20 项拒绝（含重复 accession），并核对详情、查询、下载及数据库／文件哈希；仅关闭调度器与派发器。该结果不覆盖专项成员归属正向流程、并发、前端联调或生产部署，详见[已验证范围与待验证事项](./reference-library-frontend-handoff.md#11-已验证范围与待验证事项)。

## 1. 页面范围与关键约定

本页面管理 `bio_reference_sequence`，提供查询、上传追加、启用和禁用。旧接口 `POST /referenceGenome/queryReferenceGenomes` 及其旧参考表语义不变，不应拿旧接口刷新本管理列表。

- 本期前端只管理病毒参考，不显示“病毒／细菌”类别选择器；管理列表显式传 `orgType=0`，上传固定传 `orgType=0`。后端全局接口仍支持 `0` 病毒、`1` 细菌，专项上传只允许 `0`；这不是将后端接口整体收窄为病毒。任务创建的完整候选查询须遵循下文白名单说明，不复用管理列表的 `orgType` 筛选。
- **一份 FASTA 对应一条参考记录。** 文件内可有多条 FASTA 记录，后端保留在同一文件，不按 header 自动拆成多条候选；不同节段需要分别上传，并明确填写相应元数据。
- 上传只追加，不覆盖同 accession 的记录或原文件；本期没有编辑原序列、替换文件、上传注释文件或物理删除接口。
- `enabled` 是“是否允许后续候选加载使用”，不是上传进度、审核状态、质量结论或访问权限。管理查询默认同时返回启用和禁用记录。
- 管理页的 `enabled` 是全局设置；普通病毒／新冠任务还可通过创建参数 `enabledReferenceIds` 进一步缩小本次任务的候选范围，不会修改参考记录的全局 `enabled`。
- 已经保存参考快照的任务继续使用原快照。禁用不会删除文件，不会替换旧任务引用，也不会在 Executor 中增加实时启停校验。
- 页面可提供“参考列表 / 上传参考 / 详情 / 启停开关”。没有异步上传任务状态接口，上传接口成功返回才表示本次追加完成。
- 专项入口复用 `GET /pipeline/virusTargets` 下发的 `taxId + pipelineType`，在管理请求中写为 `targetTaxId + pipelineType`；实际参考 `taxId` 不改变含义。前端不维护目标到成员的字典，也不提供成员级启停。

### 给前端的功能说明：参考库更新与候选范围配置

后端已支持参考基因序列库的追加更新，以及按物种／分类配置可用于分析的参考序列范围。请在前端增加“参考序列库管理”页面，提供以下两项功能；这里描述的是待接入的页面能力，不代表前端页面已经实现。

**1. 更新参考序列库**

用户可以上传新的 FASTA 文件，并填写该参考实际所属的 `taxId`，以及分类名称 `organismName`、参考标识 `accession`、节段 `segment` 等信息。其中 `file`、`taxId` 必填，`orgType` 由前端固定提交 `0`（病毒），不让用户选择类别；名称等字段目前可选。上传成功后新增一条参考记录，不覆盖已有序列；一份文件里的多条 FASTA 记录仍属于同一条库记录。上传弹窗提供“导入后启用”选项，默认开启，也允许先保存为禁用状态。

**2. 配置该分类下允许使用的参考范围**

用户可选择专项，让后端展开成员后查询参考，也可按参考记录的实际 `taxId` 精确筛选；管理列表默认包含已启用和已禁用的记录。列表建议展示分类名称、taxId、accession、来源、长度、节段和启用状态，并为每条记录提供“允许作为分析候选”开关。

- 开启：`enabled=true`，后续候选加载允许纳入该记录。
- 关闭：`enabled=false`，后续候选加载排除该记录；文件与记录仍然保留，之后可以重新启用。
- 这是在配置**允许参与选择的候选范围**，不是指定最终必须使用的参考；最终选择仍由流水线根据输入样本完成。

例如，同一 taxId 下有 A、B、C 三条参考，启用 A、C，禁用 B，则后续按该 taxId 加载的候选只有 A、C。不能把 A、C 显示为“本次分析已选中的参考”。

页面操作与现有接口对应如下，具体请求格式见后文：

| 页面操作 | 接入方式 |
| --- | --- |
| 查看某分类的病毒参考 | 调用 `POST /referenceGenome/library/query`，传实际 `taxId` 及 `orgType=0`，省略 `enabled` 或传 `null` |
| 查看某专项的病毒参考 | 同一查询接口成对传 `targetTaxId + pipelineType`，并传 `orgType=0`，由后端展开成员；省略 `enabled` 或传 `null` |
| 仅看启用／禁用项 | 同一查询接口传 `orgType=0` 及 `enabled=true/false` |
| 上传追加参考 | 调用 `POST /referenceGenome/library/upload` |
| 改变一条参考的候选资格 | 调用 `POST /referenceGenome/library/{referenceId}/enabled` |
| 下载参考 FASTA | 复用 `GET /stageOutput/download`，将 `fastaObjectName` 作为 URL 编码后的 `outputUrl`，见第 4 节 |

**前端需要明确告知用户的范围边界：**

- 本页启停接口是**参考库级的全局设置**，会影响共享此库的后续候选加载，不是当前用户或某一条任务的临时勾选。任务创建默认不需要提交参考 ID 列表；只有用户自定义本次候选范围时才提交 `enabledReferenceIds`，且不能调用全局启停接口来保存该任务的勾选。已有 Stage 保存的候选／参考快照不被追溯更改。
- 本期只有逐条启停接口，没有整组保存或原子批量启停接口。可先采用逐行开关；如前端做多选操作，需要逐条请求并显示各条的成功／失败结果，不能将部分成功提示成“整组保存成功”。
- 不要根据当前分页中已启用的行数判断全库剩余数量。启停接口允许禁用最后一条；普通病毒／新冠流程在启动加载时没有匹配的启用候选会失败，前端应展示服务端提示，不默认改用禁用记录。显式提交任务白名单时还会在创建前校验，创建成功也不能保证启动时参考仍然有效。
- 管理请求的 **`taxId` 仍按实际参考分类精确筛选**；只有成对传入 `targetTaxId + pipelineType` 才展开已配置的专项成员，不递归查询分类树。本次扩展现有查询接口，没有新增候选列表接口；完整候选仍需读取专项查询的全部分页，不能把目标 taxId 的精确查询或一页结果当成全部候选。
- 这些开关仅适用于读取本参考候选库的流程，不是所有工具数据库的统一开关；不会重建细菌分类使用的 Kraken2/FastANI 数据库，也不会修改污水专项使用的服务端配置资源。

可直接用于页面顶部的说明文案：

> 支持上传 FASTA 追加病毒参考基因序列，并按物种／分类启用或禁用参考记录，配置后续分析可参与选择的候选范围。禁用不会删除序列，也不会修改已有任务保存的参考快照；最终使用的参考仍由流水线自动选择。

### 任务创建时的可选候选白名单

普通病毒 `pipelineType=100` 和新冠 `pipelineType=101` 可提交 `enabledReferenceIds`（`List<Long>`），表示本次任务允许参与参考选择的记录 ID 列表，不是最终参考选择结果，也不是优先级排序。字段名固定为 `enabledReferenceIds`，不是 `candidateReferenceIds`。

- 单个创建：`pipelineStageParameters.enabledReferenceIds`，与 `taxId` 同级，不放在 `extraParams` 内。
- 批量创建：每项的 `pipelines[i].pipelineParameters.enabledReferenceIds`，各任务独立配置。
- 不传或传 `null`：启动时继承当前目标范围内所有全局启用的参考；前端未开启自定义时可直接使用此默认方式。
- 显式列表：必须非空，且每项为不重复的正整数 ID；`[]` 是错误，不代表默认范围。每个 ID 都必须存在、全局启用，且属于当前目标解析出的参考成员范围；任何一项不满足条件都拒绝整份列表，不取交集、不跳过错误项，也不退回默认范围。服务端保留请求顺序，但顺序不代表匹配质量或优先选中。
- 其他流水线类型（包括 `102`）不支持该参数；非 `null` 值会被拒绝，不会静默忽略。

交互上可从目标的全局启用候选开始，允许用户取消本次不使用的项，再提交剩余 ID；不能让用户借此启用全局禁用项或加入目标范围外的参考。只改变任务参数，不调用 `/referenceGenome/library/{referenceId}/enabled`，也不改变其他任务的候选范围。

候选初始化使用 `POST /referenceGenome/library/query`，成对传所选 `targetTaxId + pipelineType`，并传 `enabled=true`，读取全部分页。不要附加 `taxId`、`orgType` 或 accession 等会缩小范围的条件来初始化完整候选；专项查询不会自动过滤 `orgType`，与任务候选范围一致。这里与本期病毒管理列表固定传 `orgType=0` 的用途不同，不能复用管理列表的筛选条件。`GET /pipeline/virusTargets` 仍不返回成员或参考列表；不能把当前页的默认勾选保存成完整白名单。无法完整读取或部署尚不支持专项查询时，保留省略／`null` 的默认方式，不声称已展示全量候选。

单个和批量创建会在插入任务前校验显式列表及目标成员归属，但待启动任务只保存这些 ID，不提前保存完整参考快照。启动时重新校验全局启用状态和成员关系，任一项发生失效即失败；此时才把有效参考转换为 Stage 快照。已有 Stage 的重试继续使用原快照，不因全局开关或成员调整重新筛选。新冠始终按后端固定目标 `2697049` 校验，不受请求中其他 `taxId` 影响。完整请求示例见[病毒流水线创建页面调整说明](./virus-pipeline-creation-guide.md)。

## 2. 接口与响应包装

管理接口前缀：`/referenceGenome/library`。下载复用现有 `/stageOutput/download`，不在此前缀下，见第 4 节。

| 方法与路径 | 请求 | 成功时的 `data` |
| --- | --- | --- |
| `POST /referenceGenome/library/query` | JSON 查询条件，可省略请求体或传 `{}` | 分页对象 |
| `GET /referenceGenome/library/{referenceId}` | 路径中的正整数 ID | 单条参考对象 |
| `POST /referenceGenome/library/upload` | `multipart/form-data`，文件及独立表单字段 | 新增的参考对象 |
| `POST /referenceGenome/library/{referenceId}/enabled` | JSON：`{"enabled":true}` 或 `{"enabled":false}` | 更新后的参考对象 |

正常业务响应使用以下结构：

```json
{
  "status": 0,
  "data": {},
  "failMsg": null
}
```

前端必须同时检查 HTTP 状态及响应体 `status`，不能把 HTTP 200 一律当作成功。

| 业务 `status` | 含义 | 这些管理接口的典型 HTTP 状态 |
| --- | --- | --- |
| `0` | 成功 | `200` |
| `1` | 专项上传需要归属确认、记录不存在、文件不存在不能启用、更新未完成等业务失败 | `200` |
| `2` | 内部错误；也可能是导入提交状态待确认 | `500` |
| `3` | accession 已存在，未覆盖原记录或文件 | `200` |
| `4` | 参数无效、文件读取或格式/大小校验失败 | 通常 `200`；请求体/字段类型绑定错误为 `400` |

错误时通常为 `data=null`。服务端对外返回可展示的简化错误提示，不返回 SQL、堆栈或内部文件路径；前端作为普通文本展示 `failMsg`，不要作为 HTML 渲染。

仅“需要专项归属确认”的响应扩展 `code` 和 `confirmation` 字段，见第 5 节；普通响应结构不变，不要求其他响应带这两个字段。

## 3. 列表与筛选

`POST /referenceGenome/library/query`，`Content-Type: application/json`。

```json
{
  "start": 0,
  "pageSize": 20,
  "taxId": 123,
  "orgType": 0,
  "enabled": null,
  "accession": null
}
```

示例中的 `123` 仅说明字段类型，实际分类编号由用户核对后填写。

| 字段 | 规则 |
| --- | --- |
| `start` | 从 0 开始的行偏移量，不是页码；默认 `0`，必须非负 |
| `pageSize` | 默认 `20`，范围 `1–100` |
| `taxId` | 可选正整数，按参考记录的实际 taxId 精确筛选；同时传专项上下文时为专项范围内的进一步筛选 |
| `targetTaxId` | 可选正整数，与 `pipelineType` 成对提供；使用该目标解析出的成员范围 |
| `pipelineType` | 与 `targetTaxId` 成对提供，仅 `100`／`101`；组合必须已登记，`101` 的目标必须为 `2697049` |
| `orgType` | 后端可选：`0` 病毒、`1` 细菌；本期病毒管理列表固定传 `0`，不显示类别选择器；不是流水线 `pipelineType` |
| `enabled` | `true` 仅启用、`false` 仅禁用；省略或 `null` 表示全部 |
| `accession` | 可选，去除两端空白后精确匹配，最长 64；不是模糊搜索 |

后端固定按 `referenceId` 降序，前端无需提交排序表达式。所有已传筛选条件一起生效。部分运行时错误提示仍使用旧类别枚举描述，前端不要从 `failMsg` 推断枚举或改用旧值；实际取值以当前 `RefSeqService` 常量和本文 `0`／`1` 约定为准。

病毒管理页按专项查询示例（`700001` 仅为结构示例，不是可直接使用的目标；不是任务完整候选初始化请求）：

```json
{
  "targetTaxId": 700001,
  "pipelineType": 100,
  "start": 0,
  "pageSize": 20,
  "orgType": 0,
  "enabled": null
}
```

- 后端核对 `bio_pipeline_target` 中的目标／类型组合，然后按任务候选加载的同一规则解析成员：存在显式映射时仅使用映射成员；无映射时使用目标自身。
- 解析后的成员 `IN` 条件与实际 `taxId`、`orgType`、`enabled`、accession 条件使用同一组 `AND` 过滤，并在此范围内计数和分页；不会先分页再在内存裁剪。
- 后端不隐式增加 `orgType=0`。本期病毒管理页须显式传该条件；任务完整候选查询则必须省略它，避免缩小流水线候选范围。
- `bio_pipeline_target.enabled` 仅控制选项展示，已登记但被隐藏的组合仍可用于专项请求；通常从 `GET /pipeline/virusTargets` 的启用选项中选择。
- 只提供半组上下文、不支持的类型、不存在的组合或错误的 `101` 目标都报错，不退回全库。目录／成员解析失败也不能按“无配置”继续。
- 两个专项字段均不传时，维持原有全库／精确 taxId 查询。响应仍是以下分页对象和单条参考字段，不增加成员列表。

```json
{
  "status": 0,
  "data": {
    "total": 1,
    "start": 0,
    "pageSize": 20,
    "items": [
      {
        "referenceId": 501,
        "accession": "LOCAL_DEMO_001",
        "sourceDb": "USER_UPLOAD",
        "taxId": 123,
        "orgType": 0,
        "organismName": "示例分类名称",
        "genomeLength": 22,
        "segment": null,
        "completeness": null,
        "isAnnotated": false,
        "enabled": true,
        "fastaObjectName": "refseq/custom/ccfe48b9-a477-4eab-9d11-5fbeb70e943a/reference.fna"
      }
    ]
  },
  "failMsg": null
}
```

没有匹配项时为 `status=0`、`total=0`、`items=[]`。翻页时按行偏移更新 `start`；筛选条件改变后重置为 `0`。

## 4. 详情与返回字段

`GET /referenceGenome/library/501` 的成功 `data` 与列表中的单条对象结构相同。禁用记录仍可查看详情。

| 字段 | 前端含义 |
| --- | --- |
| `referenceId` | 参考库记录 ID，详情与启停操作使用它 |
| `accession` | 记录唯一标识；可由用户提交，也可由服务端生成 |
| `sourceDb` | 来源标记；本接口新增记录固定为 `USER_UPLOAD` |
| `taxId`、`orgType`、`organismName` | 用户声明的分类元数据；上传不会联网核实或自动推断 |
| `genomeLength` | 上传 FASTA 中所有记录的核酸字符总数，由后端计算 |
| `segment` | 用户声明的节段标记，可为空 |
| `completeness` | 用户填写的文本，可为空；不是后端自动判定结果 |
| `isAnnotated` | 新上传固定为 `false`；本接口不接收注释文件 |
| `enabled` | 当前参考记录是否启用 |
| `fastaObjectName` | 相对存储对象键，不是 HTTP URL 或服务器绝对路径 |

新上传的对象键格式为 `refseq/custom/{服务端UUID}/reference.fna`，与用户文件名、accession、ZIP entry 路径无关。历史记录没有路径，或路径为绝对路径、非规范路径、包含路径穿越等无效键时，`fastaObjectName` 返回 `null`；前端显示“文件位置不可用”，不要自行拼接或猜测路径。

响应不包含 `rawMetadata`、内部服务器路径、注释路径、SHA-256 或 FASTA 记录数；不要假定这些字段已经可供前端展示。记录数与文件 SHA-256 由服务端计算并保存在内部导入元数据中。

### 下载参考 FASTA

复用已有 `GET /stageOutput/download?outputUrl=<URL 编码后的 fastaObjectName>`，不新增参考库专用下载接口。路径以项目现有 API base URL 配置和请求封装为准，不硬编码域名，也不把对象键直接设为链接 `href`、当成完整 URL 或拼成服务器路径。

- `fastaObjectName` 非空时，把完整对象键作为 `outputUrl` 的值，使用 `URLSearchParams` 或 `encodeURIComponent` 编码一次；不编码整个下载 URL。
- `fastaObjectName=null` 时禁用下载并显示“文件位置不可用”；不能猜测旧文件位置。
- 成功响应是文件流（`application/octet-stream`），不是管理接口的 `Result` JSON。下载请求仍需先检查 HTTP 状态；若返回 JSON 错误，则解析并展示可用的 `failMsg`；非 JSON 的 HTTP／网关错误使用文本或通用失败提示，不能直接下载成 `.fna` 文件。只有确认成功文件响应后才触发浏览器保存。
- 此接口同样使用现有 API 访问配置和错误处理；不能因管理列表可见就声称文件一定存在或下载一定成功。

## 5. 上传追加

`POST /referenceGenome/library/upload`，采用 `multipart/form-data`。

文件字段固定为 `file`，元数据是其他独立表单字段，**不是一个 JSON 字符串 part**。

| 表单字段 | 是否必填 | 规则 |
| --- | --- | --- |
| `file` | 是 | 非空 FASTA 或其支持的压缩文件 |
| `taxId` | 是 | 正整数；由用户明确提供 |
| `orgType` | 是 | 本期前端固定传 `0`（病毒），不提供类别选择器；后端全局上传仍支持 `0` 病毒、`1` 细菌，专项上传只允许 `0` |
| `accession` | 否 | 去除两端空白后最长 64；首字符为字母或数字，后续只允许字母、数字、`.`、`_`、`:`、`-`；必须唯一 |
| `organismName` | 否 | 最长 255 |
| `segment` | 否 | 最长 64 |
| `completeness` | 否 | 最长 32，自由文本；本期没有额外枚举约束 |
| `enabled` | 否 | `true` 或 `false`；不传默认 `true` |
| `targetTaxId` | 否 | 与 `pipelineType` 成对提供的目标正整数 taxId，不替代实际 `taxId` |
| `pipelineType` | 否 | 与 `targetTaxId` 成对提供，仅 `100`／`101`；组合必须已登记，`101` 的目标必须为 `2697049` |
| `confirmAddTaxonomyMember` | 否 | 布尔值，默认 `false`；用户明确确认扩展专项范围后才传 `true`，无专项上下文时传 `true` 会报错 |

可选文本字段会去除两端空白，空白视为未填写，非空值不允许控制字符。前端不要发送字符串 `"null"` 代替未填写，应省略该字段。

未提供 accession 时，服务端生成 `CUSTOM_` 加 32 位 UUID 十六进制字符的本地标识，例如 `CUSTOM_a88642a57f144771973f36efdc6f737a`。这不是向 NCBI 申请得到的 accession。`sourceDb=USER_UPLOAD`、`referenceId`、`genomeLength`、对象键、注释状态及原始元数据均由服务端决定，不能通过表单指定。

浏览器示例（变量值来自实际表单）：

```javascript
const form = new FormData();
form.append("file", selectedFile);
form.append("taxId", String(taxId));
form.append("orgType", "0"); // 本期只上传病毒参考，不读取类别选择值。
form.append("enabled", String(enabled));
if (accession.trim()) form.append("accession", accession.trim());
if (organismName.trim()) form.append("organismName", organismName.trim());
if (segment.trim()) form.append("segment", segment.trim());
if (completeness.trim()) form.append("completeness", completeness.trim());

const response = await fetch("/referenceGenome/library/upload", {
  method: "POST",
  body: form
});
// 不手动设置 Content-Type，浏览器会生成 multipart boundary。
// 后续同时检查 HTTP 状态、可解析的响应体以及业务 status。
```

成功时 `data` 是新增的单条参考对象，不是后台任务 ID。前端可刷新列表或打开返回的 `referenceId` 详情；提交期间禁止重复点击。

### 专项上下文与分类归属确认

无专项上下文时，后端全局上传仍支持 `orgType=0`／`1`，不新增成员关联；本期前端无论是否提供专项上下文都固定提交 `0`。有专项上下文时，后端要求 `orgType` 必须为 `0`，目标组合校验规则与查询相同；已登记但展示禁用的目标仍有效，不将目标 `enabled` 作为权限开关。

- 实际 `taxId` 已属于当前专项解析范围：正常上传，不重复新增成员。
- 实际 `taxId` 不在范围内且 `confirmAddTaxonomyMember` 为 `false` 或未传：预检在文件写入存储前返回以下确认响应，不新增参考或成员。
- 用户确认后保留原文件、实际 `taxId` 及专项上下文，用 `confirmAddTaxonomyMember=true` 重提；普通成功时才刷新列表。
- 确认会把该实际分类下的所有已有及未来参考纳入目标范围，不是只关联当前文件；实际参与分析仍受参考全局 `enabled` 和任务白名单限制。
- 成员映射按目标 taxId 共享，因此也影响使用同一目标 taxId 的其他流水线选项。此操作不新增专项下拉项，也不提供成员级启停。

HTTP `200` 的确认响应示例：

```json
{
  "status": 1,
  "data": null,
  "failMsg": "该参考分类尚未纳入当前专项，请确认是否加入专项分类范围",
  "code": "REFERENCE_TARGET_MEMBERSHIP_CONFIRMATION_REQUIRED",
  "confirmation": {
    "targetTaxId": 700001,
    "pipelineType": 100,
    "taxId": 700002
  }
}
```

示例 `failMsg` 只用于展示。前端精确判断 `code === "REFERENCE_TARGET_MEMBERSHIP_CONFIRMATION_REQUIRED"`，核对 `confirmation` 与当前表单一致后提示影响范围；用户同意后才重提，不按提示文本猜测、不对任意失败自动传 `true`。更换专项或实际 taxId 后须重新确认。运行时仅该确认响应使用 `ReferenceMembershipConfirmationResult` 子类，其他 `Result` 包装以及分页／单条参考 DTO 不变。

后端在数据库事务中按目标 taxId 锁定对应的全部目标目录行，并重新校验专项和成员归属，预检后发生范围变化也不会绕过确认。新参考与新增成员在同一数据库事务内提交；无显式映射时首次加入其他分类会同时保留原目标自身成员，已有非自身的显式范围则不擅自补入目标自身。重复 accession 不覆盖、不重新分类，也不通过本接口给原记录补关联。

文件存储不属于数据库事务：明确回滚时补偿清理本次文件，提交结果不明时保留文件并按第 7 节核对 accession。未登记专项或目录／成员读取失败均直接报错，不扩大范围继续上传。

### 文件格式和大小

- 支持明文 `.fasta`、`.fa`、`.fna`，以及 `.gz`、`.gzip`、`.zip`，后缀大小写不敏感。
- ZIP 沿用“一槽位一个文件”约定：跳过目录，只使用第一个非目录 entry，不继续挑选后续文件。请上传单文件 ZIP；第一个文件不合法时不会自动改用第二个。
- 多记录 FASTA 的 header 必须包含非空 ID，文件内 ID 不得重复；每条记录必须有非空序列。序列允许大小写 IUPAC 核酸字符及 `U`，不允许空格、gap、数字等非法字符；内容按 UTF-8 读取，不自动改写 header 或碱基。
- accession 与 FASTA header 中的记录 ID 是两回事，生成本地 accession 不会重命名 header。以后需要组合使用的多个参考文件也应避免记录 ID 重复；本次上传不扫描全库执行跨文件重命名或去重。
- 服务端默认上限为 **256 MiB，即 268435456 字节**，由 `reference-library.max-upload-bytes` 配置。整个上传字节流及解压后字节流都受限，压缩文件小于上限不代表一定能导入；ZIP 被跳过目录 entry 的数据也占用解压预算。
- 另有 HTTP multipart/代理限制。仓库当前 `application.yml` 配置单文件 `1024MB`、总请求 `1200MB`，部署可覆盖；有效限制取相关约束中更严格者，HTTP 上限并不会取消参考导入的 256 MiB 上限。
- HTTP 容器或网关在进入接口前拒绝超大请求时，响应可能不是上述 `Result` JSON；前端应兼容非 JSON 错误及超限提示，不依赖固定业务码。

格式检查仅检查文件结构及字符，不验证分类、完整性、节段归属或生物学准确性，也不会从文件内容自动填 taxId、orgType、segment。

## 6. 启用与禁用

```http
POST /referenceGenome/library/501/enabled
Content-Type: application/json

{"enabled":false}
```

重新启用时提交 `{"enabled":true}`，必须明确传 JSON 布尔值，不能省略或传 `null`。

- 启用前检查引用的文件是否存在且可读取；本地存储还要求规范对象键、普通文件，不接受目录或符号链接。缺失或不可读时返回 `status=1`，不会报告启用成功。即使记录已经是启用状态，提交 `true` 也会执行检查；此处不重新扫描整个 FASTA 或判断内容质量。
- 禁用只更新本条记录的 `enabled`，不删除 FASTA，不修改其余元数据或已有任务快照。
- 当前状态与目标一致且相关检查通过时，仍返回 `status=0`，可以重复提交。
- 修改成功后可先用响应对象更新当前行，但必须按当前筛选条件、当前分页重新查询并同步 `items` 和 `total`；仅更新开关会在“仅启用／仅禁用”筛选下留下不匹配的行和错误总数。若末页变空且 `start>0`，回退一页并重新查询，必要时继续回退，直到有效页或首页。
- 请求失败时恢复开关原状态，并重新拉取列表／详情确认服务端状态，不把本地乐观状态当作服务端结果。若启停成功但列表刷新失败，明确提示刷新失败，不把旧列表当作已同步状态。
- 没有版本号或并发比较字段供前端提交；不要宣称该开关提供独占编辑或并发冲突检测。

新候选加载对 exact taxId / taxId IN 查询都附加 `enabled=true`。待启动任务的显式 `enabledReferenceIds` 不能绕过此全局设置；启动前禁用其中任一记录会导致白名单重新校验失败，而不是删除该项后继续。如果某任务的 Stage 此前已经保存参考列表或参考快照，之后禁用记录不追溯修改它，已有 Stage 重试仍使用原快照。

## 7. 导入失败、重复与提交待确认

常见提示处理：

| 情况 | 前端处理 |
| --- | --- |
| `status=4` 参数/文件校验失败 | 保留表单，提示检查格式、压缩内容、必填字段及大小；不要当作导入成功 |
| `status=3` accession 已存在 | 告知未覆盖，允许按 accession 查找现有记录；若确实是另一份参考，应由用户确认新的唯一标识 |
| `status=1` 且 `code` 精确为 `REFERENCE_TARGET_MEMBERSHIP_CONFIRMATION_REQUIRED` | 核对确认上下文，展示扩展范围影响，用户同意后才保留原字段并传确认标志重提 |
| `status=1` 记录或文件不可用 | 刷新详情/列表，显示 `failMsg` |
| `status=2` 常规内部错误 | 显示服务端简化提示，允许用户查询确认后决定是否重试 |
| 提示“导入提交状态待确认，请按 accession 查询后再重试” | 暂停自动重传，使用该 accession 精确查询，确认是否已经落库 |

提交状态待确认示例：

```json
{
  "status": 2,
  "data": null,
  "failMsg": "导入提交状态待确认，请按 accession 查询后再重试：LOCAL_DEMO_001"
}
```

随后查询：

```json
{
  "start": 0,
  "pageSize": 20,
  "accession": "LOCAL_DEMO_001"
}
```

这里必须省略专项上下文和其他限制性筛选，`enabled` 省略或传 `null`，避免漏掉已落库或导入为禁用状态的记录；这是提交状态核对，不是把正常专项查询失败降级成全库查询。失败响应没有 `data.accession`；使用用户原先填写的 accession，或提示中给出的服务端生成标识。不要仅因 HTTP 500、网络断开或超时就自动重新上传：事务结果不明确时，后端会保留可能已经被记录引用的文件。如果网络中断导致自动生成的 accession 也未收到，可刷新近期列表并交由使用者确认，不能把重传当成幂等操作。

## 8. 不自动联动的内容与访问边界

- 新增参考不会自动修改目标选项表 `bio_pipeline_target`，也不会新增前端病毒下拉项。
- 全局上传、启停和专项内已有成员的上传不会新增 `bio_reference_taxonomy_member`；只有专项上传发现实际 taxId 不在范围内，且用户确认后，才与新参考一起提交新增成员。管理列表的 `taxId` 仍为实际分类精确筛选，`targetTaxId + pipelineType` 才解析专项范围。能在全库管理页看到一条记录，不代表每个目标都会加载它。
- 该操作不重建、不增补 FastANI/Kraken2 等独立分析数据库，也不会改变其他专项由服务端配置管理的资源。
- 本期没有新增管理员角色或鉴权体系。当前 `SecurityConfig` 仍为 `anyRequest().permitAll()`；隐藏前端按钮不构成权限控制。管理页面与接口应仅在受控内部环境或已有网络/网关访问控制下开放，不应直接暴露为无需外层访问控制的公网管理接口。

## 9. 对照源码

- [ReferenceLibraryController](../../src/main/java/com/xjtlu/bio/controller/ReferenceLibraryController.java)：路由、multipart 绑定、HTTP 状态。
- [ReferenceLibraryService](../../src/main/java/com/xjtlu/bio/service/ReferenceLibraryService.java)：筛选、元数据约束、追加与启停语义。
- [RefSeqService](../../src/main/java/com/xjtlu/bio/service/RefSeqService.java)：实际类别常量 `0` 病毒、`1` 细菌。
- [StageOutputController](../../src/main/java/com/xjtlu/bio/controller/StageOutputController.java)：复用的 `GET /stageOutput/download` 文件下载接口。
- [ReferenceLibraryItem](../../src/main/java/com/xjtlu/bio/response/ReferenceLibraryItem.java) / [ReferenceLibraryPage](../../src/main/java/com/xjtlu/bio/response/ReferenceLibraryPage.java)：响应字段。
- [ReferenceMembershipConfirmationResult](../../src/main/java/com/xjtlu/bio/response/ReferenceMembershipConfirmationResult.java)：仅归属确认分支的机器码与上下文响应。
- [ReferenceFastaUpload](../../src/main/java/com/xjtlu/bio/service/reference/ReferenceFastaUpload.java)：格式、大小及文件准备规则。
- [PipelineService](../../src/main/java/com/xjtlu/bio/service/PipelineService.java)：新候选加载中的 enabled 过滤。
