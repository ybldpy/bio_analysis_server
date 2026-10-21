# 参考序列库管理：前端接口说明

更新日期：2026-10-22。本文对应当前 `ReferenceLibraryController`、`ReferenceLibraryService` 和管理 DTO；实施范围见[参考序列库追加与启停计划](../reference-library-management-plan.md)。

## 1. 页面范围与关键约定

本页面管理 `bio_reference_sequence`，提供查询、上传追加、启用和禁用。旧接口 `POST /referenceGenome/queryReferenceGenomes` 及其旧参考表语义不变，不应拿旧接口刷新本管理列表。

- **一份 FASTA 对应一条参考记录。** 文件内可有多条 FASTA 记录，后端保留在同一文件，不按 header 自动拆成多条候选；不同节段需要分别上传，并明确填写相应元数据。
- 上传只追加，不覆盖同 accession 的记录或原文件；本期没有编辑原序列、替换文件、上传注释文件或物理删除接口。
- `enabled` 是“是否允许后续候选加载使用”，不是上传进度、审核状态、质量结论或访问权限。管理查询默认同时返回启用和禁用记录。
- 已经保存参考快照的任务继续使用原快照。禁用不会删除文件，不会替换旧任务引用，也不会在 Executor 中增加实时启停校验。
- 页面可提供“参考列表 / 上传参考 / 详情 / 启停开关”。没有异步上传任务状态接口，上传接口成功返回才表示本次追加完成。

## 2. 接口与响应包装

接口前缀：`/referenceGenome/library`。

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
| `1` | 记录不存在、文件不存在不能启用、更新未完成等业务失败 | `200` |
| `2` | 内部错误；也可能是导入提交状态待确认 | `500` |
| `3` | accession 已存在，未覆盖原记录或文件 | `200` |
| `4` | 参数无效、文件读取或格式/大小校验失败 | 通常 `200`；请求体/字段类型绑定错误为 `400` |

错误时通常为 `data=null`。服务端对外返回可展示的简化错误提示，不返回 SQL、堆栈或内部文件路径；前端作为普通文本展示 `failMsg`，不要作为 HTML 渲染。

## 3. 列表与筛选

`POST /referenceGenome/library/query`，`Content-Type: application/json`。

```json
{
  "start": 0,
  "pageSize": 20,
  "taxId": 123,
  "orgType": 1,
  "enabled": null,
  "accession": null
}
```

示例中的 `123` 仅说明字段类型，实际分类编号由用户核对后填写。

| 字段 | 规则 |
| --- | --- |
| `start` | 从 0 开始的行偏移量，不是页码；默认 `0`，必须非负 |
| `pageSize` | 默认 `20`，范围 `1–100` |
| `taxId` | 可选正整数，按参考记录的 taxId 精确查询；不展开目标成员映射 |
| `orgType` | 可选：`1` 病毒、`2` 细菌；不是流水线 `pipelineType` |
| `enabled` | `true` 仅启用、`false` 仅禁用；省略或 `null` 表示全部 |
| `accession` | 可选，去除两端空白后精确匹配，最长 64；不是模糊搜索 |

后端固定按 `referenceId` 降序，前端无需提交排序表达式。所有已传筛选条件一起生效。

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
        "orgType": 1,
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

本期管理接口没有专用下载接口，不把 `fastaObjectName` 直接设置为链接的 `href`。响应也不包含 `rawMetadata`、内部服务器路径、注释路径、SHA-256 或 FASTA 记录数；不要假定这些字段已经可供前端展示。记录数与文件 SHA-256 由服务端计算并保存在内部导入元数据中。

## 5. 上传追加

`POST /referenceGenome/library/upload`，采用 `multipart/form-data`。

文件字段固定为 `file`，元数据是其他独立表单字段，**不是一个 JSON 字符串 part**。

| 表单字段 | 是否必填 | 规则 |
| --- | --- | --- |
| `file` | 是 | 非空 FASTA 或其支持的压缩文件 |
| `taxId` | 是 | 正整数；由用户明确提供 |
| `orgType` | 是 | `1` 病毒，`2` 细菌 |
| `accession` | 否 | 去除两端空白后最长 64；首字符为字母或数字，后续只允许字母、数字、`.`、`_`、`:`、`-`；必须唯一 |
| `organismName` | 否 | 最长 255 |
| `segment` | 否 | 最长 64 |
| `completeness` | 否 | 最长 32，自由文本；本期没有额外枚举约束 |
| `enabled` | 否 | `true` 或 `false`；不传默认 `true` |

可选文本字段会去除两端空白，空白视为未填写，非空值不允许控制字符。前端不要发送字符串 `"null"` 代替未填写，应省略该字段。

未提供 accession 时，服务端生成 `CUSTOM_` 加 32 位 UUID 十六进制字符的本地标识，例如 `CUSTOM_a88642a57f144771973f36efdc6f737a`。这不是向 NCBI 申请得到的 accession。`sourceDb=USER_UPLOAD`、`referenceId`、`genomeLength`、对象键、注释状态及原始元数据均由服务端决定，不能通过表单指定。

浏览器示例（变量值来自实际表单）：

```javascript
const form = new FormData();
form.append("file", selectedFile);
form.append("taxId", String(taxId));
form.append("orgType", String(orgType));
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
- 修改后使用响应中的对象更新当前行，必要时刷新列表。失败时恢复开关显示并刷新确认，不把本地乐观状态当作服务端结果。
- 没有版本号或并发比较字段供前端提交；不要宣称该开关提供独占编辑或并发冲突检测。

新候选加载对 exact taxId / taxId IN 查询都附加 `enabled=true`。如果某任务此前已经保存参考列表或参考快照，之后禁用记录不追溯修改它；是否使用禁用前的快照，以该任务已有状态为准。

## 7. 导入失败、重复与提交待确认

常见提示处理：

| 情况 | 前端处理 |
| --- | --- |
| `status=4` 参数/文件校验失败 | 保留表单，提示检查格式、压缩内容、必填字段及大小；不要当作导入成功 |
| `status=3` accession 已存在 | 告知未覆盖，允许按 accession 查找现有记录；若确实是另一份参考，应由用户确认新的唯一标识 |
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

这里必须省略 `enabled` 或传 `null`，避免漏掉导入为禁用状态的记录。失败响应没有 `data.accession`；使用用户原先填写的 accession，或提示中给出的服务端生成标识。不要仅因 HTTP 500、网络断开或超时就自动重新上传：事务结果不明确时，后端会保留可能已经被记录引用的文件。如果网络中断导致自动生成的 accession 也未收到，可刷新近期列表并交由使用者确认，不能把重传当成幂等操作。

## 8. 不自动联动的内容与访问边界

- 新增参考不会自动修改目标选项表 `bio_pipeline_target`，也不会新增前端病毒下拉项。
- 新增/启停参考不会自动修改 `bio_reference_taxonomy_member`。管理列表的 taxId 是精确查询；普通病毒的候选选择仍按已有成员映射或 exact 回退执行。能在管理页看到一条记录，不代表每个目标都会加载它。
- 该操作不重建、不增补 FastANI/Kraken2 等独立分析数据库，也不会改变其他专项由服务端配置管理的资源。
- 本期没有新增管理员角色或鉴权体系。当前 `SecurityConfig` 仍为 `anyRequest().permitAll()`；隐藏前端按钮不构成权限控制。管理页面与接口应仅在受控内部环境或已有网络/网关访问控制下开放，不应直接暴露为无需外层访问控制的公网管理接口。

## 9. 对照源码

- [ReferenceLibraryController](../../src/main/java/com/xjtlu/bio/controller/ReferenceLibraryController.java)：路由、multipart 绑定、HTTP 状态。
- [ReferenceLibraryService](../../src/main/java/com/xjtlu/bio/service/ReferenceLibraryService.java)：筛选、元数据约束、追加与启停语义。
- [ReferenceLibraryItem](../../src/main/java/com/xjtlu/bio/response/ReferenceLibraryItem.java) / [ReferenceLibraryPage](../../src/main/java/com/xjtlu/bio/response/ReferenceLibraryPage.java)：响应字段。
- [ReferenceFastaUpload](../../src/main/java/com/xjtlu/bio/service/reference/ReferenceFastaUpload.java)：格式、大小及文件准备规则。
- [PipelineService](../../src/main/java/com/xjtlu/bio/service/PipelineService.java)：新候选加载中的 enabled 过滤。
