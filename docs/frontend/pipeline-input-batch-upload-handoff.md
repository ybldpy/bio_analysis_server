# 流水线批量上传与 sequenceLevel 前端变更说明

更新日期：2026-10-25。状态：后端已修正为仅支持首次完整上传，本次单元/回归与隔离真实 HTTP 测试通过；现有业务服务尚未由本轮操作重启/部署。

本文可单独交给前端 agent，说明新增批量上传入口的参数和交互约定。前端可按本文实现表单与请求封装，在确认目标后端已部署/重启到本次修正版本后启用入口。不能把隔离环境测试通过理解为 `wsl.test:8080` 已更新。现有单文件上传接口保持不变。

## 本次变更

新增的 `POST /pipelineInput/uploadBatch` 用一个请求提交一条流水线的完整样本输入。清单顶层增加必填 `sequenceLevel`，整批统一选择 reads 或 assembly，不再逐文件传 `fileType`。

本接口**仅用于首次完整上传，没有替换已有文件的功能**。只有尚未启动且尚无任何样本输入的流水线可用；已有任意 `fileRole = 10/11` 输入时拒绝，不追加、不替换、不复用，也不能切换已保存输入的类型。即使此前通过旧接口只上传了 R1，也不能调用此接口补充 R2。

本次上传是一个整体：所有文件及数据库提交都成功，才算上传成功；任意一个文件失败，整批失败，不产生本批次的有效输入。不能因为 R2 失败就让本次 R1 单独生效。用户只能在提交前切换本地表单的 reads/assembly、单文件/双端选项；上传成功后不能通过本接口修改。

这不是新增创建流水线的参数，也不改变测序平台 `sequencingPlatform`、专项 `taxId` 或参考范围 `enabledReferenceIds` 的现有配置。

## 接口和字段

```text
POST /pipelineInput/uploadBatch
Content-Type: multipart/form-data
```

请求由一个名为 `manifest` 的 JSON Part 和若干文件 Part 组成。`pipelineId`、`sequenceLevel` 和输入清单都放在 `manifest` 中，不放在 URL 或单独表单字段中。

| 字段 | 位置 | 约定 |
| --- | --- | --- |
| `pipelineId` | `manifest` 顶层 | 已创建、尚未启动且没有任何样本输入的流水线 ID |
| `sequenceLevel` | `manifest` 顶层 | 必填整数，`0 = READ`，`1 = ASSEMBLY` |
| `inputs` | `manifest` 顶层 | 本次完整的样本输入列表，不可为空 |
| `inputKey` | `inputs[]` | 文件槽位，如 `sample/0/0` |
| `filePart` | `inputs[]` | 对应文件 Part 的名称，如 `r1` |

`0` 是有效的 READ 值，不是 UNKNOWN，不能用 `if (!sequenceLevel)` 判断未选择。应显式校验它是否为数字 `0` 或 `1`；`null`、漏传、`-1`、`2`、`10`、`11` 等均不合法。表单控件返回字符串时，先进行明确的枚举转换，不把空值转换成 `0`。

每个输入项必须对应本次上传的一个非空文件，Part 名称不能重复、缺失或多余。新接口不接受项级 `fileType`、旧记录 `inputFileId`、存储路径或只有清单没有文件的请求。文件名来自文件 Part，无需再传 `fileName` 字段。

`filePart` 使用英文字母开头，其余为英文字母、数字、下划线或短横线，总长 1～64 字符；不能命名为 `manifest`。JSON 不接受未知字段、重复字段或字符串/小数形式的整数参数，亦不接受额外文本表单字段和 URL 参数。

`manifest` 最多 1 MiB，必须为 `application/json`；支持普通 JSON Part 和浏览器 JSON Blob。不要直接 `body.append("manifest", JSON.stringify(manifest))`，这会成为普通文本字段；使用下方 Blob 示例。

旧 `POST /pipelineInput/upload` 仍使用其原有参数和 `fileType`，不要将新协议直接套在旧接口上。

## 输入形式与示例

以下为首次上传的请求协议示例，`pipelineId` 使用当前尚未启动且没有样本输入的任务真实 ID。

### 双端 reads

`manifest`：

```json
{
  "pipelineId": 123,
  "sequenceLevel": 0,
  "inputs": [
    { "inputKey": "sample/0/0", "filePart": "r1" },
    { "inputKey": "sample/0/1", "filePart": "r2" }
  ]
}
```

同时提交文件 Part `r1`、`r2`。R1 和 R2 都是 READ，不使用不同的 `sequenceLevel` 区分两端。

### 单文件 reads

```json
{
  "pipelineId": 123,
  "sequenceLevel": 0,
  "inputs": [
    { "inputKey": "sample/0/0", "filePart": "reads" }
  ]
}
```

提交文件 Part `reads`。单文件可为单端或交错输入，后续 ReadInspect 按现有逻辑处理。不能仅提交 R2 槽位，也不能把长短读、测序平台当作 `sequenceLevel`。

### Assembly

```json
{
  "pipelineId": 123,
  "sequenceLevel": 1,
  "inputs": [
    { "inputKey": "sample/0/0", "filePart": "assembly" }
  ]
}
```

提交文件 Part `assembly`。首期单样本 assembly 只使用一个槽位，不提交 R2。所有文件必须符合顶层声明，不能混合 reads 和 assembly。

后缀校验忽略大小写：READ 支持 `.fastq/.fq`，ASSEMBLY 支持 `.fasta/.fa/.fna/.contig/.contigs`；均可附加 `.gz/.gzip/.zip`。后端原样保存文件，不在上传时解压或完整扫描序列内容。

文件名不能含路径分隔符、控制字符或以点开头；原名最长 512 字符。后端会去掉首尾空白并将空白替换为下划线，处理后的名称不得超过 255 UTF-8 字节。保存后按响应 `fileName` 展示，不自行推导服务器路径。

### 浏览器 FormData 示例

下面构造双端上传请求体。`r1File`、`r2File` 为用户选择的完整 `File` 对象。

```javascript
function buildPairedUploadBody(pipelineId, r1File, r2File) {
  const manifest = {
    pipelineId,
    sequenceLevel: 0,
    inputs: [
      { inputKey: "sample/0/0", filePart: "r1" },
      { inputKey: "sample/0/1", filePart: "r2" }
    ]
  };

  const body = new FormData();
  body.append(
    "manifest",
    new Blob([JSON.stringify(manifest)], { type: "application/json" })
  );
  body.append("r1", r1File, r1File.name);
  body.append("r2", r2File, r2File.name);
  return body;
}
```

实际发请求时将该 `FormData` 作为 body。不要把整个请求 JSON 序列化，也不要手动设置请求的 `Content-Type: multipart/form-data`；应由浏览器生成包含 boundary 的请求头。项目请求封装若默认设置 `application/json`，需对此请求取消该默认值；认证信息仍按原有方式携带。

## 首次上传与页面行为

- 仅允许尚未启动且没有任何样本输入的流水线首次上传，当前 Pipeline 的 PENDING 值为 `3`；这是流水线状态，不是 Stage 状态。
- 首次提交必须包含完整清单和全部文件，不是增量添加。已有任意样本输入时关闭本入口，包括旧接口只上传过 R1 的情况；后端会返回业务拒绝 `status = 1`。
- 提交前可以在本地表单切换双端、单文件、reads、assembly，并选择与最终声明一致的全部文件。上传成功后，本接口不提供修改、重新上传或切换已保存类型的功能。
- 不提供按旧 ID 复用文件的操作，不通过先删后传规避首次上传限制，也不引入删除或重置接口。
- 上传中禁用本页面重复保存与启动。可以显示传输进度，但进度达到 100% 不等于业务保存成功，仍需等待提交结果。
- 明确失败后整批显示失败；确认仍无样本输入且允许上传时，才重新提交完整清单和全部文件。不仅重传失败文件，不把双端请求降级为单端，也不拆成多个旧上传请求。
- 超时、断网、主动取消请求或无法判断提交结果的服务端错误，不代表后端一定回滚。先刷新输入状态，再决定是否重试。
- 上传成功不自动启动分析，仍沿用单独的启动操作。

原有 `fileRole = 20` 参考输入不属于样本输入，既不阻止首次样本上传，也不会被本接口修改或删除；参考库配置同样不受影响。当前仅支持 `pipelineType = 100`（常规病毒）、`101`（新冠）、`200`（细菌）；其他类型（包括污水专项、多样本 SNP）及 `parentPipelineId > 0` 的系统派生任务会被拒绝，不要开放新入口。

## 返回值与状态处理

首次双端上传成功响应示例：

```json
{
  "status": 0,
  "data": {
    "pipelineId": 123,
    "sequenceLevel": 0,
    "inputs": [
      { "inputFileId": 103, "inputKey": "sample/0/0", "fileName": "sample_R1.fastq.gz" },
      { "inputFileId": 104, "inputKey": "sample/0/1", "fileName": "sample_R2.fastq.gz" }
    ]
  },
  "failMsg": null
}
```

必须检查 `Result.status === 0`，不能仅凭 HTTP 200 判定成功。成功后以 `data.sequenceLevel` 和 `data.inputs` 展示已保存输入，并关闭本接口的再次上传入口。返回的 `inputFileId` 只标识记录，不用于下一次请求复用旧文件。响应没有 `removedInputFileIds` 字段，因为本接口不删除或替换记录。

失败时 `data = null`，展示 `failMsg`。状态码为：`1` 业务限制（包括已有任何样本输入）、`2` 内部失败、`3` 当前任务忙或事务复核时输入状态已变化、`4` 参数不合法。Service 业务/参数/忙碌结果使用 HTTP 200，内部失败使用 HTTP 500；控制器识别的非法 JSON/Part 结构返回 HTTP 400 和 `status = 4`。multipart 解析或请求超限也可能在控制器外返回 HTTP 400/413，响应可能不是 JSON，需要保留通用错误提示。

数据库提交状态未知时也可能返回 `status = 2`，并提示“提交结果尚未确认”。此时先刷新状态，不自动重传或显示“本次一定未保存”；后端会保留可能已生效的文件，避免误删。查询到样本输入时按已上传展示；只有确认仍无样本输入且允许上传，才可整批重试。成功后盲目重传将被拒绝，不会替换第一次提交的结果。

## 刷新已有输入

沿用 `POST /pipeline/getPipelineStates`，JSON 请求体为 `[123]`，读取 `data[].pipelineInputs`。该查询的旧字段 `fileRole` 不改变，前端需要显式转换：

| 查询中的 `fileRole` | 新表单的 `sequenceLevel` |
| --- | --- |
| `10` | `0`，READ |
| `11` | `1`，ASSEMBLY |
| `20` | 参考输入，不参与样本级别推断 |

不能把 `10`、`11` 直接回填给 `sequenceLevel`。没有样本输入时保留未选择状态；有任何 `10/11` 输入时只展示已保存信息，不开放本接口再次上传。历史样本输入同时出现 `10`、`11` 时提示类型冲突，不任取一种，也不通过本接口覆盖修正。流水线状态也可通过 `GET /pipeline/getPipeline?pipelineId=123` 查询。

当前单文件限制为 `1024MB`、整请求限制为 `1200MB`，多个文件合计仍受总量约束，并应考虑 multipart 开销和代理限制。

## 前端接入检查

- `sequenceLevel = 0` 可正常提交，空值及非法枚举不可提交。
- 双端、单文件 reads、assembly 的清单和文件 Part 一一对应。
- 新请求不携带 `fileType` 或旧 `inputFileId`，`manifest` 使用 JSON Part。
- 已有任何样本输入时不开放本入口，不能追加、替换、复用或切换已保存输入的类型。
- 任一文件失败显示整批失败，不把已传完的一份当作保存成功；只清理本次失败上传的新文件，不修改已有输入。
- 进度完成后继续等待业务结果；超时或取消后先查询状态。
- 后端上线并完成联调前不启用新入口；遇到 404 或新接口失败时，不自动拆成旧接口调用。

## 本次修正的后端验证状态

本次“仅首次完整上传”修正后重新通过 225 项测试，其中 11 项为隔离环境真实 HTTP + MySQL + 本地存储集成测试，共发送 30 次请求。覆盖首次双端/单文件/assembly 上传、已有样本时拒绝、仅有参考输入时允许、Blob 清单、普通/gzip/ZIP 文件，以及文件写入和数据库插入失败后的整批回滚。其余测试包含参数校验、并发许可、旧接口兼容及提交结果未知处理。该结论来自本次重新执行，不沿用之前含替换逻辑版本的测试统计。

测试仅针对接口、事务与存储，没有运行分析任务或修改现有业务数据库记录；独立测试 MySQL 已停止。本机请求/响应证据：[result.json](../../target/batch-http/r-16006314763503247052/result.json)。当前互斥机制适用于单实例部署；本轮未变更上传大小配置。前端 UI 联调与现有业务服务部署仍需在本次修正版本启动后进行。

后端完整设计见[流水线输入首次批量上传设计](../pipeline-input-batch-upload-plan.md)。本文已包含前端所需协议，可独立转交。
