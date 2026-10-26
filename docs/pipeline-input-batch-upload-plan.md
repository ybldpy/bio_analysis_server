# 流水线输入首次批量上传设计

日期：2026-10-25。状态：已修正为仅支持首次完整上传，本次单元/回归与隔离真实 HTTP 测试通过；尚未重启或部署现有业务服务。

保留现有 `POST /pipelineInput/upload` 单文件接口，新增 `POST /pipelineInput/uploadBatch`。新接口通过一次 multipart 请求提交完整输入清单及其全部文件。本次上传是一个原子批次：所有文件校验、写入和数据库提交均成功才算成功；任一文件或提交步骤失败，整批失败，不允许部分文件单独生效。

本接口仅用于没有任何样本输入的流水线首次上传，**没有替换已有文件的功能**。流水线已有任意 `fileRole IN (10, 11)` 的输入记录时直接拒绝，不追加、不替换、不复用，也不能切换已保存输入的类型。首次上传失败时，不产生任何有效输入；不能因为 R2 上传失败就保留本次 R1 并自动按单端提交。

本文记录修正后的接口约定，供后端维护和前端接入使用。独立批量入口及外层并发协调不修改旧单文件上传和启动方法体，不修改数据库表结构或生成的 Mapper。前端须在后端部署/重启到本次修正版本后启用入口。

前端可单独接收[批量上传与 sequenceLevel 变更说明](frontend/pipeline-input-batch-upload-handoff.md)，其中包含请求构造、完整示例及上线前限制。

## 目标与范围

- 一个请求只操作一条流水线，不同时创建流水线或启动分析。
- 仅允许尚未启动且没有任何样本输入的流水线首次上传，即当前 `PipelineService.PIPELINE_STATUS_PENDING = 3` 且不存在 `fileRole IN (10, 11)` 的记录；不要与 Stage 的 PENDING 值混淆。
- 清单表示提交后完整的样本输入集合，不是增量追加列表。
- 清单顶层通过 `sequenceLevel` 统一声明整批输入为 reads 或 assembly，不在每个文件上重复声明类型。
- 支持首次上传双端 reads、单文件 reads 或 assembly。用户可以在提交前切换本地表单选项；上传成功后不能通过本接口更改文件、单双端或输入级别。
- 首次提交完整的一组文件，不提供追加、替换、按旧记录 ID 复用文件或只调整关联的局部编辑模式。即使只有通过旧接口上传的 R1，也不能用本接口补充 R2。
- 仅操作样本输入，不修改参考库、专项配置、`enabledReferenceIds` 或其他流水线参数。
- 首期不引入上传会话表、分片上传、断点续传或跨请求的多阶段提交。

首期支持 `pipelineType = 100`（常规病毒）、`101`（新冠）和 `200`（细菌）。新接口明确拒绝其他类型，包括污水专项和多样本 SNP；这些类型继续使用原入口及其自身输入限制。`parentPipelineId > 0` 的系统派生子任务不开放此接口。

## 现有实现与兼容约束

现有单文件入口位于 `PipelineInputController.sampleUpload()`，保存逻辑位于 `PipelineInputService.createInputs()`。旧实现每次写一个存储对象、插入一条输入记录，不具备多文件统一提交语义。

新接口不能简单循环调用旧接口：如果第一个文件成功、第二个失败，就已经产生部分有效输入。应使用独立的批量 Service，复用实体、Mapper 和存储能力，而不是复用单文件保存流程。

旧接口的地址、参数、响应和单文件业务逻辑保持不变。新旧接口与启动入口仍需在外层共用并发协调机制，详见“并发与启动互斥”；只给新接口加锁不足以保证一致性。

## 请求协议

### 请求地址与组成

```text
POST /pipelineInput/uploadBatch
Content-Type: multipart/form-data
```

| Part | 类型 | 用途 |
| --- | --- | --- |
| `manifest` | `application/json` | 必填，描述本次整批上传的完整输入集合 |
| 清单中 `filePart` 指定的名称 | 文件 | 每个输入项都必须提供对应文件，每个名称只能出现一次 |

服务端使用明确的请求 DTO 解析清单。文件 Part 按名称关联，不能依赖 multipart 的排列顺序；`manifest` 为保留名称，不可用作文件 Part 名称。

`manifest` 上限为 1 MiB，支持普通 JSON Part 和浏览器 JSON Blob（带 `filename="blob"`），Content-Type 均须为 `application/json`。`filePart` 限定为英文字母开头、后续为英文字母/数字/下划线/短横线，总长 1～64 字符。重复 JSON 字段、未知字段、额外文本字段或 URL 参数均拒绝，不进行宽松类型转换。

### 清单字段

```json
{
  "pipelineId": 123,
  "sequenceLevel": 0,
  "inputs": [
    {
      "inputKey": "sample/0/0",
      "filePart": "r1"
    },
    {
      "inputKey": "sample/0/1",
      "filePart": "r2"
    }
  ]
}
```

| 字段 | 约定 |
| --- | --- |
| `pipelineId` | 已创建、尚未启动、尚无任何样本输入且允许当前用户操作的流水线 ID |
| `sequenceLevel` | 顶层必填整数；`0` 为 READ，`1` 为 ASSEMBLY，适用于本批次所有文件 |
| `inputs` | 本次上传的完整样本输入列表，不接受空列表 |
| `inputKey` | 沿用槽位规则，如 `sample/0/0`、`sample/0/1` |
| `filePart` | 必填，值为本次请求中对应文件 Part 的名称 |

每个输入项都必须通过 `filePart` 提供本次上传的文件；请求不接受 `inputFileId` 复用旧记录，也不接受只有清单而没有文件的请求。新文件名取自 multipart 的原始文件名，经后端校验和安全处理后使用，不额外要求前端提交存储路径。

`sequenceLevel` 沿用 `Constants.SequenceInput` 的取值。这里的 `0` 是 READ，不是 UNKNOWN；省略、`null`、UNKNOWN 值 `-1`、REFERENCE 值 `2` 及其他值均不接受。请求 DTO 应使用 `Integer` 配合必填和取值校验，避免漏传时因基本类型默认 `0` 被当作 READ。

新接口不再使用逐文件 `fileType`，若请求仍传该字段，应明确拒绝，不能让它与顶层声明产生冲突。后端在写入输入记录时显式转换：

| 请求 `sequenceLevel` | 含义 | 数据库 `fileRole` |
| --- | --- | --- |
| `0` | 原始 reads | `10` |
| `1` | 已组装序列或 contigs | `11` |

不能直接把 `sequenceLevel` 的 `0`、`1` 写入 `fileRole`。旧单文件接口继续接收原来的 `fileType = 10/11/20`，不受影响；其中 `20` 是参考输入，不是 R2。新接口不接收参考输入，R1 和 R2 通过 `inputKey` 区分。

`sequenceLevel` 不代表单双端、长短读或测序平台：READ 可以是单端、双端或交错输入，相关检测仍交给后续 ReadInspect。

### 首次上传双端文件

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

同时提交文件 Part `r1` 和 `r2`。两个文件及数据库变更全部成功后，这组输入才生效。任一文件缺失、校验失败或写入失败，整批失败；即使另一份文件已写入暂存位置，也不能产生有效输入记录，需清理本批次文件。

### 首次上传单文件 reads

```json
{
  "pipelineId": 123,
  "sequenceLevel": 0,
  "inputs": [
    { "inputKey": "sample/0/0", "filePart": "singleRead" }
  ]
}
```

同时提交文件 Part `singleRead`，作为完整的首次单文件输入。它要求该流水线尚无任何样本输入，不能用于将已保存的双端输入改为单端。

用户可以在尚未提交时将表单从双端改为单文件。这不是双端上传失败后的自动降级：如果提交的是双端清单，不能跳过失败的 R2 后继续提交 R1。

### 首次上传 assembly

```json
{
  "pipelineId": 123,
  "sequenceLevel": 1,
  "inputs": [
    { "inputKey": "sample/0/0", "filePart": "assembly" }
  ]
}
```

将 `sequenceLevel` 设为 `1`，并提交文件 Part `assembly`。它要求该流水线尚无任何样本输入，成功后保存一条 assembly 输入；不能用来将已保存的 reads 改为 assembly。提交前可以修改本地表单选项，但必须同时选择与声明匹配的文件。

## 首次上传与校验规则

对于首期单样本流水线，`sequenceLevel = 0` 时必须包含 `sample/0/0`，可选 `sample/0/1`；`sequenceLevel = 1` 时仅使用 `sample/0/0`。所有文件必须符合顶层声明的输入级别，同一批不能混合 reads 与 assembly，也不能只有 R2。

单文件 reads 可以是单端或交错格式，仍由后续 ReadInspect 按现有逻辑判断；上传清单不代替内容检测。本次按文件名后缀校验：READ 为 `.fastq/.fq`，ASSEMBLY 为 `.fasta/.fa/.fna/.contig/.contigs`，均可附加 `.gz/.gzip/.zip`，忽略大小写。文件原样存储，不在上传时解压或增加全文件测序内容扫描。

文件名不能含路径分隔符、控制字符或以点开头；原名最长 512 字符，去除首尾空白并将空白替换为下划线后的文件名不得超过 255 UTF-8 字节。响应 `fileName` 是处理后的名称，原名另存 `originalFilename`。

整体校验还包括：

- 流水线存在、尚未启动、尚无任何样本输入且属于本接口支持范围，权限校验沿用项目规则。
- `sequenceLevel` 必须显式提供且为 `0` 或 `1`，并满足流水线类型限制；不根据文件名为缺失或非法的级别补默认值。
- `inputKey`、`filePart` 不重复，实际文件 Part 与清单一一对应；缺失或未引用的文件均导致整单拒绝。
- 不接受请求中的 `inputFileId`，不从历史上传记录补齐本次缺失的文件，也不因其中一份失败而缩减清单继续提交。
- 新文件非空，文件名格式与顶层 `sequenceLevel` 兼容，压缩输入按现有规则识别其序列格式后缀；不支持的格式或级别不匹配均整批拒绝。安全处理路径分隔符及异常名称，不接受客户端指定绝对路径或存储对象键。
- 已有任何样本输入即拒绝，不论槽位、文件名或输入级别是否与本次请求相同；不能用新批次追加旧输入，也不能借首次上传入口进行替换。

是否已上传样本按当前流水线 `fileRole IN (10, 11)` 的全部输入记录判断。已有任何一条即返回 `Result.status = 1`，不写入本次文件、不修改已有记录。`fileRole = 20` 等非样本记录保持原样，也不阻止首次样本上传。整批成功时只在同一事务中插入本次全部新样本记录，不删除任何旧记录。

`sequenceLevel` 只声明本批次输入，随整批事务通过 `fileRole` 映射生效。启动时仍由已提交输入记录确定输入级别，无需新增数据库列。不在上传接口中创建 Stage，也不改写已经创建的分析配置。前端提交前的临时选择不会改变后端状态；保存后本接口不提供修改路径。

## 原子提交与文件存储

### 提交流程

1. 获取当前流水线的批量操作独占许可，读取并校验流水线与完整清单，确认不存在任何样本输入。
2. 为本次提交分配服务器生成的批次 ID，将所有新文件写入独立对象键。
3. 全部新文件写入成功后，开启短数据库事务，再次核对流水线状态及样本输入仍为空，插入本批次全部新记录；任一数据库操作失败，事务整体回滚。若复核时出现样本输入，返回忙碌状态 `3` 并补偿清理本次文件，不修改并发出现的输入。
4. 数据库事务提交后，本批次输入集合生效，返回完整输入列表。没有删除旧记录或清理旧文件的步骤。
5. 在 `finally` 中释放本次真正获取到的操作许可。

不能逐文件提交数据库记录，也不能捕获某个文件的错误后跳过它、继续保存剩余文件。文件写入成功不等于单项业务成功；只有整批事务提交成功，才能返回本次上传成功。

大文件写入期间不持有数据库事务。新存储路径采用：

```text
pipelineInput/p<pipelineId>/batches/<batchUuid>/<itemUuid>/<safeFileName>
```

保留真实格式后缀，使用现有 `StorageService.putObject(key, stream, options)` 的非覆盖写入能力。唯一批次路径避免覆盖已有对象，也避免失败后重试与并发请求互相删除对象。

### 失败处理

| 失败位置 | 有效输入处理 | 文件处理 |
| --- | --- | --- |
| 清单校验失败，或流水线已有样本输入 | 不新增、不修改任何输入 | 不写业务存储对象 |
| 任一文件校验或写入失败 | 整批失败；本次不产生任何有效输入 | 尽力删除本次已写入及部分写入的对象 |
| 数据库操作失败且确认回滚 | 本批次没有有效输入；其他记录不变 | 尽力删除本次新对象 |
| 数据库提交结果不明确 | 先核对数据库实际关联，不宣称已回滚 | 暂不删除可能已生效的新对象 |
| 回滚后的本次文件清理失败 | 不将文件残留视为有效输入 | 记录清理失败，不影响已有业务记录 |
| 响应丢失或客户端超时 | 以服务端实际提交状态为准 | 前端先查询，不假定已回滚 |

这里的“原子”指本批次有效输入全部生效或全部不生效，不是数据库与文件系统之间的分布式事务。文件清理仅用于失败补偿，不属于输入是否生效的判据：进程崩溃或清理失败可能留下未引用文件，但不能让半组新输入进入启动流程。清理只能针对明确属于本次操作的新对象，不删除其他上传的文件，不递归删除流水线公共目录。

## 并发与启动互斥

新批量上传、旧单文件上传和流水线启动必须按同一 `pipelineId` 协调。旧实现已经有上传计数和启动锁，但不能直接把它们当作批量事务锁。

已新增外层协调器及 Service 调用拦截器，保留旧方法体，使用以下许可规则：

| 操作 | 许可 | 同一流水线上的关系 |
| --- | --- | --- |
| 旧 `createInputs()` | 共享许可 | 保留旧接口不同槽位并发上传能力，槽位冲突仍由旧逻辑判断 |
| 新批量 Service | 独占许可 | 与旧上传、其他批量上传、启动互斥 |
| `startPipeline()` | 独占许可 | 与所有上传互斥 |

冲突时快速返回，不在服务端长时间等待。新批量接口使用 `Result.DUPLICATE_OPERATION = 3`；旧入口按其现有契约适配，例如旧上传遇到启动占用仍返回 `status = 201`、`data = -1`，不能统一改成 `3`。旧上传遇到新增批量操作占用属于新场景，可返回原有的忙碌码 `3`。协调器应记录独占操作类别，以便准确区分这些情况。

不同流水线之间不互相阻塞。许可必须覆盖整个调用及其异常处理，只由获取成功的调用释放；锁对象生命周期需安全管理，不能随意移除仍被其他线程使用的锁。

当前旧上传存在一个并发细节：遇到启动占用标记 `-1` 后即使提前返回，仍会进入递减上传计数的 `finally`。因此，新入口只复用该标记并不能安全互斥。外层统一协调应在旧方法执行前拦住冲突，不能让批量操作的占用被旧上传错误释放。

旧上传与启动通过 Spring Advisor 在方法外协调，许可覆盖提交/回滚。`PipelineService` 内部存在合并任务的 `startPipeline()` 自调用，普通代理不会拦截它；本方案不开放系统派生任务的批量上传。日后新增上传或启动入口时，也必须接入同一协调器，不得绕过它。

上述方案针对单实例部署；多实例需要所有入口共同使用跨实例协调，不能把 JVM 锁描述为集群级保证。

## 响应与前端交互

### 成功响应

沿用项目 `Result` 包装，`data.inputs` 返回首次提交后的完整样本输入集合，不包含保留不动的参考输入等其他角色。下面是首次双端上传成功的示例：

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

成功响应的 `data.sequenceLevel` 对应已经提交的完整样本输入集合，文件项不再重复返回 `fileType`。没有 `removedInputFileIds` 字段，因为本接口不删除或替换旧记录。新接口不返回“部分成功”状态或部分成功的输入列表。结果不必暴露物理存储路径；响应中的输入记录 ID 仅标识已提交记录，不是后续批量请求中的复用参数。

### 失败响应

| `status` | 含义 |
| --- | --- |
| `1` | 业务限制，例如已有任意样本输入、流水线已经启动或不支持该输入类型 |
| `2` | 存储或数据库等内部失败 |
| `3` | 当前流水线正在上传或启动，或事务复核时输入状态发生变化；先刷新状态 |
| `4` | 清单、槽位、文件引用等参数不合法 |

失败时 `data` 为 `null`，`failMsg` 提供可理解的原因，不暴露服务器异常栈。业务失败沿用 HTTP 200 加 `Result.status`；内部失败使用 HTTP 500。非法 multipart/JSON、请求超限等可能在进入控制器前返回 HTTP 400/413，前端也需处理非 `Result` 响应。

### 前端操作约定

1. 读取流水线状态及已有输入。只有尚未启动且没有任何样本输入时开放本入口；已有任意样本输入时只展示结果，不提供通过此接口追加、替换、切换类型或重新上传的操作。
2. 首次上传前，选择 READ 或 ASSEMBLY 并设置顶层 `sequenceLevel`，选择完整的一组本地文件，在本地构建与文件一一对应的完整清单。此时允许切换表单选项，仅影响本地待提交状态。
3. 点击保存后只发送一次批量请求；上传中禁用本页面的保存和启动按钮。
4. 收到成功响应后，以返回的完整集合更新页面，并关闭本接口的再次上传入口；任一文件或提交失败时，整批显示失败，不把已传完的某个文件标为业务保存成功。
5. 超时、断网或提交结果不明确时，先刷新后端状态。查到样本输入则按已上传展示，不重新调用本接口；只有确认仍无样本输入且允许上传时才重试。

现有 `POST /pipeline/getPipelineStates` 可用于刷新，请求体是流水线 ID 数组，例如 `[123]`，读取 `data[].pipelineInputs`。该查询保持原样，返回实体字段 `fileRole`：样本记录的 `10` 对应新接口 `sequenceLevel = 0`，`11` 对应 `sequenceLevel = 1`；参考角色 `20` 不参与样本级别推断。不能把 `fileRole` 的数值直接回填给 `sequenceLevel`；若历史样本记录同时包含 `10`、`11`，应提示输入类型冲突，不任取其一。`GET /pipeline/getPipeline?pipelineId=123` 可查询流水线状态。

首期不增加请求幂等键或前端版本号；不承诺丢失响应后重传仍返回相同记录 ID。已提交成功后重传会因存在样本输入而拒绝，不能用重传替换或追加。保存前应刷新状态；确认本批次失败且仍没有任何样本输入后，重试需重新提交完整清单和全部文件，不仅重传失败的一份。

当前应用配置单文件上限为 `1024MB`、整请求上限为 `1200MB`。多个文件合计仍受总请求上限约束，并需考虑 multipart 开销和代理限制。一次请求并不自动扩大上传容量，本轮不调整这些配置。

## 已实现文件

以下均为本次新增文件：

| 位置 | 职责 |
| --- | --- |
| 独立 `PipelineInputBatchController` | 新增 `/pipelineInput/uploadBatch`，不改旧控制器接口 |
| `BatchPipelineInputUploadRequest` | 顶层必填 `Integer sequenceLevel`，每个输入项必须对应本次请求的文件，无逐文件 `fileType` |
| `BatchPipelineInputUploadResponse` | 已提交的 `sequenceLevel` 与完整输入列表 |
| `PipelineInputBatchService` | 首次上传校验、文件写入、事务插入与失败补偿 |
| `PipelineInputOperationCoordinator`、`PipelineInputOperationConfiguration` | 协调旧上传、新批量上传和启动，保持旧方法体不变 |
| 对应测试类 | 请求协议、事务、存储补偿、并发和旧接口兼容回归 |

复用 `BioPipelineInputFile` 和现有 Mapper，无需新增表或重新运行 MyBatis Generator。数据库提交使用 `TransactionTemplate` 的独立短事务，不依赖同类自调用触发 `@Transactional`。通过事务完成回调区分已提交、已回滚和结果未知，避免误删可能已经生效的新文件。

## 验收用例

| 场景 | 必须满足的结果 |
| --- | --- |
| 首次双端上传 | 两条记录共同生效，均可读取对应文件 |
| 首次单端或 assembly 上传 | 槽位、输入类型与清单一致 |
| `sequenceLevel` 缺失、为 `null` 或非法值 | 整批拒绝，包括 `-1`、`2`、`10`、`11` 等，不能默认为 READ |
| `sequenceLevel` 为合法的 `0` 或 `1` | 分别保存为 `fileRole = 10` 或 `11`，响应级别与已提交输入一致 |
| 顶层级别与文件格式不符、assembly 提交两个槽位 | 整批拒绝；普通文件及压缩格式后缀均覆盖 |
| 新请求仍传逐文件 `fileType` | 明确拒绝，不接受两处类型声明 |
| 已有单端、双端或 assembly 输入后再次上传 | 返回业务拒绝 `1`，不修改已有记录、不写入本次文件 |
| 旧接口只上传过 R1 后调用本接口 | 拒绝，不补充 R2、不替换 R1 |
| 仅有参考输入记录 | `fileRole = 20` 保留且不阻止首次样本上传 |
| 缺失 Part、重复槽位或 Part、未引用文件、空文件 | 整单拒绝，原输入不变 |
| 仅清单无文件、传旧 `inputFileId`、R2 单独存在、混合 reads 与 assembly | 整单拒绝，不能借旧记录补齐或修改类型 |
| 首次双端上传中任一文件失败 | 整批失败，两份都不形成有效输入，不能自动降级为单端 |
| 任一新记录插入失败 | 数据库整体回滚，不出现半组输入；清理仅限本次新对象 |
| 文件写入后、事务复核时发现其他样本记录 | 拒绝本批次，保留并发出现的记录和文件，补偿清理本次新对象 |
| 失败补偿清理文件失败 | 本批次仍失败且无有效输入，记录清理异常，不误报成功 |
| 新批量与旧上传、启动相互竞争 | 同一流水线正确互斥，被拒绝的调用不释放持有者的许可 |
| 旧接口两个不同槽位并发上传 | 保留原能力，与批量独占操作互斥 |
| 旧上传遇到启动占用 | 保留原 `201` 响应契约，不进入旧方法误释放启动占用 |
| 同一流水线两个批量请求、不同流水线并行 | 前者互斥，后者可并行，异常后许可可正常释放 |
| 已启动流水线、系统派生任务 | 拒绝上传 |
| 响应丢失后查询 | 可以查到实际提交后的完整集合，不依赖客户端猜测 |
| 普通文件、gzip、单文件 ZIP | 格式命名和存储交接沿用现有支持范围，不在上传时拆分双端 |

本次修正已重新执行单元测试、MockMvc 请求协议及隔离环境真实 HTTP 测试，确认已有输入被拒绝、首次批次原子插入、失败仅清理本次文件，以及并发兼容性。测试没有运行分析工具或修改现有业务流水线。

## 本轮验证记录

2026-10-25：本次“仅首次完整上传”修正后共通过 225 项测试。以下为本轮新执行的结果，不沿用之前含替换逻辑版本的统计或证据。

| 测试类 | 数量 | 验证范围 |
| --- | ---: | --- |
| `PipelineInputBatchControllerTest` | 45 | JSON/Blob 清单、严格字段与类型校验、重复/缺失 Part、HTTP 状态 |
| `PipelineInputBatchServiceTest` | 113 | 首次上传限制、类型映射、保存补偿、事务完成状态、未知提交结果核对与文件保留 |
| `PipelineInputOperationCoordinatorTest` | 10 | 共享/独占许可、并发、重复释放与锁生命周期 |
| `PipelineInputOperationConfigurationTest` | 13 | Spring 代理、启动互斥、旧返回码、异常及事务结束后的许可释放 |
| `PipelineInputServiceCovidWastewaterTest` | 33 | 既有上传限制回归（未开放新批量入口） |
| `PipelineInputBatchHttpIntegrationTest` | 11 | 真实 HTTP、MyBatis/MySQL 事务及本地存储，共 30 次 HTTP 请求 |

真实 HTTP 测试覆盖首次双端/单文件/assembly 上传、已有样本拒绝、仅有参考输入时允许、普通文件/gzip/ZIP 字节一致性、同名文件独立路径，以及存储与 SQL 插入失败后的整批回滚。SQL 失败在独立测试库注入，不修改业务数据库。测试结果记录 `isolatedMysqlStopped = true`，独立 MySQL 已停止。

本机留存的本轮请求/响应证据：[result.json](../target/batch-http/r-16006314763503247052/result.json)。该文件在 `target` 下，不作为版本库交付物，运行 `clean` 会移除。

复跑单元和兼容测试（Mockito 代理路径按本机缓存版本调整）：

```bash
mvn -o -Dtest=PipelineInputBatchControllerTest,PipelineInputBatchServiceTest,PipelineInputOperationCoordinatorTest,PipelineInputOperationConfigurationTest,PipelineInputServiceCovidWastewaterTest \
  -DargLine=-javaagent:/home/jcy/.m2/repository/org/mockito/mockito-core/5.17.0/mockito-core-5.17.0.jar \
  -Dmaven.test.redirectTestOutputToFile=true test
```

真实 HTTP 测试需本机网络、MySQL 程序及本地开发库表结构的只读访问；所有写入发生在新建的 `target/batch-http/r-*` 独立测试环境：

```bash
mvn -o -Dtest=PipelineInputBatchHttpIntegrationTest -Dbatch.upload.http.integration=true \
  -DargLine=-javaagent:/home/jcy/.m2/repository/org/mockito/mockito-core/5.17.0/mockito-core-5.17.0.jar \
  -Dmaven.test.redirectTestOutputToFile=true test
```

## 代码定位

- [旧上传控制器](../src/main/java/com/xjtlu/bio/controller/PipelineInputController.java)
- [旧上传 Service 和上传协调状态](../src/main/java/com/xjtlu/bio/service/PipelineInputService.java)
- [输入级别常量](../src/main/java/com/xjtlu/bio/analysisPipeline/Constants.java)
- [流水线启动与状态查询](../src/main/java/com/xjtlu/bio/service/PipelineService.java)
- [存储接口](../src/main/java/com/xjtlu/bio/service/StorageService.java)
- [当前上传大小配置](../src/main/resources/application.yml)
