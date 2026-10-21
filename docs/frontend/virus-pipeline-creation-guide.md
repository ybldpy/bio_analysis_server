# 病毒流水线创建页面调整说明

更新日期：2026-10-21。本文描述当前后端接口和前端接入要求，包含选项查询接口 `GET /pipeline/virusTargets`，以及第 14 节的新冠污水独立入口。

初始化选项和审计快照：[virus-taxonomy-options.json](./virus-taxonomy-options.json)。前端运行时从后端接口获取选项，不把该文件作为实时配置源。

## 1. 调整背景

以前创建普通病毒流水线时，用户需要选择具体的参考记录，例如某个 `referenceId` 或 accession。现在调整为：用户选择要分析的目标分类，提交它的 `taxId`，后端负责加载对应的候选参考记录，并由流水线选择使用的参考。

因此，前端不再要求用户从多条参考序列中手动挑选具体的一条。普通病毒在“通过目标 taxId 创建任务”这一交互方式上与新冠保持一致，但两者的 `pipelineType` 仍然不同。

后端通过 `bio_pipeline_target` 表维护目标名称、taxId、流水线类型、启用状态和展示顺序，并通过查询接口下发选项。

有些业务目标在参考库里对应多个具体分类编号。为了避免前端维护这些成员关系，后端另用 `bio_reference_taxonomy_member` 表维护“目标 taxId → 参考成员 taxId”的映射。

需要区分三个概念：

| 概念 | 用途 | 前端创建时是否提交 |
| --- | --- | --- |
| 目标 `taxId` | 用户选择的分析目标 | 是 |
| `memberTaxIds` | 后端查找参考记录时使用的分类成员集合 | 否 |
| `referenceId` / accession | 具体参考记录的标识 | 否 |

这里的“分析目标”不要求全部处于严格的物种层级；部分选项是具体型别或其他分类层级。页面建议使用“分析目标”或“病毒类型”作为字段标题，不必统一称作“物种”。

## 2. 选项展示规则

### 现有“选择参考基因组”控件如何调整

普通病毒和新冠创建页面中的“选择参考基因组”控件，统一使用后端 `GET /pipeline/virusTargets` 下发的配置作为选项来源。这里选择的是参考所属的分析目标，不是某条具体参考序列；具体候选参考由后端加载和选择。

控件可以保留现有名称，但其数据来源和提交值需要一起调整：

1. 进入创建页面时请求选项接口，只有 HTTP 请求成功且响应体 `status=0` 时，才使用 `data` 填充控件。
2. 每项展示 `label`，以 `taxId` 作为选中值，同时保存该项的 `pipelineType`。不要让用户另外选择与该目标不匹配的流水线类型。
3. 不再从旧的具体参考记录列表或前端静态 JSON 中生成这些病毒选项，也不要把新旧两份选项合并。
4. 用户选定后，按下面的对应关系构造创建请求，不提交具体 `referenceId`、accession 或 `memberTaxIds`。
5. 配置加载中、加载失败或返回空列表时，禁止提交创建，显示相应提示并提供重试。重新加载后，如果原选项已不在返回列表中，应清空该选择，要求重新选择。

| 接口下发字段 | 控件用途 | 单个创建时写入 | 批量创建时写入 |
| --- | --- | --- | --- |
| `label` | 选项显示名称 | 不作为参考参数提交 | 不作为参考参数提交 |
| `taxId` | 选项值 | `pipelineStageParameters.taxId` | `pipelines[i].pipelineParameters.taxId` |
| `pipelineType` | 所选目标配套的流水线类型 | 顶层 `pipelineType` | 顶层 `pipelineType`，不同类型分批 |

这是对病毒／新冠页面的调整，不要求其他分析类型的参考选择控件改用这个接口。

### 列表展示

初始配置包含 67 个最终选项，使用平铺的单选列表或可搜索下拉框。运行时以接口返回的启用项为准，不在前端写死选项数量。

- 页面加载时请求 `GET /pipeline/virusTargets`，读取成功响应中的 `data` 数组。
- 展示文本使用 `label`，选中值使用数字类型的 `taxId`。
- 同时保留所选项的 `pipelineType`，用于创建请求。
- 拆分后的选项与原有独立选项处于同一级，不要求用户先选择父级大类。
- 不把“流感病毒”“腺病毒”等分组标题重新做成可提交的父级选项。
- 本期不提供第 20 项“其他病毒”。
- 不把 `optionKey`、`memberTaxIds` 或具体 `referenceId` 当作选中值。

例如，甲型、乙型、丙型流感是三个独立选项，与新冠病毒等选项同级。

### 选项查询接口

`GET /pipeline/virusTargets`，无需查询参数。身份认证沿用项目既有机制，本次不改变访问权限配置。

成功响应示例（仅展示两项，实际数量由数据库配置决定）：

```json
{
  "status": 0,
  "data": [
    {
      "label": "新冠病毒（SARS-CoV-2）",
      "taxId": 2697049,
      "pipelineType": 101
    },
    {
      "label": "猴痘病毒",
      "taxId": 10244,
      "pipelineType": 100
    }
  ],
  "failMsg": null
}
```

接口只返回已启用的普通病毒和新冠选项，按照 `sort_order`、`id` 升序排列。前端按返回顺序展示即可；不会收到成员列表、数据库 ID、启用状态或时间戳等内部字段。

- 没有启用项：HTTP 200，`status=0`，`data=[]`。页面提示“暂无可选分析目标”并禁止创建。
- 查询失败：HTTP 500，返回 JSON，`status=2`、`data=null` 和可展示的 `failMsg`。
- 接口失败时不要静默回退到旧静态字典，避免重新展示后端已禁用的项。
- 后端没有应用层选项缓存，每次请求读取当前数据库。前端应支持刷新或重新进入页面后重新获取。
- 本次 `enabled` 控制接口展示范围；没有改变旧创建／启动接口的校验规则，不把它当作访问控制或执行许可。

### 静态快照字段如何使用

| 字段 | 前端用途 |
| --- | --- |
| `options[].label` | 初始显示名称，运行时使用接口下发的 label |
| `options[].taxId` | 初始化分析目标编号 |
| `options[].pipelineType` | 初始化流水线类型 |
| `options[].coverageNote` | 初始参考范围的审计说明，不是查询接口返回字段 |
| `options[].optionKey` | 内部追踪标识，不提交给创建接口 |
| `options[].memberTaxIds` | 初始化时的成员审计快照，不作为请求参数 |
| `referenceTaxa` | 来源审计信息，不作为另一组选项 |

运行时的选项和成员关系均以后端对应数据库表为准。修改 JSON 文件不会自动修改数据库或接口结果。

## 3. 流水线类型与 taxId

| 场景 | `pipelineType` | `taxId` |
| --- | --- | --- |
| 普通病毒 | `100` | 接口中所选选项的 `taxId` |
| 新冠病毒 | `101` | `2697049` |
| 新冠污水混合分析 | `102` | 不需要提交；使用后端配置的专项方案，见第 14 节 |

建议直接使用接口中所选项的 `pipelineType` 和 `taxId` 组合，不让用户独立修改两者。

新冠类型在后端固定使用 `2697049`。即使请求传入其他 taxId，也不会把新冠流水线变成另一个目标的分析任务。

`taxId` 没有“传 0 表示未知”的约定。普通病毒创建页面应要求用户选择有效目标，不提交空值、`0` 或父级分组占位值。

## 4. sequencingPlatform 参数

`sequencingPlatform` 表示用户声明的测序平台，与 `taxId` 放在同一层参数对象中。

常用选项如下；完整后端编码以 `Constants.SequencingPlatform` 为准。

| 显示名称 | 提交值 |
| --- | --- |
| 未知／未声明 | `0` |
| Illumina | `1001` |
| BGI / MGI | `1002` |
| Oxford Nanopore（ONT） | `2001` |
| PacBio | `2002` |

前端默认提交 `0`。

- `0`、`null`、不传，均表示没有在这个字段声明平台。
- 没有平台声明时，ReadInspect 会尝试检测；不保证一定识别成功。
- `0` 不表示默认 Illumina。
- 有明确非零声明时，后端优先使用用户声明。
- 新前端统一使用这个字段，不再同时往 `extraParams.sequenceInputMeta.sequencingPlatform` 写一份。
- 后端仍兼容旧位置：如果新字段没有明确声明，而旧位置已有平台声明，仍可能采用旧声明。因此不要同时传入互相冲突的值。

对于 assembly-level 输入，不需要为了创建任务强行推断原始测序平台；未知时传 `0` 即可。

## 5. 单个创建接口

### 请求

`POST /pipeline/create`

Content-Type：`application/json`

```json
{
  "projectId": 123,
  "pipelineType": 100,
  "analysisName": "病毒分析任务01",
  "pipelineStageParameters": {
    "taxId": 11320,
    "sequencingPlatform": 0,
    "extraParams": {}
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `projectId` | 所属项目 ID |
| `pipelineType` | 所选选项对应的流水线类型 |
| `analysisName` | 分析任务名称 |
| `pipelineStageParameters.taxId` | 所选分析目标的 taxId |
| `pipelineStageParameters.sequencingPlatform` | 平台编码，未知默认 `0` |
| `pipelineStageParameters.extraParams` | 其他扩展参数；本次没有额外配置时可传 `{}` |

不要在此请求中增加 `memberTaxIds`、`referenceId` 或 accession 来替代 `taxId`。

新冠示例只需要使用相应的类型和目标：

```json
{
  "projectId": 123,
  "pipelineType": 101,
  "analysisName": "新冠分析任务01",
  "pipelineStageParameters": {
    "taxId": 2697049,
    "sequencingPlatform": 0,
    "extraParams": {}
  }
}
```

### 成功响应

```json
{
  "status": 0,
  "data": 456,
  "failMsg": null
}
```

`data` 是新建的 `pipelineId`。

创建接口只创建任务并保存参数，任务处于待启动状态。它不会自动上传文件，也不会自动启动分析。

## 6. 批量创建接口

### 请求

`POST /pipeline/batchCreatePipelines`

Content-Type：`application/json`

```json
{
  "projectId": 123,
  "pipelineType": 100,
  "pipelines": [
    {
      "pipelineName": "病毒分析任务01",
      "pipelineParameters": {
        "taxId": 11320,
        "sequencingPlatform": 0,
        "extraParams": {}
      }
    },
    {
      "pipelineName": "病毒分析任务02",
      "pipelineParameters": {
        "taxId": 11520,
        "sequencingPlatform": 1001,
        "extraParams": {}
      }
    }
  ]
}
```

注意单个与批量接口的字段名称不同：

| 含义 | 单个创建 | 批量创建中的每个子项 |
| --- | --- | --- |
| 任务名称 | `analysisName` | `pipelineName` |
| 参数对象 | `pipelineStageParameters` | `pipelineParameters` |
| taxId 路径 | `pipelineStageParameters.taxId` | `pipelines[i].pipelineParameters.taxId` |
| 平台路径 | `pipelineStageParameters.sequencingPlatform` | `pipelines[i].pipelineParameters.sequencingPlatform` |

批量请求的 `pipelineType` 对整批共用：

- 同一批普通病毒任务可以分别选择不同的 taxId。
- 普通病毒 `100` 与新冠 `101` 应分批提交。
- `pipelines` 必须是非空数组，每项提供有效名称和参数对象。

### 成功响应

```json
{
  "status": 0,
  "data": [456, 457],
  "failMsg": null
}
```

`data` 为按创建顺序返回的任务 ID 列表。批量创建同样不会自动上传输入或启动任务。

## 7. 创建、上传、启动的调用顺序

正常调用顺序：

```text
选择分析目标和平台
        ↓
创建任务，取得 pipelineId
        ↓
通过现有上传接口上传输入文件
        ↓
调用启动接口
        ↓
使用现有任务／Stage 状态接口观察执行进度
```

### 上传

沿用 `POST /pipelineInput/upload`，Content-Type 为 `multipart/form-data`。

| 表单字段 | 说明 |
| --- | --- |
| `pipelineId` | 创建接口返回的任务 ID |
| `inputKey` | 输入槽位 |
| `fileName` | 文件名 |
| `fileType` | reads 为 `10`，assembly 为 `11` |
| `inputFile` | 文件二进制内容 |

当前常规输入槽位：

| 输入场景 | `inputKey` |
| --- | --- |
| R1 或单端 reads | `sample/0/0` |
| R2 | `sample/0/1` |
| assembly 文件 | `sample/0/0`，同时使用 `fileType=11` |

创建请求不包含上传文件字段。上传成功返回的 `data` 是输入文件 ID，不是新的 pipelineId。

本次目标选择改动不要求用户额外上传参考序列。不要把旧的参考文件上传流程作为此功能的必填步骤。

同一任务、同类型、同槽位重复上传，目前会返回业务状态 `100` 和已有输入 ID，并不会覆盖已有文件。启动或运行中的任务也不能继续按普通待启动任务上传。

### 启动

当前接口为：

```text
GET /pipeline/start?pipelineId=456
```

成功示例：

```json
{
  "status": 0,
  "data": null,
  "failMsg": null
}
```

启动成功不等于分析已经完成。前端应继续使用现有状态查询流程，不要根据 `data` 是否为 `true` 判断启动成功。

## 8. 后端如何查找参考

参考成员在任务启动时查询，不是在创建时查询。

1. 获取任务保存的目标 `taxId`；新冠类型固定为 `2697049`。
2. 查询 `bio_reference_taxonomy_member` 是否配置了该目标的成员。
3. 有成员配置时，仅按这些 `member_tax_id` 查询 `bio_reference_sequence`。
4. 没有成员配置时，按原始目标 `tax_id` 精确查询。
5. 将查询到的参考记录转换为候选参数快照，写入本次任务的 Stage。

例如，前端选择目标 `11320` 时只提交 `11320`；具体查询哪些成员分类由后端表维护，前端不需要展开这些成员。

边界约定：

- 不自动递归展开分类树。
- 有映射时，不隐式把目标自身加入成员列表；如需自身，必须显式配置。
- 映射查询发生数据库异常时，不会当作“没有配置”而悄悄改用其他查询范围。
- 已配置成员但没有查询到参考时，不扩大到其他分类兜底。
- 此映射逻辑适用于普通病毒和新冠；不表示其他流水线也改用了相同规则。

因此，创建成功不保证启动一定成功。参考缺失、数据库异常或输入问题仍可能在启动阶段返回错误。

## 9. 配置维护与生效时间

展示配置由后端维护在 `bio_pipeline_target` 表：

| 字段 | 含义 |
| --- | --- |
| `label` | 下发给前端的名称 |
| `tax_id` | 创建任务时提交的 taxId |
| `pipeline_type` | 与目标配套的流水线类型 |
| `enabled` | 是否出现在选项查询结果中 |
| `sort_order` | 展示顺序，值越小越靠前；相同时按 id |

修改名称、顺序、启用状态后，后续选项请求即可读取新配置，不需要重新发布前端。

成员配置另由 `bio_reference_taxonomy_member` 表维护：

| 字段 | 含义 |
| --- | --- |
| `tax_id` | 前端提交的分析目标 taxId |
| `member_tax_id` | 查询参考库时使用的成员 taxId |
| `sort_order` | 成员读取顺序，不代表匹配质量或优先选中 |

新任务启动时会重新读取映射，无应用层成员缓存。映射调整应在同一事务中完成，避免任务读到一组修改中的不完整成员。

已有 Stage 保存自己的候选快照；修改表不会自动重写已建立的 Stage 参数，也不会自动替换这些 Stage 重试时使用的候选。

目前初始化了 7 组映射，共 18 条成员记录；其余 60 个选项使用精确 taxId 查询。这些数字是 2026-10-20 的初始化状态，不是接口长期不变的限制。

需要分别维护：

- 选项名称、显示顺序或启用状态变化：更新 `bio_pipeline_target`。
- 后端参考成员关系变化：更新 `bio_reference_taxonomy_member`。
- 新增目标时核对两部分配置和相应参考记录，不仅仅新增展示名称。
- 静态 JSON 保留为初始化和审计快照；修改它不会更新后端数据库。

## 10. 前端校验和错误处理

### 提交前

- 检查项目、任务名称和分析目标已填写。
- 从选项接口返回的所选项获取匹配的 `pipelineType` 与 `taxId`。
- taxId 使用数字，不用 label 或 optionKey 替代。
- 未声明测序平台时提交数字 `0`，不要将其误用于 taxId。
- 批量创建检查数组非空、每个子项有效，并按 pipelineType 分批。
- 文件全部上传成功后再启动任务。

这些是前端应执行的交互校验，不代表后端当前已经为所有嵌套字段实现了完整参数校验。

### 响应处理

必须区分 HTTP 状态与响应体中的业务 `status`。HTTP 200 不代表业务成功。

常见业务状态：

| `Result.status` | 含义 |
| --- | --- |
| `0` | 成功 |
| `1` | 业务失败 |
| `2` | 内部错误 |
| `3` | 重复操作或操作冲突 |
| `4` | 参数错误 |

部分接口还存在特定状态：

- 上传返回业务 `100`：该槽位已存在输入，未覆盖。
- 启动返回业务 `200`：任务已启动。
- 启动返回业务 `404`：任务不存在。

不要把业务 `status=200` 当成通用成功码。通用成功判断是业务 `status=0`；其他状态按接口语义提示。

错误响应不完全统一：部分接口的内部错误会返回 HTTP 500 文本，空文件上传可能返回 HTTP 400 文本。因此除了显示 JSON 中的 `failMsg`，还需要兼容非 JSON 错误和网络错误。

## 11. 部署与范围说明

其他环境上线时，需要应用迁移并部署新代码：

- [参考成员映射表迁移](../../scripts/migrations/20261020_add_bio_reference_taxonomy_member.sql)
- [分析目标配置表迁移](../../scripts/migrations/20261020_add_bio_pipeline_target.sql)

本地开发库已应用这两项迁移，但不代表其他环境也已建表。仅创建数据库表不能代替加载新后端代码；已运行的旧版本服务需要部署／重启后才能提供新接口。

首次接入需要部署新代码；之后单纯修改选项或成员配置，不需要为此重启服务。种子迁移不会覆盖已有目标的名称、启用状态和排序，但重新执行可能补回已删除的初始化项；不要把反复执行种子迁移当作日常配置维护方式。

当前字典是本期专项范围，并非所有病毒分类的完整目录：

- 67 个最终选项来自已核对的 78 个来源 taxId。
- 字典记录的 201 条参考是数据库行数，不是 201 个完整基因组。
- `coverageNote` 中记录的参考范围限制仍然有效。
- 选项存在不代表该目标所有下级分类、型别或参考都已覆盖。
- 元数据查询核验不等于序列文件存在性、节段完整性或完整分析能力已经验证。
- 提供 HIV 等目标选项，不等于专门的耐药分析或分型功能已经实现。

## 12. 前端联调检查清单

- [ ] 病毒／新冠的“选择参考基因组”控件已改用后端下发配置，不再使用具体参考记录列表或静态清单。
- [ ] 页面调用 `GET /pipeline/virusTargets`，单层列表展示返回的 `data`，不引入必选父级。
- [ ] 新冠使用 `101 + 2697049`，其他目标使用接口对应值。
- [ ] 空列表与接口错误分别提示，不静默回退到静态字典。
- [ ] 创建只传目标 taxId，不传成员列表或参考记录 ID。
- [ ] sequencingPlatform 未知默认传 `0`。
- [ ] 单个与批量接口分别使用正确的参数字段名。
- [ ] 批量任务按 pipelineType 分组提交。
- [ ] 创建、上传、启动三个动作按顺序执行。
- [ ] 创建成功或启动成功后，不直接展示“分析完成”。
- [ ] 正确处理 HTTP 200 中的业务失败以及非 JSON 错误响应。
- [ ] 选项配置与参考成员配置分别维护和核对。

## 13. 实现依据

- [单个创建 DTO](../../src/main/java/com/xjtlu/bio/requestParameters/CreateAnalysisPipelineRequest.java)
- [批量创建 DTO](../../src/main/java/com/xjtlu/bio/requestParameters/BatchCreateAnalysisPipelineRequest.java)
- [流水线 Controller](../../src/main/java/com/xjtlu/bio/controller/PipelineController.java)
- [输入上传 Controller](../../src/main/java/com/xjtlu/bio/controller/PipelineInputController.java)
- [PipelineService](../../src/main/java/com/xjtlu/bio/service/PipelineService.java)
- [选项查询服务](../../src/main/java/com/xjtlu/bio/service/PipelineTargetService.java)
- [选项查询 Mapper](../../src/main/java/com/xjtlu/bio/mapper/BioPipelineTargetMapper.java)
- [参考成员解析服务](../../src/main/java/com/xjtlu/bio/service/ReferenceTaxonomyResolver.java)
- [参考成员 Mapper](../../src/main/java/com/xjtlu/bio/mapper/BioReferenceTaxonomyMemberMapper.java)
- [选项字典](./virus-taxonomy-options.json)

## 14. 新冠污水混合分析（独立入口）

### 14.1 输入提醒和流程区别

新增 `pipelineType=102`，名称建议为“新冠污水混合分析”。后端仅创建一个 `stageType=92` 的“新冠污水分析”阶段，不经过普通病毒路线的 Assembly、Reference Selection、Consensus 等阶段。

上传页请显示以下提示：

> 请上传新冠扩增子测序的 FASTQ 文件。双端数据请分别上传至 R1、R2；单端数据上传至 R1。请确认测序数据所用的扩增子方案与本系统配置一致。此入口不接收已组装的 FASTA/contigs。

这是用户应确认的输入要求，不是文件检测结论。FASTQ 格式和扩展名不能证明建库方式；后端没有自动识别引物方案。本轮不修改前端应用源码，由前端根据本文添加提示。

- 每条流水线只接收一个样本，R1 必填、R2 可选。
- 上传 `inputType=10`（READ），`inputKey=sample/0/0` 或 `sample/0/1`。
- 文件名接受 `.fastq`、`.fq` 以及对应的 `.gz/.gzip/.zip`；ZIP 沿用单文件解压约定，不用于把 R1/R2 打包到同一个槽位。
- 此入口不提交 `taxId`、`referenceId`、`memberTaxIds` 或前端指定的参考路径，也不使用 `/pipeline/virusTargets` 的普通病毒目标选择器。
- 单 Stage 不另建 ReadInspect：`sequencingPlatform` 是用户声明，未声明仍为 `0`；只有后端方案明确允许的平台才可执行。不要把 `0` 自动改成 Illumina。

### 14.2 创建参数

沿用现有创建、上传、启动接口。下面是单个创建请求示意；`projectId`、名称应替换为实际值：

```json
{
  "projectId": 1,
  "pipelineType": 102,
  "analysisName": "污水样本分析",
  "pipelineStageParameters": {
    "sequencingPlatform": 1001,
    "extraParams": {
      "stageParams": {
        "92": {
          "threads": 2,
          "timeoutSeconds": 7200
        }
      }
    }
  }
}
```

整个 `extraParams.stageParams["92"]` 可以省略，使用默认值。批量创建仍使用已有的 `pipelines[i].pipelineParameters` 字段，每个任务上传自己的一个样本。

| Stage 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `threads` | `2` | 前处理适配器和求解器的线程上限参数，范围 1–256；不是所有外部程序都会使用全部线程 |
| `timeoutSeconds` | `7200` | 执行预算，范围 1–604800 秒；外部进程按剩余预算限时，文件操作之间检查预算。对象存储下载本身仍使用存储客户端的 I/O 超时，不是本参数的硬中断 |
| `analysisProfile` | 后端 `default-profile` | 后端注册的方案标识；省略即可，不需要前端传命令、文件路径或建库配置 |

`analysisProfile` 若提供，应为 1–128 位字母、数字、点、下划线或连字符，首字符为字母或数字。方案不存在或平台不支持时明确失败，不自动切到其他方案。本期没有新增方案下发接口，前端无需自建方案下拉框。

### 14.3 部署前置条件（后端维护，不由用户填写）

这次实现了入口、单 Stage 执行适配、结果解析和保存；**尚未提供真实 FASTQ → 预处理 BAM 的适配脚本，也未安装或运行 Freyja 做真实数据验收**。配置为空时会明确返回“新冠污水扩增子分析方案未配置”，不能把新入口显示成已完成真实分析验证。

2026-10-21 验证：新增 183 项软件测试通过；连同相关既有回归，本轮限定执行 309 项，0 失败、0 错误、0 跳过。数据库和分析工具采用模拟，真实子进程测试仅验证退出码、超时及中断，不是污水样本端到端测试。

`CovidWastewaterConfig` 读取 `analysis-pipeline.covid-wastewater`：

| 配置 | 作用 |
| --- | --- |
| `freyja-command` | 服务端维护的命令参数数组，可指向已有 Conda 环境中的 CLI；不使用请求提供的命令 |
| `configured-freyja-version` | 部署登记的工具版本；产物明确标注这是配置声明，不是运行时检测结果 |
| `default-profile` | 默认方案标识 |
| `profiles.<id>.version` | 前处理方案版本 |
| `profiles.<id>.library-type` | 本期为 `AMPLICON` |
| `profiles.<id>.preparation-command` | 已部署的前处理适配器命令参数数组 |
| `profiles.<id>.supported-platforms` | 明确支持的平台编码列表；若允许 `UNKNOWN=0`，也必须显式列出 |
| `profiles.<id>.reference-fasta` | 参考 FASTA 的绝对路径 |
| `profiles.<id>.barcodes` | Freyja 数据文件的绝对路径 |
| `profiles.<id>.lineage-hierarchy` | 谱系层级文件的绝对路径 |
| `profiles.<id>.lineage-metadata` | 谱系汇总元数据文件的绝对路径 |
| `profiles.<id>.data-version` | 上述数据资源的部署版本 |

不在任务运行时自动联网更新数据。每次运行会复制分析资源并记录 SHA-256，避免只记录一个可能变化的路径。真实部署仍需验证所装工具版本和方案兼容性。

前处理适配器是待接入的服务端程序，本 Stage 按以下软件协议调用：

```text
<preparation-command 的参数数组> --request-json <本次任务清单的绝对路径>
```

清单字段为 `schemaVersion=1`、`profileId`、`profileVersion`、`libraryType`、`r1`、`r2`（可空）、`sequencingPlatform`、`referenceFasta`、`referenceSha256`、`bamOutput`、`bamIndexOutput`、`threads`。文件路径由后端生成；适配器须根据已确认的方案准备 BAM 和索引，写入指定输出路径，失败时非零退出并输出可诊断日志，不得后台化或分离子进程。

内部随后调用 Freyja 的统计及混合分析接口，并校验报告；不是将普通病毒 Consensus 当作混合分析输入。CLI 接口依据：[Freyja variants](https://andersen-lab.github.io/Freyja/src/usage/variants.html)、[Freyja demix](https://andersen-lab.github.io/Freyja/src/usage/demix.html)。前处理方法仍需与实际数据及甲方方案确认。

### 14.4 结果展示

结果仅使用新阶段的 `outputUrl` 文件对象键，不把谱系估计塞入数据库短字符串。字段与展示规则见 [结果展示说明](./pipeline-results-visualization-guide.md) 第 11 节。运行成功仅表示执行与报告格式校验完成，不代表样本质量或混合估计已经通过专业验收。
