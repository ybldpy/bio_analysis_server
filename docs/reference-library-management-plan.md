# 参考序列库追加与启停实施计划

日期：2026-10-21。范围：`bio_reference_sequence`；不更新旧 `bio_refseq`，不重建 Kraken2/FastANI 等独立分析库。

## 已确认的约定

- 用户自行完成 MyBatis Generator，后续不手工修改生成的 Entity、Example、Mapper 或 XML。
- 一份 FASTA 对应一条参考库记录；多记录 FASTA 保留在同一文件，不自动拆成多个候选。不同节段分别上传，`taxId`、`orgType`、`segment` 等元信息由用户明确提供。
- 只追加和启停，不覆盖已有参考、不提供物理删除接口。
- `enabled` 只表示能否用于新的候选加载，不是上传进度或分析质量结论。新导入默认完成后启用，也允许导入为禁用状态。
- 现有任务使用已保存的参考快照，禁用不能删除或修改原文件。`ReferenceSequence` 快照及 Executor 不添加实时数据库状态依赖。
- 管理接口查询全部记录（包括禁用），分析查询强制 `enabled=true`。
- 新上传来源固定标记为 `USER_UPLOAD`；可提供唯一 accession，没有 accession 时分配明确的本地标识，不冒充 NCBI accession。

## 执行清单

- [x] 真实表新增 `enabled BOOLEAN NOT NULL DEFAULT TRUE` 及 `(tax_id, enabled)` 索引，保留现有 accession 唯一约束。
- [x] 用户重新生成；核对 `enabled` Java/SQL 映射、自增主键回填和 BLOB 列。
- [x] 提供新管理 DTO、Service、Controller：分页查询、按 ID 详情、FASTA 上传追加、按 ID 更新 enabled。旧查询接口保留原语义。
- [x] 实现有大小上限的 FASTA 文件准备与格式校验。支持明文、gzip、ZIP；ZIP 沿用一个文件槽位的约定，取首个非目录文件，不新增多文件 ZIP 拒绝逻辑，也不合并后续文件。检测空序列、缺 header、重复记录名、非法序列字符，计算总长度、记录数和 SHA-256。不推断分类或节段，不做生物学准确性判断。
- [x] 文件存储采用服务端 UUID 对象键和不覆盖写入；复用 StorageService，补齐 LocalStorageService 尚未实现的对应写入/删除能力，并覆盖路径边界测试。
- [x] 文件保存与数据库追加协调：重复 accession 明确失败；数据库明确未保存时清理本次文件，清理失败记录诊断；提交结果不明确时保留文件，避免删除可能已经被记录引用的文件。
- [x] 启用前检查记录引用文件存在且可读取，不把目录或悬空符号链接当成参考文件；禁用仅更新 enabled，不触碰文件或历史任务。
- [x] 在 PipelineService 的 taxId exact/IN 候选查询同一条件组增加 enabled；空候选明确提示，不修改成员映射或分类工具库。
- [x] 补充管理接口、服务、文件校验、存储、MyBatis 映射和候选过滤测试，运行相关既有回归。测试使用临时文件与模拟数据，不向实际参考库追加测试记录，不运行分析工具。
- [x] 写前端接口说明，记录测试结果、限制和实施状态。

## 设计边界

- 本期接口使用现有项目 Result 包装，前端继续区分业务 status 与 HTTP 状态。
- 新管理入口使用 `/referenceGenome/library`，旧 `/referenceGenome/queryReferenceGenomes` 不改表、不改响应结构。
- 上传采用 multipart：`file` + 表单元数据。文件路径、原始 JSON、注释路径等内部字段不可由请求直接指定。
- 查询使用新响应 DTO，不直接暴露 rawMetadata 或服务器路径；需要定位文件时返回存储对象键。
- 分页有上限，排序由后端固定；文件大小/解压后大小有服务端上限。HTTP multipart 上限仍由现有部署配置控制。
- 继承现有鉴权策略。本项目当前 SecurityConfig 为 permitAll，本次不擅自引入管理员角色体系；不应在没有外层访问控制的环境公开管理接口。
- 表级 enabled 不等于目标字典 enabled；不会自动新增或修改前端目标字典、成员映射。

## 验收

1. 上传成功后可管理查询；仅启用项能进入新的候选集合。
2. 相同 accession 并发导入由现有唯一约束兜底，不覆盖旧文件。
3. 坏文件、存储失败、数据库拒绝都不会产生可用的错误记录。
4. 禁用/重新启用可以重复调用；已有候选快照和旧产物保持不变。
5. 既有普通病毒、细菌和污水分支的相关软件回归通过。

## 实施记录

2026-10-22 完成以上代码和文档。用户生成的 Entity、Example、Mapper 与 XML 未被手工修改。

主要入口：

- [ReferenceLibraryController](../src/main/java/com/xjtlu/bio/controller/ReferenceLibraryController.java)：`query`、详情、`upload`、`enabled` 四个管理接口。
- [ReferenceLibraryService](../src/main/java/com/xjtlu/bio/service/ReferenceLibraryService.java)：元数据校验、分页、文件与事务协调、启停。
- [ReferenceFastaUpload](../src/main/java/com/xjtlu/bio/service/reference/ReferenceFastaUpload.java)：保留解压后的原字节，格式校验及摘要；默认上传/解压上限均为 256 MiB，可用 `reference-library.max-upload-bytes` 调整。
- [LocalStorageService](../src/main/java/com/xjtlu/bio/service/LocalStorageService.java)：三参写入支持原子不覆盖、失败清理；新增可读性检查和仅指定文件删除。旧两参写入方法体与 `exists` 语义不变。
- [PipelineService](../src/main/java/com/xjtlu/bio/service/PipelineService.java)：加载候选时附加 `enabled=true`，不让历史快照依赖实时库状态。
- [前端参考库管理接口说明](frontend/reference-library-management-guide.md)：请求示例、返回字段、错误处理、启停语义及部署边界。

### 验证结果

最终定向 Maven 回归：**18 个测试类，369 项，0 失败、0 错误、0 跳过**。

| 范围 | 用例数 |
| --- | ---: |
| FASTA / gzip / ZIP 准备与校验 | 49 |
| 用户生成的 MyBatis enabled / 主键 / BLOB 映射 | 14 |
| 管理响应投影与路径隐藏 | 20 |
| 本地存储不覆盖、删除、可读性、并发与路径边界 | 51 |
| 管理 Service、导入事务及故障处理 | 59 |
| Controller MockMvc HTTP 契约 | 12 |
| 真实本地文件 + Service 组合 | 5 |
| Pipeline 候选查询、启停影响及历史快照 | 21 |
| 既有成员解析、目标接口、平台参数、专项入口等回归 | 138 |
| 合计 | 369 |

组合测试对明文、gzip、ZIP 实际落盘，验证字节一致、禁用保留文件、重新启用、缺失文件拒绝启用、回滚只清理新对象及未知提交保留文件。事务测试使用 Spring 的真实事务同步回调机制与假的资源管理器，覆盖回滚失败等未知完成状态，不把未知提交误报为普通重复操作。

复跑命令（Mockito agent 路径按本机 Maven 缓存）：

```bash
mvn -o -q \
  -Dtest=ReferenceFastaUploadTest,BioReferenceSequenceEnabledMappingTest,ReferenceLibraryItemTest,LocalStorageServiceReferenceImportTest,ReferenceLibraryServiceTest,ReferenceLibraryControllerTest,ReferenceLibraryStorageIntegrationTest,PipelineServiceReferenceTaxonomyTest,ReferenceTaxonomyResolverTest,BioReferenceTaxonomyMemberMapperMappingTest,PipelineServiceSequencingPlatformTest,PipelineServiceCovidWastewaterTest,PipelineInputServiceCovidWastewaterTest,CovidWastewaterStagesBuilderTest,CovidWastewaterStageOrchestratorTest,PipelineTargetServiceTest,PipelineControllerVirusTargetsTest,ReferenceSelectionStageDoneHandlerTest \
  -DargLine=-javaagent:/home/jcy/.m2/repository/org/mockito/mockito-core/5.17.0/mockito-core-5.17.0.jar \
  -Dmaven.test.redirectTestOutputToFile=true test
```

### 本轮未做及部署注意

- 这是软件单元/组件集成回归；未向真实参考库插入测试记录，未启动完整服务做网络 HTTP/MySQL 端到端上传，未运行生信分析工具。
- 启用前可读性检查不是重新校验全文件；用户上传的 taxId、segment、分类及完整性仍由业务人员确认。
- 本地存储要求服务控制的可信目录及支持硬链接、安全目录操作的文件系统；不支持时写入/清理失败并记录日志，不降级为覆盖已有文件。
- 崩溃、未知提交或文件清理失败可能留下待核对对象。本期没有后台孤立文件清理任务；不可直接删除未知提交对应的文件。
- 当前鉴权仍是已有 `permitAll`，上线前必须确保内网/网关等访问控制；本期没有添加管理员角色模型。
