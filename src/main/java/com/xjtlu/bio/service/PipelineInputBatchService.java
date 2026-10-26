package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_FILE_STATUS_UPLOADED;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_READ;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.web.multipart.MultipartFile;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineInputFileExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioPipelineInputFileMapper;
import com.xjtlu.bio.requestParameters.BatchPipelineInputUploadRequest;
import com.xjtlu.bio.response.BatchPipelineInputUploadResponse;

/** Publishes the initial complete sample-input set atomically; existing inputs are never replaced. */
@Service
public class PipelineInputBatchService {

    private static final Logger logger = LoggerFactory.getLogger(PipelineInputBatchService.class);
    private static final List<Integer> SAMPLE_ROLES = List.of(
            PIPELINE_INPUT_TYPE_SEQUENCE_READ, PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY);
    private static final Set<Integer> SUPPORTED_PIPELINES = Set.of(
            Constants.PipelineType.PIPELINE_VIRUS,
            Constants.PipelineType.PIPELINE_VIRUS_COVID,
            Constants.PipelineType.PIPELINE_REGULAR_BACTERIA);
    private static final Pattern PART_NAME = Pattern.compile("[A-Za-z][A-Za-z0-9_-]{0,63}");
    private static final Set<String> READ_SUFFIXES = Set.of(".fastq", ".fq");
    private static final Set<String> ASSEMBLY_SUFFIXES = Set.of(".fasta", ".fa", ".fna", ".contig", ".contigs");

    private final BioAnalysisPipelineMapper pipelineMapper;
    private final BioPipelineInputFileMapper inputMapper;
    private final StorageService storage;
    private final TransactionTemplate transaction;
    private final PipelineInputOperationCoordinator coordinator;

    public PipelineInputBatchService(BioAnalysisPipelineMapper pipelineMapper,
            BioPipelineInputFileMapper inputMapper, StorageService storage,
            PlatformTransactionManager transactionManager, PipelineInputOperationCoordinator coordinator) {
        this.pipelineMapper = pipelineMapper;
        this.inputMapper = inputMapper;
        this.storage = storage;
        this.coordinator = coordinator;
        this.transaction = new TransactionTemplate(transactionManager);
        // The response and cleanup must follow this batch's actual commit, not a caller's later commit.
        this.transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public Result<BatchPipelineInputUploadResponse> uploadBatch(BatchPipelineInputUploadRequest request,
            MultiValueMap<String, MultipartFile> files) {
        final List<PreparedInput> prepared;
        try {
            prepared = validateManifest(request, files);
        } catch (BatchRejected rejected) {
            return failure(rejected.code, rejected.getMessage());
        }

        long pipelineId = request.getPipelineId();
        var acquisition = coordinator.tryAcquire(pipelineId,
                PipelineInputOperationCoordinator.Operation.BATCH_UPLOAD);
        if (!acquisition.acquired()) {
            return failure(Result.DUPLICATE_OPERATION, "当前流水线正在上传或启动，请稍后重试");
        }

        try (var permit = acquisition.permit()) {
            return uploadWhileLocked(request, prepared);
        }
    }

    private Result<BatchPipelineInputUploadResponse> uploadWhileLocked(BatchPipelineInputUploadRequest request,
            List<PreparedInput> prepared) {
        long pipelineId = request.getPipelineId();
        List<String> ownedObjects = new ArrayList<>();
        List<BioPipelineInputFile> newInputs = new ArrayList<>();
        Completion completion = new Completion();
        try {
            validatePipeline(pipelineMapper.selectByPrimaryKey(pipelineId));
            if (!readSamples(pipelineId).isEmpty()) {
                throw new BatchRejected(Result.BUSINESS_FAIL,
                        "流水线已存在样本输入，批量接口仅支持首次完整上传，不支持追加或替换");
            }
            String batchPrefix = "pipelineInput/p" + pipelineId + "/batches/" + UUID.randomUUID() + "/";
            int role = request.getSequenceLevel() == SEQUENCE_LEVEL_READ
                    ? PIPELINE_INPUT_TYPE_SEQUENCE_READ : PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY;
            for (PreparedInput input : prepared) {
                String objectKey = batchPrefix + UUID.randomUUID() + "/" + input.safeFileName();
                // Never claim/delete an object that predates this batch, even on an unexpected collision.
                if (storage.exists(objectKey)) {
                    throw new IOException("New batch object already exists");
                }
                ownedObjects.add(objectKey);
                try (InputStream stream = input.file().getInputStream()) {
                    StorageService.PutResult result = storage.putObject(objectKey, stream,
                            StorageService.PutOptions.builder().overwrite(false).size(input.file().getSize()).build());
                    if (result == null || !result.success()) {
                        throw new IOException("Unable to store a batch input", result == null ? null : result.e());
                    }
                }
                if (!storage.isReadableObject(objectKey)) {
                    throw new IOException("Stored batch input is not readable");
                }
                newInputs.add(newRecord(pipelineId, role, input, objectKey));
            }

            transaction.executeWithoutResult(status -> {
                completion.entered = true;
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCompletion(int outcome) {
                        completion.outcome = outcome;
                    }
                });
                validatePipeline(pipelineMapper.selectByPrimaryKey(pipelineId));
                if (!readSamples(pipelineId).isEmpty()) {
                    throw new BatchRejected(Result.DUPLICATE_OPERATION,
                            "流水线已出现样本输入，本批次未生效，请刷新输入状态");
                }
                for (BioPipelineInputFile input : newInputs) {
                    if (inputMapper.insertSelective(input) != 1 || input.getInputFileId() == null) {
                        throw new IllegalStateException("Failed to insert batch input record");
                    }
                }
            });
            return success(request, newInputs);
        } catch (Exception failure) {
            if (completion.outcome == TransactionSynchronization.STATUS_COMMITTED) {
                // A post-commit exception must not delete the newly published files or report a rollback.
                logger.warn("Batch input transaction committed despite a completion exception, pipelineId={}",
                        pipelineId, failure);
                return success(request, newInputs);
            }
            if (!completion.entered || completion.outcome == TransactionSynchronization.STATUS_ROLLED_BACK) {
                cleanupOwnedObjects(ownedObjects);
            } else {
                // Connection loss during COMMIT is not proof of rollback. Only a complete, matching
                // committed set can be reported as success; otherwise preserve the objects for reconciliation.
                List<BioPipelineInputFile> committed = findCommittedBatch(pipelineId, newInputs);
                if (committed != null) {
                    return success(request, committed);
                }
                logger.error("Batch input commit outcome is unknown; retaining objects {}, pipelineId={}",
                        ownedObjects, pipelineId, failure);
                return failure(Result.INTERNAL_FAIL, "批量上传提交结果尚未确认，请刷新流水线输入状态后再决定是否重试");
            }
            if (failure instanceof BatchRejected rejected) {
                return failure(rejected.code, rejected.getMessage());
            }
            logger.error("Batch input upload failed, pipelineId={}", pipelineId, failure);
            return failure(Result.INTERNAL_FAIL, "批量上传失败，本批次未生效");
        }
    }

    private List<PreparedInput> validateManifest(BatchPipelineInputUploadRequest request,
            MultiValueMap<String, MultipartFile> files) {
        require(request != null && request.getPipelineId() != null && request.getPipelineId() > 0,
                "pipelineId 必须为有效的流水线 ID");
        require(request.getSequenceLevel() != null
                && (request.getSequenceLevel() == SEQUENCE_LEVEL_READ
                    || request.getSequenceLevel() == SEQUENCE_LEVEL_ASSEMBLY),
                "sequenceLevel 必须显式设置为 0（READ）或 1（ASSEMBLY）");
        require(request.getInputs() != null && !request.getInputs().isEmpty() && request.getInputs().size() <= 2,
                "必须提交完整的单样本输入清单（1 至 2 个文件）");
        require(files != null, "缺少上传文件");
        Set<String> slots = new HashSet<>();
        Set<String> partNames = new HashSet<>();
        List<PreparedInput> prepared = new ArrayList<>();
        boolean reads = request.getSequenceLevel() == SEQUENCE_LEVEL_READ;
        for (BatchPipelineInputUploadRequest.Input input : request.getInputs()) {
            require(input != null, "输入清单不能包含空项");
            String slot = input.getInputKey();
            require("sample/0/0".equals(slot) || (reads && "sample/0/1".equals(slot)),
                    "inputKey 不合法：READ 使用 sample/0/0 和可选的 sample/0/1，ASSEMBLY 仅使用 sample/0/0");
            require(slots.add(slot), "输入槽位不能重复");
            String part = input.getFilePart();
            require(part != null && PART_NAME.matcher(part).matches() && !"manifest".equals(part),
                    "filePart 必须为有效且非 manifest 的文件 Part 名称");
            require(partNames.add(part), "filePart 不能重复");
            List<MultipartFile> matching = files.get(part);
            require(matching != null && matching.size() == 1 && matching.get(0) != null,
                    "每个 filePart 必须对应且仅对应一个文件");
            MultipartFile file = matching.get(0);
            require(!file.isEmpty() && file.getSize() > 0, "上传文件不能为空");
            String original = file.getOriginalFilename();
            String safe = safeFileName(original);
            String sequenceName = Constants.CompressedFormat.removeCompressedSuffix(safe).toLowerCase(Locale.ROOT);
            Set<String> suffixes = reads ? READ_SUFFIXES : ASSEMBLY_SUFFIXES;
            require(suffixes.stream().anyMatch(sequenceName::endsWith), reads
                    ? "READ 仅支持 FASTQ/FQ（可使用 .gz/.gzip/.zip 压缩）"
                    : "ASSEMBLY 仅支持 FASTA/FA/FNA/CONTIG/CONTIGS（可使用 .gz/.gzip/.zip 压缩）");
            prepared.add(new PreparedInput(slot, original, safe, file));
        }
        require(slots.contains("sample/0/0"), "必须提供 sample/0/0，不能仅上传 R2");
        require(files.keySet().equals(partNames), "实际文件 Part 必须与清单完全一致，不能缺失或多余");
        return prepared;
    }

    private static String safeFileName(String original) {
        require(original != null && !original.isBlank() && original.length() <= 512,
                "文件名不能为空或超过长度限制");
        require(original.indexOf('/') < 0 && original.indexOf('\\') < 0
                && original.codePoints().noneMatch(Character::isISOControl), "文件名不能包含路径或控制字符");
        String safe = original.strip().replaceAll("\\s+", "_");
        require(!safe.startsWith(".") && safe.getBytes(StandardCharsets.UTF_8).length <= 255,
                "文件名不合法或超过存储长度限制");
        return safe;
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new BatchRejected(Result.PARAMETER_NOT_VALID, message);
        }
    }

    private static void validatePipeline(BioAnalysisPipeline pipeline) {
        if (pipeline == null) {
            throw new BatchRejected(Result.BUSINESS_FAIL, "流水线不存在");
        }
        if (!Objects.equals(pipeline.getStatus(), PipelineService.PIPELINE_STATUS_PENDING)) {
            throw new BatchRejected(Result.BUSINESS_FAIL, "仅尚未启动的流水线可以上传输入");
        }
        if ((pipeline.getParentPipelineId() != null && pipeline.getParentPipelineId() > 0)
                || pipeline.getPipelineType() == null || !SUPPORTED_PIPELINES.contains(pipeline.getPipelineType())) {
            throw new BatchRejected(Result.BUSINESS_FAIL, "批量上传目前仅支持普通病毒、新冠及细菌的用户流水线，不支持派生任务");
        }
    }

    private static BioPipelineInputFile newRecord(long pipelineId, int role, PreparedInput input, String key) {
        BioPipelineInputFile row = new BioPipelineInputFile();
        row.setPipelineId(pipelineId);
        row.setFileRole(role);
        row.setInputKey(input.inputKey());
        row.setFileName(input.safeFileName());
        row.setOriginalFilename(input.originalFileName());
        row.setFilePath(key);
        row.setStatus(PIPELINE_INPUT_FILE_STATUS_UPLOADED);
        row.setCreateTime(new Date());
        return row;
    }

    private static BioPipelineInputFileExample samplesQuery(long pipelineId) {
        BioPipelineInputFileExample query = new BioPipelineInputFileExample();
        query.createCriteria().andPipelineIdEqualTo(pipelineId).andFileRoleIn(SAMPLE_ROLES);
        return query;
    }

    private List<BioPipelineInputFile> readSamples(long pipelineId) {
        return inputMapper.selectByExample(samplesQuery(pipelineId));
    }

    private List<BioPipelineInputFile> findCommittedBatch(long pipelineId, List<BioPipelineInputFile> expected) {
        if (expected.isEmpty()) {
            return null;
        }
        try {
            List<BioPipelineInputFile> actual = readSamples(pipelineId);
            if (actual.size() != expected.size()) {
                return null;
            }
            List<BioPipelineInputFile> ordered = new ArrayList<>();
            for (BioPipelineInputFile item : expected) {
                List<BioPipelineInputFile> matches = actual.stream().filter(row ->
                        row.getInputFileId() != null && Objects.equals(row.getFilePath(), item.getFilePath())
                        && Objects.equals(row.getInputKey(), item.getInputKey())
                        && Objects.equals(row.getFileRole(), item.getFileRole())
                        && Objects.equals(row.getStatus(), PIPELINE_INPUT_FILE_STATUS_UPLOADED)
                        && Objects.equals(row.getFileName(), item.getFileName())).toList();
                if (matches.size() != 1) {
                    return null;
                }
                ordered.add(matches.get(0));
            }
            return ordered;
        } catch (Exception lookupFailure) {
            logger.warn("Unable to reconcile batch input commit, pipelineId={}", pipelineId, lookupFailure);
            return null;
        }
    }

    private void cleanupOwnedObjects(List<String> keys) {
        keys.forEach(this::deleteObjectQuietly);
    }

    private void deleteObjectQuietly(String key) {
        try {
            if (!storage.delete(key) && storage.exists(key)) {
                logger.warn("Input object cleanup did not remove {}; retained for cleanup", key);
            }
        } catch (Exception cleanupFailure) {
            logger.warn("Unable to clean input object {}; retained for cleanup", key, cleanupFailure);
        }
    }

    private static Result<BatchPipelineInputUploadResponse> success(BatchPipelineInputUploadRequest request,
            List<BioPipelineInputFile> rows) {
        BatchPipelineInputUploadResponse response = new BatchPipelineInputUploadResponse();
        response.setPipelineId(request.getPipelineId());
        response.setSequenceLevel(request.getSequenceLevel());
        response.setInputs(rows.stream().map(row -> {
            BatchPipelineInputUploadResponse.Input item = new BatchPipelineInputUploadResponse.Input();
            item.setInputFileId(row.getInputFileId());
            item.setInputKey(row.getInputKey());
            item.setFileName(row.getFileName());
            return item;
        }).toList());
        return new Result<>(Result.SUCCESS, response, null);
    }

    private static Result<BatchPipelineInputUploadResponse> failure(int code, String message) {
        return new Result<>(code, null, message);
    }

    private record PreparedInput(String inputKey, String originalFileName, String safeFileName, MultipartFile file) { }
    private static final class Completion {
        private boolean entered;
        private int outcome = TransactionSynchronization.STATUS_UNKNOWN;
    }

    private static final class BatchRejected extends RuntimeException {
        private final int code;

        private BatchRejected(int code, String message) {
            super(message);
            this.code = code;
        }
    }
}
