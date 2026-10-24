package com.xjtlu.bio.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

import com.github.pagehelper.PageHelper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.entity.BioReferenceSequenceExample;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.requestParameters.ReferenceLibraryQuery;
import com.xjtlu.bio.requestParameters.UploadReferenceRequest;
import com.xjtlu.bio.response.ReferenceLibraryItem;
import com.xjtlu.bio.response.ReferenceLibraryPage;
import com.xjtlu.bio.response.ReferenceMembershipConfirmationResult;
import com.xjtlu.bio.service.ReferenceTargetScopeService.MembershipConfirmationRequiredException;
import com.xjtlu.bio.service.ReferenceTargetScopeService.Scope;
import com.xjtlu.bio.service.reference.ReferenceFastaUpload;
import com.xjtlu.bio.utils.JsonUtil;

@Service
public class ReferenceLibraryService {
    private static final Logger logger = LoggerFactory.getLogger(ReferenceLibraryService.class);
    private static final Pattern ACCESSION = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,63}");
    private final BioReferenceSequenceMapper mapper;
    private final StorageService storage;
    private final TransactionTemplate transactions;
    private final ReferenceTargetScopeService targetScopes;

    @Value("${reference-library.max-upload-bytes:268435456}")
    private long maxUploadBytes = 268435456L;

    public ReferenceLibraryService(BioReferenceSequenceMapper mapper, StorageService storage,
            PlatformTransactionManager transactionManager, ReferenceTargetScopeService targetScopes) {
        this.mapper = mapper;
        this.storage = storage;
        this.targetScopes = targetScopes;
        transactions = new TransactionTemplate(transactionManager);
        transactions.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        // File cleanup must follow this import's actual completion, not an outer transaction.
        transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public Result<ReferenceLibraryPage> query(ReferenceLibraryQuery request) {
        try {
            ReferenceLibraryQuery query = request == null ? new ReferenceLibraryQuery() : request;
            int start = query.getStart() == null ? 0 : query.getStart();
            int limit = query.getPageSize() == null ? 20 : query.getPageSize();
            if (start < 0 || limit < 1 || limit > 100) {
                return invalid("start 必须非负，pageSize 必须为 1–100");
            }
            if (query.getTaxId() != null && query.getTaxId() <= 0) return invalid("taxId 必须为正整数");
            if (query.getOrgType() != null && !validOrgType(query.getOrgType())) {
                return invalid("orgType 仅支持 1（病毒）或 2（细菌）");
            }
            String accession = optionalText(query.getAccession(), 64, "accession");
            Scope scope = resolveScope(query.getTargetTaxId(), query.getPipelineType());
            BioReferenceSequenceExample example = new BioReferenceSequenceExample();
            var criteria = example.createCriteria();
            if (scope != null) criteria.andTaxIdIn(scope.memberTaxIds());
            if (query.getTaxId() != null) criteria.andTaxIdEqualTo(query.getTaxId());
            if (query.getOrgType() != null) criteria.andOrgTypeEqualTo(query.getOrgType());
            if (query.getEnabled() != null) criteria.andEnabledEqualTo(query.getEnabled());
            if (accession != null) criteria.andAccessionEqualTo(accession);
            long total = mapper.countByExample(example);
            example.setOrderByClause("reference_id desc");
            List<ReferenceLibraryItem> items;
            try {
                PageHelper.offsetPage(start, limit, false);
                items = mapper.selectByExample(example).stream().map(ReferenceLibraryItem::from).toList();
            } finally {
                PageHelper.clearPage();
            }
            return success(new ReferenceLibraryPage(total, start, limit, items));
        } catch (IllegalArgumentException e) {
            return invalid(e.getMessage());
        } catch (Exception e) {
            logger.error("Reference library query failed", e);
            return internal("参考库查询失败，请稍后重试");
        }
    }

    public Result<ReferenceLibraryItem> detail(long referenceId) {
        if (referenceId <= 0) return invalid("referenceId 必须为正整数");
        try {
            BioReferenceSequence row = mapper.selectByPrimaryKey(referenceId);
            return row == null ? unavailable("参考记录不存在") : success(ReferenceLibraryItem.from(row));
        } catch (Exception e) {
            logger.error("Reference detail query failed, referenceId={}", referenceId, e);
            return internal("参考记录读取失败，请稍后重试");
        }
    }

    public Result<ReferenceLibraryItem> setEnabled(long referenceId, Boolean enabled) {
        if (referenceId <= 0 || enabled == null) return invalid("referenceId 必须为正整数，enabled 必须明确传 true 或 false");
        try {
            BioReferenceSequence row = mapper.selectByPrimaryKey(referenceId);
            if (row == null) return unavailable("参考记录不存在");
            if (enabled && (row.getPath() == null || row.getPath().isBlank() || !storage.isReadableObject(row.getPath()))) {
                return unavailable("参考文件不存在或不可读取，不能启用；请先核对文件存储");
            }
            if (enabled.equals(row.getEnabled())) return success(ReferenceLibraryItem.from(row));
            BioReferenceSequence patch = new BioReferenceSequence();
            patch.setReferenceId(referenceId);
            patch.setEnabled(enabled);
            if (mapper.updateByPrimaryKeySelective(patch) != 1) return unavailable("参考记录未更新，请刷新后重试");
            row.setEnabled(enabled);
            return success(ReferenceLibraryItem.from(row));
        } catch (Exception e) {
            logger.error("Reference enabled update failed, referenceId={}", referenceId, e);
            return internal("参考启用状态更新失败，请刷新列表确认状态");
        }
    }

    public Result<ReferenceLibraryItem> upload(UploadReferenceRequest request, MultipartFile file) {
        Path temporary = null;
        String objectKey = null;
        boolean stored = false;
        AtomicBoolean databaseAttempted = new AtomicBoolean(false);
        AtomicInteger completion = new AtomicInteger(TransactionSynchronization.STATUS_UNKNOWN);
        BioReferenceSequence row = null;
        try {
            row = importMetadata(request);
            if (file == null || file.isEmpty()) return invalid("请上传非空 FASTA 文件");
            if (maxUploadBytes < 1) throw new IllegalStateException("Reference upload size limit is not configured correctly");
            if (file.getSize() > maxUploadBytes) return invalid("上传文件超过服务端大小限制");

            Scope scope = resolveScope(request.getTargetTaxId(), request.getPipelineType());
            boolean confirmed = Boolean.TRUE.equals(request.getConfirmAddTaxonomyMember());
            if (scope == null && confirmed) {
                return invalid("confirmAddTaxonomyMember=true 时必须指定 targetTaxId 和 pipelineType");
            }
            if (scope != null) {
                if (row.getOrgType() != RefSeqService.VIRUS_TYPE) {
                    return invalid("病毒专项上传的 orgType 必须为 1（病毒）");
                }
                targetScopes.checkMembership(scope, row.getTaxId(), confirmed);
            }

            BioReferenceSequenceExample duplicate = new BioReferenceSequenceExample();
            duplicate.createCriteria().andAccessionEqualTo(row.getAccession());
            if (mapper.countByExample(duplicate) != 0) return duplicate();

            temporary = Files.createTempDirectory("reference-import-");
            ReferenceFastaUpload.Prepared prepared;
            try (InputStream input = file.getInputStream()) {
                prepared = ReferenceFastaUpload.prepare(input, file.getOriginalFilename(), temporary, maxUploadBytes);
            } catch (IOException e) {
                logger.warn("Reference FASTA read/validation failed, accession={}", row.getAccession(), e);
                return invalid("FASTA 文件读取或校验失败，请检查格式、压缩内容及大小限制");
            }
            row.setGenomeLength(prepared.totalBases());
            objectKey = "refseq/custom/" + UUID.randomUUID() + "/reference.fna";
            row.setPath(objectKey);
            var metadata = new LinkedHashMap<String, Object>();
            metadata.put("importedAt", Instant.now().toString());
            metadata.put("originalFilename", displayFilename(file.getOriginalFilename()));
            metadata.put("recordCount", prepared.recordCount());
            metadata.put("sha256", prepared.sha256());
            row.setRawMetadata(JsonUtil.toJson(metadata));
            try (InputStream data = Files.newInputStream(prepared.path())) {
                StorageService.PutResult put = storage.putObject(objectKey, data,
                        StorageService.PutOptions.builder().overwrite(false).contentType("text/plain")
                                .size(Files.size(prepared.path())).build());
                if (put == null || !put.success()) {
                    logger.error("Reference file storage failed, objectKey={}", objectKey, put == null ? null : put.e());
                    return internal("参考文件保存失败，未追加参考记录");
                }
                stored = true;
            }
            BioReferenceSequence record = row;
            transactions.executeWithoutResult(status -> {
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCompletion(int result) { completion.set(result); }
                });
                // Membership writes also belong to this transaction; track uncertain outcomes
                // before attempting them, even if the reference insert has not been reached.
                databaseAttempted.set(true);
                if (scope != null) {
                    targetScopes.ensureMembership(request.getTargetTaxId(), request.getPipelineType(),
                            record.getTaxId(), confirmed);
                }
                if (mapper.insertSelective(record) != 1 || record.getReferenceId() == null) {
                    throw new IllegalStateException("Reference insert did not return exactly one generated ID");
                }
            });
            return success(ReferenceLibraryItem.from(row));
        } catch (Exception e) {
            // The transaction outcome takes precedence over exception type. An
            // uncertain commit must not be reported as a safe retry.
            boolean uncertainCommit = databaseAttempted.get()
                    && completion.get() == TransactionSynchronization.STATUS_UNKNOWN;
            if (!uncertainCommit && e instanceof MembershipConfirmationRequiredException confirmation) {
                return new ReferenceMembershipConfirmationResult(confirmation.getTargetTaxId(),
                        confirmation.getPipelineType(), confirmation.getTaxId());
            }
            if (!uncertainCommit && e instanceof IllegalArgumentException) return invalid(e.getMessage());
            if (!uncertainCommit && e instanceof DuplicateKeyException) return duplicate();
            logger.error("Reference import failed, accession={}, objectKey={}, transactionStatus={}",
                    row == null ? null : row.getAccession(), objectKey, completion.get(), e);
            if (uncertainCommit) {
                return internal("导入提交状态待确认，请按 accession 查询后再重试：" + row.getAccession());
            }
            return internal("参考导入失败，请稍后重试");
        } finally {
            if (stored && (!databaseAttempted.get() || completion.get() == TransactionSynchronization.STATUS_ROLLED_BACK)) {
                cleanupStoredFile(objectKey);
            }
            if (temporary != null) {
                try { FileUtils.deleteDirectory(temporary.toFile()); }
                catch (IOException e) { logger.error("Failed to clean owned reference import directory: {}", temporary, e); }
            }
        }
    }

    private Scope resolveScope(Integer targetTaxId, Integer pipelineType) {
        if (targetTaxId == null && pipelineType == null) return null;
        Scope scope = targetScopes.resolve(targetTaxId, pipelineType);
        if (scope == null || scope.memberTaxIds().isEmpty()) {
            // A malformed resolver result must never turn a scoped request into a global query.
            throw new IllegalStateException("Target scope resolution returned no scope");
        }
        return scope;
    }

    private static BioReferenceSequence importMetadata(UploadReferenceRequest request) {
        if (request == null || request.getTaxId() == null || request.getTaxId() <= 0) {
            throw new IllegalArgumentException("taxId 必须为正整数");
        }
        if (!validOrgType(request.getOrgType())) throw new IllegalArgumentException("orgType 仅支持 1（病毒）或 2（细菌）");
        String accession = optionalText(request.getAccession(), 64, "accession");
        if (accession == null) accession = "CUSTOM_" + UUID.randomUUID().toString().replace("-", "");
        if (!ACCESSION.matcher(accession).matches()) throw new IllegalArgumentException("accession 格式不正确");
        BioReferenceSequence row = new BioReferenceSequence();
        row.setAccession(accession);
        row.setSourceDb("USER_UPLOAD");
        row.setTaxId(request.getTaxId());
        row.setOrgType(request.getOrgType());
        row.setOrganismName(optionalText(request.getOrganismName(), 255, "organismName"));
        row.setSegment(optionalText(request.getSegment(), 64, "segment"));
        row.setCompleteness(optionalText(request.getCompleteness(), 32, "completeness"));
        row.setIsAnnotated(false);
        row.setEnabled(request.getEnabled() == null || request.getEnabled());
        return row;
    }

    private static boolean validOrgType(Integer value) {
        return value != null && (value == RefSeqService.VIRUS_TYPE || value == RefSeqService.BACTERIA_TYPE);
    }

    private static String optionalText(String value, int max, String field) {
        if (value == null || value.isBlank()) return null;
        String text = value.strip();
        if (text.length() > max || text.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException(field + " 过长或包含控制字符");
        }
        return text;
    }

    private static String displayFilename(String filename) {
        if (filename == null) return "";
        String name = filename.replace('\\', '/');
        name = name.substring(name.lastIndexOf('/') + 1).replaceAll("\\p{Cntrl}", "_");
        return name.substring(0, Math.min(255, name.length()));
    }

    private void cleanupStoredFile(String key) {
        try {
            if (!storage.delete(key) && storage.exists(key)) {
                logger.error("Orphan reference upload requires cleanup, objectKey={}", key);
            }
        } catch (Exception e) {
            logger.error("Orphan reference upload cleanup failed, objectKey={}", key, e);
        }
    }

    private static <T> Result<T> success(T value) { return new Result<>(Result.SUCCESS, value, null); }
    private static <T> Result<T> invalid(String message) { return new Result<>(Result.PARAMETER_NOT_VALID, null, message); }
    private static <T> Result<T> unavailable(String message) { return new Result<>(Result.BUSINESS_FAIL, null, message); }
    private static <T> Result<T> internal(String message) { return new Result<>(Result.INTERNAL_FAIL, null, message); }
    private static <T> Result<T> duplicate() { return new Result<>(Result.DUPLICATE_OPERATION, null, "accession 已存在，未覆盖原记录或文件"); }
}
