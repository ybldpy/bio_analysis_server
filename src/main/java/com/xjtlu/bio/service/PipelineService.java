package com.xjtlu.bio.service;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonMappingException;

import org.apache.ibatis.executor.BatchResult;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.AnalysisPipelineStagesBuilder;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.StageOrchestrator;
import com.xjtlu.bio.analysisPipeline.StageOrchestrator.MissingUpstreamException;
import com.xjtlu.bio.analysisPipeline.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.StageDoneHandler;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineInputFileExample;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.entity.BioRefseq;
import com.xjtlu.bio.entity.BioRefseqExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioRefseqMapper;
import com.xjtlu.bio.mapper.BioSampleMapper;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest.PipelineStageParameters;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;

import com.xjtlu.bio.analysisPipeline.AnalysisPipelineStagesBuilder.PipelineConfigurations;
import com.xjtlu.bio.analysisPipeline.AnalysisPipelineStagesBuilder.PipelineSampleInput;

import jakarta.annotation.Resource;

@Service
public class PipelineService {

    private static final Logger logger = LoggerFactory.getLogger(PipelineService.class);

    @Resource
    private BioAnalysisPipelineMapper analysisPipelineMapper;

    @Resource
    private BioPipelineStageMapper bioPipelineStageMapper;
    @Resource
    private BioAnalysisStageMapperExtension bioAnalysisStageMapperExtension;
    @Resource
    @Lazy
    private PipelineInputService pipelineInputService;

    @Resource
    private BioSampleMapper bioSampleMapper;

    @Resource
    private BioRefseqMapper bioRefseqMapper;

    @Resource
    private BioAnalysisPipelineMapper bioAnalysisPipelineMapper;

    @Resource
    private StorageService storageService;

    @Resource
    @Lazy
    private PipelineStageTaskDispatcher pipelineStageTaskDispatcher;

    @Resource
    private TransactionTemplate rcTransactionTemplate;

    @Resource
    private BioStageUtil bioStageUtil;

    @Resource
    private SqlSessionTemplate batchSqlSessionTemplate;

    @Resource
    private Map<Integer, StageDoneHandler> stageDoneHandlerMap;

    @Resource
    private StageOrchestrator stageOrchestrator;

    @Value("${analysis-pipeline.covid2RefSeq.accession}")
    private String originalCovid2Accession;

    private Set<Long> pipelineOperationLock = ConcurrentHashMap.newKeySet();

    public static final int PIPELINE_STATUS_PENDING_UPLOADING = 0;
    public static final int PIPELINE_STATUS_RUNNING = 1;
    public static final int PIPELINE_STATUS_COMPELETE = 2;

    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_REGULAR_BACTERIA = 2;
    public static final int PIPELINE_SNP_ANALYSIS = 3;

    // for genetic
    private static final int OK = 0;
    private static final int INTERNAL_FAIL = -1;

    // for genetic update
    private static final int NO_EFFECTIVE_UPDATE = 100;

    // for scedule purpose
    private static final int SCHEDULE_UPSTREAM_NOT_READY = 300;

    private Set<Long> pipelineLock = ConcurrentHashMap.newKeySet();

    @Transactional
    public Result<Long> createPipeline(CreateAnalysisPipelineRequest createAnalysisPipelineRequest) {

        String serializedStageParameters = null;

        try {
            serializedStageParameters = JsonUtil.toJson(createAnalysisPipelineRequest.getPipelineStageParameters());
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            logger.error("[Creating pipeline] serializing pipeline stage parameters expcetion", e);
            return new Result<>(Result.INTERNAL_FAIL, -1l, "创建失败");
        }

        BioAnalysisPipeline bioAnalysisPipeline = new BioAnalysisPipeline();
        bioAnalysisPipeline.setAnalysisPipelineName(createAnalysisPipelineRequest.getAnalysisName());
        bioAnalysisPipeline.setProjectId(createAnalysisPipelineRequest.getProjectId());
        bioAnalysisPipeline.setPipelineType(createAnalysisPipelineRequest.getPipelineType());
        bioAnalysisPipeline.setPipelineParameters(serializedStageParameters);

        try {

            this.bioAnalysisPipelineMapper.insertSelective(bioAnalysisPipeline);
        } catch (DuplicateKeyException duplicateKeyException) {
            return new Result<>(Result.BUSINESS_FAIL, -1l,
                    "分析任务名称重复:" + createAnalysisPipelineRequest.getAnalysisName());
        } catch (DataIntegrityViolationException e) {
            logger.warn("project id = {} not exist", bioAnalysisPipeline.getProjectId());
            return new Result<>(Result.BUSINESS_FAIL, -1l, "所选项目不存在");
        } catch (Exception e) {
            logger.error("exception when creating pipeline", e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<>(Result.INTERNAL_FAIL, -1l, "创建失败");
        }

        long pipelineId = bioAnalysisPipeline.getPipelineId();

        return new Result<Long>(Result.SUCCESS, pipelineId, null);

        // String read1OriginalName =
        // createAnalysisPipelineRequest.getRead1OriginName();
        // String read2OriginalName =
        // createAnalysisPipelineRequest.getRead2OriginName();

        // List<String> fileNames = new ArrayList<>();
        // List<Integer> fileRoles = new ArrayList<>();
        // fileNames.add(read1OriginalName);
        // fileRoles.add(PipelineInputService.PIPELINE_INPUT_FILE_ROLE_R1);
        // if (createAnalysisPipelineRequest.isPair()) {
        // fileNames.add(read2OriginalName);
        // fileRoles.add(PipelineInputService.PIPELINE_INPUT_FILE_ROLE_R2);
        // }

        // Result createRes = this.pipelineInputService.createInputs(fileRoles,
        // fileNames, pipelineId);
        // if (createRes.getStatus() != Result.SUCCESS) {
        // return new Result<>(Result.INTERNAL_FAIL, null, "创建失败");
        // }

        // BioPipelineInputFileExample bioPipelineInputFileExample = new
        // BioPipelineInputFileExample();
        // bioPipelineInputFileExample.createCriteria().andPipelineIdEqualTo(pipelineId);

        // List<BioPipelineInputFile> bioPipelineInputFiles = this.pipelineInputService
        // .queryInputs(bioPipelineInputFileExample);
        // List<InputFile> inputFiles = new ArrayList<>();

        // for (BioPipelineInputFile bioPipelineInputFile : bioPipelineInputFiles) {
        // InputFile inputFile = new InputFile();
        // inputFile.setInputId(bioPipelineInputFile.getInputFileId());
        // inputFile.setInputRole(bioPipelineInputFile.getFileRole());
        // inputFiles.add(inputFile);
        // }

        // CreatePipelineResponse createPipelineResponse = new CreatePipelineResponse();
        // createPipelineResponse.setPipelineId(pipelineId);
        // createPipelineResponse.setInputFiles(inputFiles);

        // return new Result<>(Result.SUCCESS, createPipelineResponse, null);

    }

    private List<BioPipelineStage> buildPipelineStages(long pipelineId, List<BioPipelineInputFile> inputFiles,
            List<BioRefseq> refseqs)
            throws JsonMappingException, JsonProcessingException {
        BioAnalysisPipeline bioAnalysisPipeline = this.analysisPipelineMapper.selectByPrimaryKey(pipelineId);

        logger.error("Pipeline = {} not found", pipelineId);
        if (bioAnalysisPipeline == null) {
            return Collections.emptyList();
        }

        PipelineStageParameters pipelineStageParameters = JsonUtil.toObject(bioAnalysisPipeline.getPipelineParameters(),
                PipelineStageParameters.class);

        Integer taxId = pipelineStageParameters.getTaxId();

        BioRefseqExample bioRefseqExample = new BioRefseqExample();
        bioRefseqExample.createCriteria().andTaxIdEqualTo(taxId);
        List<BioRefseq> candicates = taxId == null ? Collections.emptyList()
                : this.bioRefseqMapper.selectByExample(bioRefseqExample);
        return this.buildPipelineStages(inputFiles, bioAnalysisPipeline, candicates);
    }

    private String checkInputFiles(List<BioPipelineInputFile> bioPipelineInputFiles,
            BioAnalysisPipeline analysisPipeline) {

        if (bioPipelineInputFiles.isEmpty()) {
            return "分析任务没有输入";
        }

        if (analysisPipeline.getPipelineType() != PIPELINE_SNP_ANALYSIS) {

            List<BioPipelineInputFile> readTypeInputs = bioPipelineInputFiles.stream()
                    .filter(in -> in.getFileRole() == PipelineInputService.PIPELINE_INPUT_TYPE_READ).toList();

            if (readTypeInputs.isEmpty()) {
                return "缺少输入文件";
            }

            int r1Index = -1;
            int r2Index = -1;
            for (int i = 0; i < readTypeInputs.size(); i++) {
                String inputKey = readTypeInputs.get(i).getInputKey();
                String[] keys = inputKey.split("/");
                int slot = Integer.parseInt(keys[keys.length - 1]);
                if (slot == 0) {
                    r1Index = i;
                } else {
                    r2Index = i;
                }
            }
            if (r2Index != -1 && r1Index == -1) {
                return "缺少R1";
            }

            if (r2Index == -1 && r1Index == -1) {
                return "缺少输入文件";
            }
        } else {
            return null;
        }
    }

    private List<PipelineSampleInput> buildSampleInputs(List<BioPipelineInputFile> inputs,
            BioAnalysisPipeline pipeline) {

        if (pipeline.getPipelineType() != PIPELINE_SNP_ANALYSIS) {

            BioPipelineInputFile r1 = inputs.stream().filter(in -> {
                String[] keys = in.getInputKey().split("/");

                return Integer.parseInt(keys[keys.length - 1]) == 0;

            }).findFirst().orElse(null);

            BioPipelineInputFile r2 = inputs.stream().filter(in -> {
                String[] keys = in.getInputKey().split("/");

                return Integer.parseInt(keys[keys.length - 1]) == 1;
            }).findFirst().orElse(null);

            PipelineSampleInput sampleInput = new PipelineSampleInput(r1.getFilePath(),
                    r2 == null ? null : r2.getFilePath());
            return List.of(sampleInput);
        } else {
            // TODO: implement later
            return null;

        }

    }

    public Result startPipeline(long pipelineId) {

        boolean getPipelineOperationLockSuccess = this.pipelineOperationLock.add(pipelineId);

        if (!getPipelineOperationLockSuccess) {
            return new Result(Result.DUPLICATE_OPERATION, null, "分析任务正在启动");
        }
        boolean lockSuccess = this.pipelineInputService.lockDownPipelineUploading(pipelineId);
        if (!lockSuccess) {
            this.pipelineOperationLock.remove(pipelineId);
            return new Result(Result.BUSINESS_FAIL, null, "分析任务正在上传，请稍后重试");
        }

        try {

            BioAnalysisPipeline bioAnalysisPipeline = this.analysisPipelineMapper.selectByPrimaryKey(pipelineId);
            if (bioAnalysisPipeline == null) {
                return new Result(Result.BUSINESS_FAIL, null, "分析任务不存在");
            }

            BioPipelineInputFileExample queryCondition = new BioPipelineInputFileExample();
            queryCondition.createCriteria().andPipelineIdEqualTo(pipelineId);
            List<BioPipelineInputFile> bioPipelineInputFiles = this.pipelineInputService.queryInputs(queryCondition);

            String validateFailedMsg = checkInputFiles(bioPipelineInputFiles, bioAnalysisPipeline);
            if (validateFailedMsg != null) {
                return new Result(Result.BUSINESS_FAIL, null, validateFailedMsg);
            }

            PipelineStageParameters pipelineStageParameters = JsonUtil
                    .toObject(bioAnalysisPipeline.getPipelineParameters(), PipelineStageParameters.class);

            Integer taxId = pipelineStageParameters.getTaxId();
            List<BioRefseq> refseqs = null;
            if (taxId != null) {
                BioRefseqExample bioRefseqExample = new BioRefseqExample();
                bioRefseqExample.createCriteria().andTaxIdEqualTo(taxId);
                refseqs = this.bioRefseqMapper.selectByExample(bioRefseqExample);
                if (refseqs.isEmpty()) {
                    return new Result(Result.INTERNAL_FAIL, null, "启动失败: 未找到指定参考基因组");
                }
            }

            List<BioPipelineStage> stages = this.buildPipelineStages(bioAnalysisPipeline, pipelineStageParameters,
                    bioPipelineInputFiles, refseqs);
            if (stages.isEmpty()) {
                return new Result(Result.INTERNAL_FAIL, null, "启动失败");
            }

            List<BioPipelineStage> pipelineStages = rcTransactionTemplate.execute((status) -> {
                BioAnalysisPipeline pipelineUpdatePatch = new BioAnalysisPipeline();
                pipelineUpdatePatch.setStatus(PIPELINE_STATUS_RUNNING);
                BioAnalysisPipelineExample updatePipelineCondition = new BioAnalysisPipelineExample();
                updatePipelineCondition.createCriteria().andPipelineIdEqualTo(pipelineId);
                int updateRes = this.bioAnalysisPipelineMapper.updateByExampleSelective(bioAnalysisPipeline,
                        updatePipelineCondition);
                if (updateRes <= 1) {
                    return null;
                }

                this.batchInsertStages(stages);
                BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
                bioPipelineStageExample.createCriteria().andPipelineIdEqualTo(pipelineId);
                return this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample);
            });

            if (pipelineStages == null) {
                return new Result(Result.INTERNAL_FAIL, null, "启动失败");
            }

            BioPipelineStage entryStage = pipelineStages.stream().filter(stage -> stage.getStageIndex() == 0).findAny()
                    .orElse(null);
            this.scheduleStage(entryStage, pipelineStages);
            return new Result(Result.SUCCESS, null, null);

        } catch (Exception e) {
            logger.error("start pipeline failed, pipelineId={}", pipelineId, e);
            return new Result(Result.INTERNAL_FAIL, null, "启动分析任务失败: 内部错误");
        } finally {
            this.pipelineInputService.unlockPipelineUploading(pipelineId);
            this.pipelineOperationLock.remove(pipelineId);
        }

    }

    private Result startPipeline(long pipelineId, List<BioPipelineInputFile> inputFiles) {

        try {

            boolean lockSuccess = pipelineLock.add(pipelineId);
            if (!lockSuccess) {
                // another is processing
                return new Result(Result.DUPLICATE_OPERATION, null, "流水线正在启动");
            }

            BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
            bioPipelineStageExample.createCriteria().andPipelineIdEqualTo(pipelineId);
            List<BioPipelineStage> stages = this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample);

            if (stages.isEmpty()) {

                List<BioPipelineStage> bulidStages = this.buildPipelineStages(pipelineId, inputFiles);

                if (bulidStages.isEmpty()) {
                    logger.error("[Create stage]: Pipeline = {}, Not stages build", pipelineId);
                }

                boolean insertSuccess = rcTransactionTemplate.execute((status) -> {

                    BioPipelineStageMapper bioPipelineStageMapper = batchSqlSessionTemplate
                            .getMapper(BioPipelineStageMapper.class);

                    for (BioPipelineStage stage : bulidStages) {
                        bioPipelineStageMapper.insertSelective(stage);
                    }

                    try {
                        batchSqlSessionTemplate.flushStatements();
                        return true;
                    } catch (Exception e) {

                        logger.error("[Create stages]: pipeline = {}, insertion fail", pipelineId, e);
                        return false;

                    }
                });

                if (!insertSuccess) {
                    return new Result(Result.INTERNAL_FAIL, null, "启动流水线分析任务失败");
                }
            }

            // get stage id here
            stages = stages.isEmpty() ? this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample) : stages;

            // prerequite: Not null. First stage must exists;
            BioPipelineStage firstStage = stages.stream().filter(s -> s.getStageIndex() == 0).findAny().orElse(null);
            if (!this.pipelineStageTaskDispatcher.isStageIn(firstStage.getStageId())
                    && firstStage.getStatus() != PIPELINE_STAGE_STATUS_QUEUING
                    && firstStage.getStatus() != PIPELINE_STAGE_STATUS_RUNNING) {
                int res = scheduleStage(firstStage, stages);
                if (res == OK) {
                    return new Result(Result.SUCCESS, null, null);
                }

                if (res == INTERNAL_FAIL || res == NO_EFFECTIVE_UPDATE) {
                    return new Result(Result.INTERNAL_FAIL, null, "内部错误");
                }
            }
            return new Result(Result.SUCCESS, null, null);

        } catch (Exception e) {
            logger.error("[Create stages]: pipeline = {}, exception = ", pipelineId, e);
            return new Result(Result.INTERNAL_FAIL, null, "启动分析任务失败");
        } finally {
            pipelineLock.remove(pipelineId);
        }

    }

    // @Async
    // public void handleInputRecevied(long pipelineId, ) {
    // BioPipelineInputFileExample bioPipelineInputFileExample = new
    // BioPipelineInputFileExample();

    // if(receviedInputRole == PipelineInputService.PIPELINE_INPUT_FILE_ROLE_R1 ||
    // receviedInputRole == PipelineInputService.PIPELINE_INPUT_FILE_ROLE_R2){
    // bioPipelineInputFileExample.createCriteria().andPipelineIdEqualTo(pipelineId).andFileRoleIn(List.of(PipelineInputService.PIPELINE_INPUT_FILE_ROLE_R1,
    // PipelineInputService.PIPELINE_INPUT_FILE_ROLE_R2));
    // }

    // List<BioPipelineInputFile> pipelineInputs =
    // this.pipelineInputService.queryInputs(bioPipelineInputFileExample);
    // List<BioPipelineInputFile> notFinishedUpload = pipelineInputs.stream()
    // .filter((in) -> in.getStatus() !=
    // PipelineInputService.PIPELINE_INPUT_FILE_STATUS_UPLOADED).toList();
    // if (notFinishedUpload.size() > 0) {
    // return;
    // }
    // startPipeline(pipelineId, pipelineInputs);
    // }

    private boolean isLegalPipelineType(int pipelineType) {
        return pipelineType == PIPELINE_VIRUS || pipelineType == PIPELINE_VIRUS_COVID
                || pipelineType == PIPELINE_REGULAR_BACTERIA;
    }

    public int startStageExecute(BioPipelineStage pipelineStage) {
        BioPipelineStage updateStage = new BioPipelineStage();
        int currentVersion = pipelineStage.getVersion();
        updateStage.setVersion(currentVersion + 1);
        pipelineStage.setVersion(currentVersion + 1);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        pipelineStage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        return this.updateStageFromVersion(
                new UpdateStageCommand(updateStage, pipelineStage.getStageId(), currentVersion));
    }

    public int updateStageFromVersion(UpdateStageCommand updateStageCommand) {
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria().andStageIdEqualTo(updateStageCommand.getStageId())
                .andVersionEqualTo(updateStageCommand.getCurrentVersion());
        BioPipelineStage updateStage = updateStageCommand.getUpdateStage();
        updateStage.setVersion(updateStageCommand.getCurrentVersion() + 1);
        try {
            return this.bioPipelineStageMapper.updateByExampleSelective(updateStage, bioPipelineStageExample);
        } catch (Exception e) {
            logger.error("update stage id {} to {} from version {} exception: ", updateStageCommand.getStageId(),
                    updateStage, updateStageCommand.getCurrentVersion(), e);
            return INTERNAL_FAIL;
        }
    }

    private void batchInsertStages(List<BioPipelineStage> insertStages) {

        BioPipelineStageMapper stageMapper = batchSqlSessionTemplate.getMapper(BioPipelineStageMapper.class);

        for (BioPipelineStage bioPipelineStage : insertStages) {
            stageMapper.insertSelective(bioPipelineStage);
        }

        List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();

    }

    // @Transactional(rollbackFor = Exception.class)
    // public Result<Long> createPipeline(BioSample bioSample,
    // PipelineStageParameters pipelineStageParams) {

    // boolean noRefseq = pipelineStageParams.getTaxId() == null;

    // List<BioRefseq> candiates = null;

    // if (!noRefseq) {
    // BioRefseqExample bioRefseqExample = new BioRefseqExample();
    // bioRefseqExample.createCriteria().andTaxIdEqualTo(pipelineStageParams.getTaxId());
    // candiates = bioRefseqMapper.selectByExampleWithBLOBs(bioRefseqExample);
    // if (candiates.isEmpty()) {
    // return new Result<Long>(Result.BUSINESS_FAIL, -1l, "未能找到参考基因组");
    // }
    // }

    // BioAnalysisPipeline bioAnalysisPipeline = new BioAnalysisPipeline();
    // bioAnalysisPipeline.setPipelineType();
    // bioAnalysisPipeline.setSampleId(bioSample.getSid());
    // int insertRes = this.bioAnalysisPipelineMapper.insert(bioAnalysisPipeline);
    // if (insertRes < 1) {
    // TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
    // return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线失败");
    // }

    // List<BioPipelineStage> stages = this.buildPipelineStages(bioSample,
    // bioAnalysisPipeline, candiates);
    // if (stages == null || stages.isEmpty()) {
    // TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
    // return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线失败");
    // }

    // for (BioPipelineStage stage : stages) {
    // stage.setVersion(0);
    // }

    // insertRes = this.bioAnalysisStageMapperExtension.batchInsert(stages);
    // if (insertRes != stages.size()) {
    // TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
    // return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线失败");
    // }

    // return new Result<Long>(Result.SUCCESS, bioAnalysisPipeline.getPipelineId(),
    // null);
    // }

    public Result<List<BioAnalysisPipeline>> queryPipelines(BioAnalysisPipelineExample condition) {

        try {
            List<BioAnalysisPipeline> pipelines = this.analysisPipelineMapper.selectByExample(condition);
            return new Result<List<BioAnalysisPipeline>>(Result.SUCCESS, pipelines, null);
        } catch (Exception e) {
            logger.warn("[query pipeline]exception", e);
            return new Result<List<BioAnalysisPipeline>>(Result.INTERNAL_FAIL, null, "内部错误");
        }

    }

    private BioRefseq getOriginalCovid2Refseqs(List<BioRefseq> covid2Refseqs) {
        return covid2Refseqs.stream().filter(refseq -> {
            return refseq.getAccessions().contains(originalCovid2Accession);
        }).findFirst().orElse(null);
    }

    private BioRefseq getBestCandicateRefSeqs(List<BioRefseq> candiatesRefseqs)
            throws JsonMappingException, JsonProcessingException {

        boolean hasSegment = false;

        for (BioRefseq bioRefseq : candiatesRefseqs) {
            if (bioRefseq.getIsSegment()) {
                hasSegment = true;
                break;
            }
        }
        List<BioRefseq> candidates = candiatesRefseqs.stream().filter((refseq) -> refseq.getIsSegment()).toList();
        List<BioRefseq> originalCandicatesPointer = candiatesRefseqs;
        if (candidates.isEmpty()) {
            candidates = originalCandicatesPointer;
        }

        originalCandicatesPointer = candidates;

        candidates = candidates.stream().filter((refseq) -> refseq.getSourceDb().toLowerCase().equals("refseq"))
                .toList();
        if (candidates.isEmpty()) {
            candidates = originalCandicatesPointer;
        }

        originalCandicatesPointer = candidates;

        HashMap<Long, Integer> candiatesNCPrefixNumMap = new HashMap<>();
        HashMap<Long, Integer> candiatesAccessionsListLengthMap = new HashMap<>();
        HashMap<Long, Integer> candiateAccessionLengthMap = new HashMap<>();

        for (BioRefseq bioRefseq : candidates) {
            List<String> accessions = JsonUtil.toObject(bioRefseq.getAccessions(), List.class);
            candiatesAccessionsListLengthMap.put(bioRefseq.getRefId(), accessions.size());
            int num = 0;
            for (String accession : accessions) {
                if (accession.trim().startsWith("NC_")) {
                    num += 1;
                }
            }
            candiatesNCPrefixNumMap.put(bioRefseq.getRefId(), num);

            if (!hasSegment) {
                Map<String, Object> meta = JsonUtil.toMap(bioRefseq.getMeta());
                int len = (Integer) meta.getOrDefault("length", 1000);
                candiateAccessionLengthMap.put(bioRefseq.getRefId(), len);
            }
        }

        int accessionSourceWeight = 10;

        candidates = new ArrayList<>(candidates);
        if (hasSegment) {

            candidates.sort((refseq1, refseq2) -> {
                int refseq1Score = candiatesNCPrefixNumMap.get(refseq1.getRefId()) * accessionSourceWeight
                        + candiatesAccessionsListLengthMap.get(refseq1.getRefId());
                int refseq2Score = candiatesNCPrefixNumMap.get(refseq2.getRefId()) * accessionSourceWeight
                        + candiatesAccessionsListLengthMap.get(refseq2.getRefId());

                return Integer.compare(refseq2Score, refseq1Score);
            });

            return candidates.get(0);
        }

        candidates.sort((refseq1, refseq2) -> {
            int refseq1Score = candiatesNCPrefixNumMap.get(refseq1.getRefId()) * accessionSourceWeight
                    + candiateAccessionLengthMap.get(refseq1.getRefId());
            int refseq2Score = candiatesNCPrefixNumMap.get(refseq2.getRefId()) * accessionSourceWeight
                    + candiateAccessionLengthMap.get(refseq2.getRefId());
            return Integer.compare(refseq2Score, refseq1Score);
        });
        return candidates.get(0);
    }

    private List<BioPipelineStage> buildPipelineStages(BioAnalysisPipeline bioAnalysisPipeline,
            PipelineStageParameters pipelineStageParameters,
            List<BioPipelineInputFile> inputFiles, List<BioRefseq> refseqs)
            throws JsonMappingException, JsonProcessingException {

        List<BioPipelineStage> stages = null;

        List<PipelineSampleInput> sampleInputs = this.buildSampleInputs(inputFiles, bioAnalysisPipeline);

        BioRefseq bestRefseq = refseqs == null || refseqs.isEmpty() ? null : this.getBestCandicateRefSeqs(refseqs);
        if (bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS
                || bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS_COVID) {

            PipelineConfigurations pipelineConfigurations = new PipelineConfigurations();
            if (refseqs == null) {

            }

        }

        // try {
        // if (bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS
        // || bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS_COVID) {

        // // prefer NC prefix
        // PipelineConfigurations pipelineConfigurations = new PipelineConfigurations();
        // if (bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS_COVID) {
        // BioRefseq bioRefseq = getOriginalCovid2Refseqs(candicateRefSeqs);
        // if (bioRefseq == null) {
        // return null;
        // }
        // pipelineConfigurations.setRefId(bioRefseq.getRefId());
        // List<String> accession = JsonUtil.toObject(bioRefseq.getAccessions(),
        // List.class);
        // pipelineConfigurations.setRefseqAccessions(accession);
        // } else if (candicateRefSeqs != null && !candicateRefSeqs.isEmpty()) {
        // BioRefseq bioRefseq = getBestCandicateRefSeqs(candicateRefSeqs);
        // pipelineConfigurations.setRefId(bioRefseq.getRefId());
        // List<String> accessions = JsonUtil.toObject(bioRefseq.getAccessions(),
        // List.class);
        // pipelineConfigurations.setRefseqAccessions(accessions);
        // }

        // boolean isCovid2Pipeline = bioAnalysisPipeline.getPipelineType() ==
        // PIPELINE_VIRUS_COVID;

        // pipelineConfigurations.setRequireCoverageDepth(isCovid2Pipeline);
        // pipelineConfigurations.setRequireSNPAnnotation(isCovid2Pipeline);

        // stages = AnalysisPipelineStagesBuilder.buildVirusStages(
        // bioAnalysisPipeline.getPipelineId(),
        // new PipelineInput(read1.getFilePath(), read2 == null ? null :
        // read2.getFilePath()),
        // pipelineConfigurations);

        // } else if (bioAnalysisPipeline.getPipelineType() ==
        // PIPELINE_REGULAR_BACTERIA) {

        // PipelineInput pipelineInput = new PipelineInput(read1.getFilePath(),
        // read2 == null ? null : read2.getFilePath());
        // PipelineConfigurations pipelineConfigurations = new PipelineConfigurations();

        // stages =
        // AnalysisPipelineStagesBuilder.buildRegularBacteriaPipeline(bioAnalysisPipeline.getPipelineId(),
        // pipelineInput, pipelineConfigurations);

        // }
        // } catch (JsonProcessingException e) {
        // logger.error("pipeline = {}, creation exception",
        // bioAnalysisPipeline.getPipelineId(), e);
        // }

        // return stages == null || stages.isEmpty() ? Collections.emptyList() : stages;
    }

    // TODO: this is an quick method for just test. Never Use it in production env
    // or treat it as normal service method
    public Result<Boolean> restartStage(long stageId) {

        List<BioPipelineStage> allStages = this.bioAnalysisStageMapperExtension
                .selectAllPipelineStagesByStageId(stageId);
        if (allStages == null || allStages.isEmpty()) {
            return new Result<>(Result.BUSINESS_FAIL, false, "未能启动");
        }

        BioPipelineStage startStage = null;
        for (BioPipelineStage stage : allStages) {
            if (stage.getStageId() == stageId) {
                startStage = stage;
                break;
            }
        }

        if (this.pipelineStageTaskDispatcher.isStageIn(stageId)) {
            return new Result<Boolean>(Result.SUCCESS, true, null);
        }

        if (startStage.getStatus() == PIPELINE_STAGE_STATUS_QUEUING) {
            this.addStageTask(startStage);
            return new Result<Boolean>(Result.SUCCESS, true, null);
        }

        int curStatus = startStage.getStatus();
        if (curStatus != PIPELINE_STAGE_STATUS_PENDING && curStatus != PIPELINE_STAGE_STATUS_FAIL) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "不能启动状态为非等待的分析阶段");
        }

        int res = scheduleStage(startStage, allStages);

        if (res == INTERNAL_FAIL || res == NO_EFFECTIVE_UPDATE) {
            return new Result<Boolean>(Result.INTERNAL_FAIL, false, "内部错误");
        }

        if (res == SCHEDULE_UPSTREAM_NOT_READY) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "上游阶段未完成, 无法启动此阶段");
        }

        return new Result<Boolean>(Result.SUCCESS, true, null);

    }

    // @Transactional(rollbackFor = Exception.class)
    // public Result<Boolean> pipelineStart(long sampleId) {

    // List<BioPipelineStage> stages =
    // this.bioAnalysisStageMapperExtension.selectStagesBySampleId(sampleId);
    // if (stages == null || stages.isEmpty()) {
    // return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到流水线");
    // }
    // BioPipelineStage firstStage = null;
    // for (BioPipelineStage stage : stages) {
    // if (stage.getStageIndex() == 0) {
    // firstStage = stage;
    // break;
    // }
    // }

    // if (firstStage == null) {
    // return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未能找到初始任务");
    // }
    // if (firstStage.getStatus() != PIPELINE_STAGE_STATUS_PENDING) {
    // return new Result<Boolean>(Result.SUCCESS, true, null);
    // }

    // int res = scheduleStage(firstStage, stages);

    // if (res == OK) {
    // return new Result<Boolean>(Result.SUCCESS, true, null);
    // }

    // return new Result<Boolean>(Result.INTERNAL_FAIL, false, "内部错误");

    // }

    public void pipelineStageDone(long stageId, boolean success) {

        if (!success) {
            return;
        }

        this.scheduleDownstreamStages(stageId);

    }

    private int scheduleStage(BioPipelineStage stage, List<BioPipelineStage> pipelineAllStages) {

        OrchestratePlan plan = null;

        try {
            plan = stageOrchestrator.makePlan(pipelineAllStages, stage.getStageId());
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            logger.error(
                    "Failed to build plan for stage. StageId={}, reason={}",
                    stage.getStageId(),
                    e.getMessage(),
                    e);

            return INTERNAL_FAIL;
        } catch (MissingUpstreamException e) {
            logger.warn(
                    "Downstream orchestration skipped. Upstream not ready. StageId={}, desc={}",
                    stage.getStageId(),
                    e.getDesc());
            return SCHEDULE_UPSTREAM_NOT_READY;
        }

        int res = this.batchUpdateStages(plan.getUpdateStageCommands());
        if (res == OK) {
            for (BioPipelineStage runStage : plan.getRunStages()) {
                this.pipelineStageTaskDispatcher.addTask(runStage);
            }
            return OK;
        }

        return NO_EFFECTIVE_UPDATE;

    }

    private int scheduleDownstreamStages(long finishedStageId) {

        List<BioPipelineStage> allStages = this.bioAnalysisStageMapperExtension
                .selectAllPipelineStagesByStageId(finishedStageId);

        OrchestratePlan plan = null;
        try {

            plan = stageOrchestrator.makeDownstreamPlan(finishedStageId, allStages);

        } catch (JsonProcessingException
                | InvocationTargetException
                | IllegalAccessException
                | NoSuchMethodException e) {

            logger.error(
                    "Failed to build downstream plan. finishedStageId={}, reason={}",
                    finishedStageId,
                    e.getMessage(),
                    e);

            return INTERNAL_FAIL;

        } catch (MissingUpstreamException e) {

            logger.warn(
                    "Downstream orchestration skipped. Upstream not ready. finishedStageId={}, desc={}",
                    finishedStageId,
                    e.getDesc());
            return SCHEDULE_UPSTREAM_NOT_READY;
        }

        if (plan.isNoNextStage()) {
            return 0;
        }

        List<UpdateStageCommand> updateStageCommands = plan.getUpdateStageCommands();
        List<BioPipelineStage> pushToWaittingStages = plan.getRunStages();

        int res = this.batchUpdateStages(updateStageCommands);

        if (res == OK) {
            for (BioPipelineStage stage : pushToWaittingStages) {
                this.pipelineStageTaskDispatcher.addTask(stage);
            }
            return OK;
        }

        return NO_EFFECTIVE_UPDATE;

    }

    public int markStageFinish(BioPipelineStage bioPipelineStage, String outputUrl) {
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setOutputUrl(outputUrl);
        updateStage.setEndTime(new Date());
        updateStage.setVersion(bioPipelineStage.getVersion() + 1);
        return this.updateStageFromVersion(
                new UpdateStageCommand(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion()));
    }

    public List<BioPipelineStage> getStagesFromExample(BioPipelineStageExample bioPipelineStageExample) {

        List<BioPipelineStage> stages = null;
        try {
            stages = bioPipelineStageMapper.selectByExampleWithBLOBs(bioPipelineStageExample);
        } catch (Exception e) {
            logger.error("", e);
        }
        return stages;
    }

    // 如果更新成功的数量!=list.size()回滚
    // 0: fail. 1: success
    // remember to remain current version in stage here
    public int batchUpdateStages(List<UpdateStageCommand> updateStageCommands) {

        int res = 0;

        try {
            res = rcTransactionTemplate.execute((status) -> {
                BioPipelineStageMapper batchUpdateMapper = batchSqlSessionTemplate
                        .getMapper(BioPipelineStageMapper.class);
                for (UpdateStageCommand updateStageCommand : updateStageCommands) {
                    BioPipelineStageExample conditionExample = new BioPipelineStageExample();
                    BioPipelineStage updateStage = updateStageCommand.getUpdateStage();
                    int currentVersion = updateStageCommand.getCurrentVersion();
                    updateStage.setVersion(currentVersion + 1);
                    long stageId = updateStageCommand.getStageId();

                    conditionExample.createCriteria().andStageIdEqualTo(stageId).andVersionEqualTo(currentVersion);
                    batchUpdateMapper.updateByExampleSelective(updateStage, conditionExample);
                }
                List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();
                int count = 0;
                for (BatchResult result : batchResults) {
                    for (int i : result.getUpdateCounts()) {
                        count += i;
                    }
                }

                if (count != updateStageCommands.size()) {
                    status.setRollbackOnly();
                    return NO_EFFECTIVE_UPDATE;
                }
                return OK;
            });
        } catch (Exception e) {
            return INTERNAL_FAIL;
        }

        return res;

    }

    public boolean addStageTask(BioPipelineStage stage) {
        return this.pipelineStageTaskDispatcher.addTask(stage);
    }

}
