package com.xjtlu.bio.service;

import java.lang.reflect.InvocationTargetException;
import java.sql.Statement;
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

    private static final String INPUT_KEY_DEFAULT_READ1_MATCH = "sample_0/0";
    private static final String INPUT_KEY_DEFAULT_READ2_MATCH = "sample_0/1";

    // public static final int PIPELINE_STATUS_PENDING_UPLOADING = 0;
    public static final int PIPELINE_STATUS_RUNNING = 1;
    public static final int PIPELINE_STATUS_COMPELETE = 2;
    public static final int PIPELINE_STATUS_PENDING = 3;

    public static final int PIPELINE_VIRUS = 100;
    public static final int PIPELINE_VIRUS_COVID = 101;
    public static final int PIPELINE_REGULAR_BACTERIA = 200;

    public static final int PIPELINE_SNP_ANALYSIS = 300;
    public static final int PIPELINE_SNP_SUB_ANALYSIS = 301;
    public static final int PIPELINE_SNP_ANALYSIS_MERGE = 302;

    public static final int PIPELINE_METAGENOME_AMPLicon16s = 400;
    public static final int PIPELINE_METAGENOME_SHOTGUN = 401;

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

            return null;
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

    private List<PipelineSampleInput> paritionSubPipelineSampleInputFiles(
            List<BioPipelineInputFile> pipelineInputFiles) {

        List<BioPipelineInputFile> sampleInputs = pipelineInputFiles.stream()
                .filter(f -> f.getFileRole() == PipelineInputService.PIPELINE_INPUT_TYPE_READ).toList();

        HashMap<String, PipelineSampleInput> subPipelineSampleInputsMap = new HashMap<>();

        for (BioPipelineInputFile sample : sampleInputs) {
            String key = sample.getInputKey();

            int lastSplit = key.lastIndexOf("/");
            String groupKey = key.substring(0, lastSplit);

            if (!subPipelineSampleInputsMap.containsKey(groupKey)) {
                PipelineSampleInput pipelineSampleInput = new PipelineSampleInput();
                int sampleReadTypeCode = sampleReadTypeCode(sample.getFileName());
                pipelineSampleInput.setReadType(sampleReadTypeCode);
                subPipelineSampleInputsMap.put(groupKey, pipelineSampleInput);
            }
            int slot = Integer.parseInt(key.substring(lastSplit + 1));
            if (slot == 0) {
                subPipelineSampleInputsMap.get(groupKey).setR1(sample.getFilePath());
            } else {
                subPipelineSampleInputsMap.get(groupKey).setR2(sample.getFilePath());
            }
        }

        return new ArrayList<>(subPipelineSampleInputsMap.values());
    }


    private Result startRegularPipeline(BioAnalysisPipeline bioAnalysisPipeline, List<BioPipelineInputFile> inputs){

        


    }

    private Result startSNPAnalysisPipeline(BioAnalysisPipeline bioAnalysisPipeline, List<BioPipelineInputFile> inputs)
            throws JsonProcessingException {

        List<PipelineSampleInput> pipelineSampleInputs = paritionSubPipelineSampleInputFiles(inputs);
        BioPipelineInputFile refseqFile = inputs.stream().filter(in -> {
            return in.getFileRole() == PipelineInputService.PIPELINE_INPUT_TYPE_REFSEQ;
        }).findAny().orElse(null);

        PipelineConfigurations pipelineConfigurations = new PipelineConfigurations();
        pipelineConfigurations.setCustomReferenceSequenceObjectName(refseqFile.getFilePath());

        List<List<BioPipelineStage>> subPipelineStagesList = new ArrayList<>();

        for (int i = 0; i < pipelineSampleInputs.size(); i++) {
            List<BioPipelineStage> subPipelineStages = AnalysisPipelineStagesBuilder
                    .buildSNPAnalysisStages(pipelineSampleInputs.get(i), pipelineConfigurations);
            if (subPipelineStages == null || subPipelineStages.isEmpty()) {
                return new Result(Result.INTERNAL_FAIL, null, "创建分析任务失败");
            }
            subPipelineStagesList.add(subPipelineStages);
        }

        BioPipelineStage mergeResultPipelineStage = AnalysisPipelineStagesBuilder.buildSNPAnalysisMergeStage();

        List<BioAnalysisPipeline> subPipelines = new ArrayList<>(subPipelineStagesList.size() + 1);
        for (int i = 0; i < subPipelineStagesList.size() + 1; i++) {

            BioAnalysisPipeline subPipeline = new BioAnalysisPipeline();
            subPipeline.setParentPipelineId(bioAnalysisPipeline.getPipelineId());
            subPipeline.setPipelineParameters(bioAnalysisPipeline.getPipelineParameters());


            subPipeline.setPipelineType(PIPELINE_SNP_ANALYSIS);
            subPipeline.setStatus(PIPELINE_STATUS_PENDING);
            subPipeline.setAnalysisPipelineName(bioAnalysisPipeline.getAnalysisPipelineName() + "- 样本分析" + (i + 1));

            subPipeline.setProjectId(bioAnalysisPipeline.getProjectId());
            subPipelines.add(subPipeline);
        }

        BioAnalysisPipeline mergeResultSubPipeline = new BioAnalysisPipeline();
        mergeResultSubPipeline.setParentPipelineId(bioAnalysisPipeline.getPipelineId());
        mergeResultSubPipeline.setStatus(PIPELINE_STATUS_PENDING);
        mergeResultSubPipeline.setProjectId(bioAnalysisPipeline.getProjectId());
        mergeResultSubPipeline.setPipelineType(PIPELINE_SNP_ANALYSIS_MERGE);
        mergeResultSubPipeline.setAnalysisPipelineName(bioAnalysisPipeline.getAnalysisPipelineName() + "SNP结果汇总");

        List<BioPipelineStage> allSubPipelineStages = rcTransactionTemplate.execute((status) -> {

            BioAnalysisPipeline patch = new BioAnalysisPipeline();
            patch.setStatus(PIPELINE_STATUS_RUNNING);

            BioAnalysisPipelineExample queryCondition = new BioAnalysisPipelineExample();
            queryCondition.createCriteria().andParentPipelineIdEqualTo(bioAnalysisPipeline.getPipelineId());

            BioAnalysisPipelineMapper batchMapper = batchSqlSessionTemplate.getMapper(BioAnalysisPipelineMapper.class);

            batchMapper.updateByExampleSelective(patch, queryCondition);

            List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();

            boolean updateSuccess = false;
            for (BatchResult batchResult : batchResults) {
                for (int i : batchResult.getUpdateCounts()) {
                    if (i > 0) {
                        updateSuccess = true;
                        break;
                    }
                }
            }
            if (!updateSuccess) {
                return null;
            }

            for (BioAnalysisPipeline subPipeline : subPipelines) {
                batchMapper.insertSelective(subPipeline);
            }
            batchMapper.insertSelective(mergeResultSubPipeline);

            batchResults = batchSqlSessionTemplate.flushStatements();

            for (BatchResult batchResult : batchResults) {
                for (int i : batchResult.getUpdateCounts()) {
                    if (i == Statement.EXECUTE_FAILED) {
                        status.setRollbackOnly();
                        return null;
                    }
                }
            }

            queryCondition.clear();
            queryCondition.createCriteria().andParentPipelineIdEqualTo(bioAnalysisPipeline.getPipelineId());

            List<BioAnalysisPipeline> insertedSubPipelines = batchMapper.selectByExample(queryCondition);
            if (insertedSubPipelines.size() != subPipelines.size()+1) {
                status.setRollbackOnly();
                return null;
            }

            BioPipelineStageMapper stageBatchMapper = batchSqlSessionTemplate.getMapper(BioPipelineStageMapper.class);

            List<BioAnalysisPipeline> nonMergeSubPipeline = insertedSubPipelines.stream().filter(s->s.getPipelineType()!=PIPELINE_SNP_ANALYSIS_MERGE).toList();
            for (int i = 0; i < subPipelineStagesList.size(); i++) {
                List<BioPipelineStage> subPipelineStages = subPipelineStagesList.get(i);
                BioAnalysisPipeline subPipeline = nonMergeSubPipeline.get(i);
                for(BioPipelineStage subPipelineStage: subPipelineStages){
                    subPipelineStage.setPipelineId(subPipeline.getPipelineId());
                    stageBatchMapper.insertSelective(subPipelineStage);
                }
            }

            batchResults = batchSqlSessionTemplate.flushStatements();

            boolean allInserted = true;
            for(BatchResult batchResult:batchResults){
                for(int i:batchResult.getUpdateCounts()){
                    if(i == Statement.EXECUTE_FAILED){
                        allInserted = false;
                        break;
                    }
                }
            }

            if(!allInserted){
                status.setRollbackOnly();
                return null;
            }

            List<Long> subPipelineIds = new ArrayList<>(nonMergeSubPipeline.size()); 
            for(BioAnalysisPipeline subPipeline: nonMergeSubPipeline){
                subPipelineIds.add(subPipeline.getPipelineId());
            }

            BioPipelineStageExample stageQueryCondition = new BioPipelineStageExample();
            stageQueryCondition.createCriteria().andPipelineIdIn(subPipelineIds);

            return stageBatchMapper.selectByExampleWithBLOBs(stageQueryCondition);
        });

        if(allSubPipelineStages == null){
            return new Result(Result.INTERNAL_FAIL, null, "创建失败");
        }

        HashMap<Long, List<BioPipelineStage>> pipelineStageMap = new HashMap<>();
        for(BioPipelineStage stage:allSubPipelineStages){
            if(!pipelineStageMap.containsKey(stage.getPipelineId())){
                pipelineStageMap.put(stage.getPipelineId(), new ArrayList<>());
            }
            pipelineStageMap.get(stage.getPipelineId()).add(stage);
        }
        
        for(List<BioPipelineStage> pipelineStages: pipelineStageMap.values()){
            BioPipelineStage entry = pipelineStages.stream().filter(s->s.getStageIndex() == 0).findAny().orElse(null);
            this.scheduleStage(entry, pipelineStages);
        }

        return new Result(Result.SUCCESS, null, null);


        

    }

    private int startSinglePipeline(BioAnalysisPipeline bioAnalysisPipeline, List<BioPipelineStage> stages) {

        List<BioPipelineStage> pipelineInsertedStages = rcTransactionTemplate.execute((status) -> {

            BioAnalysisPipeline patch = new BioAnalysisPipeline();
            patch.setStatus(PIPELINE_STATUS_RUNNING);
            BioAnalysisPipelineExample updateCondition = new BioAnalysisPipelineExample();
            updateCondition.createCriteria().andPipelineIdEqualTo(bioAnalysisPipeline.getPipelineId());

            BioAnalysisPipelineMapper batchOperationMapper = this.batchSqlSessionTemplate
                    .getMapper(BioAnalysisPipelineMapper.class);

            batchOperationMapper.updateByExample(patch, updateCondition);

            List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();
            boolean updatedSuccess = false;

            for (BatchResult batchResult : batchResults) {
                for (int update : batchResult.getUpdateCounts()) {
                    if (update > 0) {
                        updatedSuccess = true;
                        break;
                    }
                }
            }

            if (!updatedSuccess) {
                return null;
            }

            BioPipelineStageMapper batchStageMapper = this.batchSqlSessionTemplate
                    .getMapper(BioPipelineStageMapper.class);

            for (BioPipelineStage stage : stages) {
                batchStageMapper.insertSelective(stage);
            }

            batchResults = batchSqlSessionTemplate.flushStatements();

            BioPipelineStageExample stageExample = new BioPipelineStageExample();
            stageExample.createCriteria().andPipelineIdEqualTo(bioAnalysisPipeline.getPipelineId());
            List<BioPipelineStage> pipelineStages = batchStageMapper.selectByExample(stageExample);

            if (stages.size() != pipelineStages.size()) {
                status.setRollbackOnly();
                return null;
            }

            return pipelineStages;
        });

        if (pipelineInsertedStages == null || pipelineInsertedStages.isEmpty()) {
            return -1;
        }

        BioPipelineStage entry = pipelineInsertedStages.stream().filter(stage -> stage.getStageIndex() == 0).findAny()
                .orElse(null);

        return this.scheduleStage(entry, pipelineInsertedStages);

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
            BioRefseq selectedRefSeqs = null;
            if (taxId != null) {
                BioRefseqExample bioRefseqExample = new BioRefseqExample();
                bioRefseqExample.createCriteria().andTaxIdEqualTo(taxId);
                refseqs = this.bioRefseqMapper.selectByExample(bioRefseqExample);
                if (refseqs.isEmpty()) {
                    return new Result(Result.INTERNAL_FAIL, null, "启动失败: 未找到指定参考基因组");
                }

                selectedRefSeqs = this.getBestCandicateRefSeqs(refseqs);
            }
            List<PipelineSampleInput> subPipelineSampleInputs = paritionSubPipelineSampleInputFiles(
                    bioPipelineInputFiles);

            List<List<BioPipelineStage>> subPipelineStages = new ArrayList<>();

            for (PipelineSampleInput sampleInput : subPipelineSampleInputs) {
                List<BioPipelineStage> stages = this.buildPipelineStages(pipelineId,
                        bioAnalysisPipeline.getPipelineType(), pipelineStageParameters, selectedRefSeqs, sampleInput);
                if (stages == null || stages.isEmpty()) {
                    return new Result(Result.INTERNAL_FAIL, null, "启动失败");
                }
                subPipelineStages.add(stages);
            }

            List<BioAnalysisPipeline> subPipelines = new ArrayList<>();

            if (subPipelineStages.size() >= 2) {
                for (int i = 0; i < subPipelineStages.size(); i++) {
                    BioAnalysisPipeline subPipeline = new BioAnalysisPipeline();
                    subPipeline.setProjectId(bioAnalysisPipeline.getProjectId());
                    subPipeline.setParentPipelineId(bioAnalysisPipeline.getPipelineId());
                    subPipeline.setCreateTime(bioAnalysisPipeline.getCreateTime());
                    subPipeline
                            .setAnalysisPipelineName(bioAnalysisPipeline.getAnalysisPipelineName() + "-样本-" + (i + 1));
                    subPipeline.setStatus(PIPELINE_STATUS_RUNNING);
                }
            }

            List<List<BioPipelineStage>> allSubPipelineStages = rcTransactionTemplate.execute((status) -> {

                BioAnalysisPipelineMapper analysisPipelineMapper = batchSqlSessionTemplate
                        .getMapper(BioAnalysisPipelineMapper.class);

                BioAnalysisPipeline pipelineUpdate = new BioAnalysisPipeline();
                pipelineUpdate.setStatus(PIPELINE_STATUS_RUNNING);

                BioAnalysisPipelineExample updateExample = new BioAnalysisPipelineExample();
                updateExample.createCriteria().andPipelineIdEqualTo(bioAnalysisPipeline.getPipelineId());

                analysisPipelineMapper.updateByExampleSelective(pipelineUpdate, updateExample);

                List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();

                boolean updated = false;

                for (BatchResult batchResult : batchResults) {
                    int[] counts = batchResult.getUpdateCounts();
                    if (counts != null) {
                        for (int count : counts) {
                            if (count > 0) {
                                updated = true;
                            }
                        }
                    }
                }

                if (!updated) {
                    return null;
                }

                // single, just run normally
                if (subPipelineStages.size() < 2) {

                }

            });

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

    // private Result startPipeline(long pipelineId, List<BioPipelineInputFile>
    // inputFiles) {

    // try {

    // boolean lockSuccess = pipelineLock.add(pipelineId);
    // if (!lockSuccess) {
    // // another is processing
    // return new Result(Result.DUPLICATE_OPERATION, null, "流水线正在启动");
    // }

    // BioPipelineStageExample bioPipelineStageExample = new
    // BioPipelineStageExample();
    // bioPipelineStageExample.createCriteria().andPipelineIdEqualTo(pipelineId);
    // List<BioPipelineStage> stages =
    // this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample);

    // if (stages.isEmpty()) {

    // List<BioPipelineStage> bulidStages = this.buildPipelineStages(pipelineId,
    // inputFiles);

    // if (bulidStages.isEmpty()) {
    // logger.error("[Create stage]: Pipeline = {}, Not stages build", pipelineId);
    // }

    // boolean insertSuccess = rcTransactionTemplate.execute((status) -> {

    // BioPipelineStageMapper bioPipelineStageMapper = batchSqlSessionTemplate
    // .getMapper(BioPipelineStageMapper.class);

    // for (BioPipelineStage stage : bulidStages) {
    // bioPipelineStageMapper.insertSelective(stage);
    // }

    // try {
    // batchSqlSessionTemplate.flushStatements();
    // return true;
    // } catch (Exception e) {

    // logger.error("[Create stages]: pipeline = {}, insertion fail", pipelineId,
    // e);
    // return false;

    // }
    // });

    // if (!insertSuccess) {
    // return new Result(Result.INTERNAL_FAIL, null, "启动流水线分析任务失败");
    // }
    // }

    // // get stage id here
    // stages = stages.isEmpty() ?
    // this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample) :
    // stages;

    // // prerequite: Not null. First stage must exists;
    // BioPipelineStage firstStage = stages.stream().filter(s -> s.getStageIndex()
    // == 0).findAny().orElse(null);
    // if (!this.pipelineStageTaskDispatcher.isStageIn(firstStage.getStageId())
    // && firstStage.getStatus() != PIPELINE_STAGE_STATUS_QUEUING
    // && firstStage.getStatus() != PIPELINE_STAGE_STATUS_RUNNING) {
    // int res = scheduleStage(firstStage, stages);
    // if (res == OK) {
    // return new Result(Result.SUCCESS, null, null);
    // }

    // if (res == INTERNAL_FAIL || res == NO_EFFECTIVE_UPDATE) {
    // return new Result(Result.INTERNAL_FAIL, null, "内部错误");
    // }
    // }
    // return new Result(Result.SUCCESS, null, null);

    // } catch (Exception e) {
    // logger.error("[Create stages]: pipeline = {}, exception = ", pipelineId, e);
    // return new Result(Result.INTERNAL_FAIL, null, "启动分析任务失败");
    // } finally {
    // pipelineLock.remove(pipelineId);
    // }

    // }

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

    private int sampleReadTypeCode(String fileName) {

        boolean isFastq = fileName.endsWith(".fastq") ||
                fileName.endsWith(".fq") ||
                fileName.endsWith(".fastq.gz") ||
                fileName.endsWith(".fq.gz");

        if (isFastq) {
            return PipelineSampleInput.READ_TYPE_FASTQ;
        }

        return PipelineSampleInput.READ_TYPE_FASTA;
    }

    private List<BioPipelineStage> buildPipelineStages(long pipelineId, int pipelineType,
            PipelineStageParameters pipelineStageParameters, BioRefseq refseq, PipelineSampleInput pipelineSampleInput)
            throws JsonMappingException, JsonProcessingException {

        PipelineConfigurations pipelineConfigurations = new PipelineConfigurations();
        List<BioPipelineStage> stages = Collections.emptyList();

        if (refseq != null) {
            pipelineConfigurations.setRefId(refseq.getRefId());
            List<String> accessions = JsonUtil.toObject(refseq.getAccessions(), List.class);
            pipelineConfigurations.setRefseqAccessions(accessions);
        }

        if (pipelineType != PIPELINE_SNP_ANALYSIS) {
            if (pipelineType == PIPELINE_VIRUS
                    || pipelineType == PIPELINE_VIRUS_COVID) {

                stages = AnalysisPipelineStagesBuilder.buildVirusStages(
                        pipelineSampleInput, pipelineConfigurations);
            } else {
                stages = AnalysisPipelineStagesBuilder.buildRegularBacteriaPipeline(pipelineSampleInput,
                        pipelineConfigurations);
            }
        } else if (pipelineType == PIPELINE_SNP_ANALYSIS) {
            stages = AnalysisPipelineStagesBuilder.buildSNPAnalysisStages(pipelineSampleInput, pipelineConfigurations);
        }

        for (BioPipelineStage stage : stages) {
            stage.setPipelineId(pipelineId);
        }

        return stages;

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
