package com.xjtlu.bio.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.xjtlu.bio.stageDoneHandler.StageDoneHandler;
import com.xjtlu.bio.taskrunner.parameters.RefSeqConfig;
import com.xjtlu.bio.taskrunner.stageOutput.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.executor.BatchResult;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioSampleMapper;
import com.xjtlu.bio.parameters.CreateSampleRequest.PipelineStageParameters;
import com.xjtlu.bio.service.StorageService.PutResult;
import com.xjtlu.bio.taskrunner.PipelineStageTaskDispatcher;
import com.xjtlu.bio.utils.BioStageUtil;
import com.xjtlu.bio.utils.JsonUtil;
import com.xjtlu.bio.utils.StageOrchestrator;
import com.xjtlu.bio.utils.StageOrchestrator.OrchestratePlan;

import io.micrometer.common.util.StringUtils;
import jakarta.annotation.Resource;

class BioPipelineStagesBuilder {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static Map<String, Object> substractStageParams(String stageName, Map<String, Object> pipelineStageParams) {
        Object obj = pipelineStageParams.get(stageName);
        if (obj == null) {
            return new HashMap<>();
        }
        return (Map) obj;
    }

    public static List<BioPipelineStage> buildBacteriaStages() {
        // todo
        return null;
    }

    private static BioPipelineStage buildReadLengthDetectStage(long pid, String readUrl, int stageIndex) {
        BioPipelineStage bioPipelineStage = new BioPipelineStage();
        bioPipelineStage.setInputUrl(readUrl);
        bioPipelineStage.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        bioPipelineStage.setPipelineId(pid);
        bioPipelineStage.setStageName(PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT_NAME);
        bioPipelineStage.setStageIndex(stageIndex);
        bioPipelineStage.setStageType(PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT);
        return bioPipelineStage;
    }

    public static List<BioPipelineStage> buildVirusStages(long pid, int pipelineType, BioSample bioSample,
            PipelineStageParameters pipelineStageParams) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>(16);
        String qcInputRead1 = bioSample.getRead1Url();
        String qcInputRead2 = bioSample.getRead2Url();

        Object refseqObj = pipelineStageParams==null?null:pipelineStageParams.getRefseq();
        long refseqId = -1;

        String longReadParamKey = PipelineService.PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY;

        Map<String,Object> pipelineStagesConfig = pipelineStageParams==null?new HashMap<>():pipelineStageParams.getExtraParams();

        if (refseqObj != null) {
            refseqId = refseqObj instanceof Integer ? (Integer) refseqObj
                    : refseqObj instanceof Long ? (Long) refseqObj : -1;
        }

        
        RefSeqConfig refseqConfig = new RefSeqConfig();

        if (refseqId >= 0) {
            refseqConfig.setInnerRefSeq(true);
            refseqConfig.setRefseqId(refseqId);
        }else {
            refseqConfig.setInnerRefSeq(false);
            refseqConfig.setRefseqId(-1);
        }

        int index = 0;

        boolean isPair = bioSample.getIsPair();
        if (!isPair) {
            BioPipelineStage readLengthDetectStage = buildReadLengthDetectStage(pid, bioSample.getRead1Url(), index);
            stages.add(readLengthDetectStage);
            index++;
        }

        BioPipelineStage qc = new BioPipelineStage();

        Map<String, String> qcInputMap = new HashMap<>();
        Map<String,Object> qcParams = substractStageParams("qc", pipelineStagesConfig);
        
        qcParams.put(longReadParamKey, false);
        

        qcInputMap.put(PipelineService.PIPELINE_STAGE_QC_INPUT_R1, qcInputRead1);
        qcInputMap.put(PipelineService.PIPELINE_STAGE_QC_INPUT_R2, qcInputRead2);
        String qcInputMapStr = objectMapper.writeValueAsString(qcInputMap);
        qc.setStageIndex(index);
        qc.setStageName(PipelineService.PIPELINE_STAGE_NAME_QC);
        qc.setStageType(PipelineService.PIPELINE_STAGE_QC);
        qc.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        qc.setPipelineId(pid);
        qc.setInputUrl(qcInputMapStr);
        qc.setParameters(objectMapper.writeValueAsString(qcParams));
        stages.add(qc);

        index++;

        if (refseqId == -1) {

            BioPipelineStage assembly = new BioPipelineStage();

            //TODO
            Map<String,Object> assemblyParams = substractStageParams("assembly", pipelineStagesConfig);
            
            assemblyParams.put(longReadParamKey, false);
            
            String assemlyParamsStr = objectMapper.writeValueAsString(assemblyParams);
            assembly.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
            assembly.setPipelineId(pid);
            assembly.setStageIndex(index);
            assembly.setParameters(assemlyParamsStr);
            assembly.setStageType(PipelineService.PIPELINE_STAGE_ASSEMBLY);
            assembly.setStageName(PipelineService.PIPELINE_STAGE_NAME_ASSEMBLY);
            stages.add(assembly);
            index++;
        }

        BioPipelineStage mappingStage = new BioPipelineStage();
        mappingStage.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        mappingStage.setPipelineId(pid);
        mappingStage.setStageIndex(index);
        mappingStage.setStageType(PipelineService.PIPELINE_STAGE_MAPPING);
        mappingStage.setStageName(PipelineService.PIPELINE_STAGE_NAME_MAPPING);



        
        Map<String, Object> mappingParams = substractStageParams("mapping", pipelineStagesConfig);
        mappingParams.put(longReadParamKey, false);
        mappingParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        mappingStage.setParameters(objectMapper.writeValueAsString(mappingParams));

        stages.add(mappingStage);
        index++;

        Map<String, Object> varientStageParams = substractStageParams("varient", pipelineStagesConfig);
        varientStageParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        varientStageParams.put(longReadParamKey, false);
        BioPipelineStage varient = new BioPipelineStage();
        varient.setStageName(PipelineService.PIPELINE_STAGE_NAME_VARIANT);
        varient.setPipelineId(pid);
        varient.setStageIndex(index);

        varient.setParameters(objectMapper.writeValueAsString(varientStageParams));
        varient.setStageType(PipelineService.PIPELINE_STAGE_VARIANT_CALL);
        varient.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);

        stages.add(varient);

        index++;

        BioPipelineStage consensus = new BioPipelineStage();
        Map<String, Object> consesusParams = substractStageParams("consensus", pipelineStagesConfig);
        consesusParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        consesusParams.put(longReadParamKey, false);
        consensus.setStageName("生成一致性序列");
        consensus.setPipelineId(pid);
        consensus.setStageIndex(index);
        consensus.setParameters(objectMapper.writeValueAsString(consesusParams));
        consensus.setStageType(PipelineService.PIPELINE_STAGE_CONSENSUS);
        consensus.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(consensus);

        if (pipelineType != PipelineService.PIPELINE_VIRUS_COVID) {
            return stages;
        }

        Map<String, Object> snpParams = substractStageParams("snp", pipelineStagesConfig);
        snpParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        snpParams.put(longReadParamKey, false);
        BioPipelineStage snp = new BioPipelineStage();
        snp.setStageName("SNP注释");
        snp.setPipelineId(pid);
        snp.setStageIndex(index);
        snp.setParameters(objectMapper.writeValueAsString(snp));
        snp.setStageType(PipelineService.PIPELINE_STAGE_SNP_CORE);
        snp.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);

        index++;

        BioPipelineStage depthConverage = new BioPipelineStage();
        Map<String, Object> depthParams = substractStageParams("depth", pipelineStagesConfig);
        depthParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        depthParams.put(longReadParamKey, false);
        depthConverage.setStageName("深度分布图");
        depthConverage.setPipelineId(pid);
        depthConverage.setStageIndex(index);
        depthConverage.setParameters(objectMapper.writeValueAsString(depthParams));
        depthConverage.setStageType(PipelineService.PIPELINE_STAGE_DEPTH_COVERAGE);
        depthConverage.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);
        index++;


        return stages;

    }

}

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
    private BioSampleMapper bioSampleMapper;

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


    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;

    //public static final int PIPELINE_STAGE_STATUS_NOT_READY = -1;
    public static final int PIPELINE_STAGE_STATUS_PENDING = 0;
    public static final int PIPELINE_STAGE_STATUS_QUEUING = 1;
    public static final int PIPELINE_STAGE_STATUS_RUNNING = 2;
    public static final int PIPELINE_STAGE_STATUS_FAIL = 3;
    public static final int PIPELINE_STAGE_STATUS_FINISHED = 4;
    public static final int PIPELINE_STAGE_STATUS_ACTION_REQUIRED = 5;

    public static final String PIPELINE_REFSEQ_ACCESSION_KEY = "refSeq";

    public static final String PIPELINE_STAGE_NAME_QC = "质控 (QC)";
    public static final String PIPELINE_STAGE_NAME_ASSEMBLY = "组装 (Assembly)";
    public static final String PIPELINE_STAGE_NAME_MAPPING = "有参比对 (Mapping)";
    public static final String PIPELINE_STAGE_NAME_VARIANT = "变异检测 (Variant calling)";

    public static final String PIPELINE_STAGE_INPUT_READ1_KEY = "r1";
    public static final String PIPELINE_STAGE_INPUT_READ2_KEY = "r2";

    public static final String PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG = "refseq_config";
    public static final String PIPLEINE_STAGE_PARAMETERS_REFSEQ_KEY = "refseq";
    public static final String PIPELINE_STAGE_PARAMETERS_REFSEQ_IS_INNER = "inner";

    // 物种鉴定
    public static final String PIPELINE_STAGE_NAME_TAXONOMY = "物种鉴定 (Taxonomy)";

    // 比对 / 组装相关
    public static final String PIPELINE_STAGE_NAME_ASSEMBLY_POLISH = "组装抛光 (Polishing)";
    public static final String PIPELINE_STAGE_NAME_CONSENSUS = "一致性序列 (Consensus)";
    public static final String PIPELINE_STAGE_NAME_DEPTH_COVERAGE = "深度分布图 (Depth / Coverage)";

    // 功能注释
    public static final String PIPELINE_STAGE_NAME_FUNC_ANNOTATION = "功能注释 (Functional annotation)";

    // 细菌病原学特征
    public static final String PIPELINE_STAGE_NAME_AMR = "耐药基因分析 (AMR)";
    public static final String PIPELINE_STAGE_NAME_VIRULENCE = "毒力因子分析 (Virulence)";
    public static final String PIPELINE_STAGE_NAME_MLST = "MLST 分型";
    public static final String PIPELINE_STAGE_NAME_CGMLST = "cgMLST 分型";
    public static final String PIPELINE_STAGE_NAME_SEROTYPE = "血清型预测 (Serotyping)";

    // SNP / 溯源
    public static final String PIPELINE_STAGE_NAME_SNP_SINGLE = "单样本 SNP 分析";
    public static final String PIPELINE_STAGE_NAME_SNP_CORE = "核心 SNP 分析 / 建树";

    public static final String PIPELINE_STAGE_QC_INPUT_R1 = "r1";
    public static final String PIPELINE_STAGE_QC_INPUT_R2 = "r2";

    public static final String PIPELINE_STAGE_QC_OUTPUT_R1 = "trimmed_r1";
    public static final String PIPELINE_STAGE_QC_OUTPUT_R2 = "trimmed_r2";
    public static final String PIPELINE_STAGE_QC_OUTPUT_JSON = "qc_json";
    public static final String PIPELINE_STAGE_QC_OUTPUT_HTML = "qc_html";

    public static final String PIPELINE_STAGE_ASSEMBLY_INPUT_R1 = "r1";
    public static final String PIPELINE_STAGE_ASSEMBLY_INPUT_R2 = "r2";

    public static final String PIPELINE_STAGE_MAPPING_INPUT_R1 = "r1";
    public static final String PIPELINE_STAGE_MAPPING_INPUT_R2 = "r2";

    public static final String PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY = "bam";
    public static final String PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY = "bamIndex";

    public static final String PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY = "contigs";
    public static final String PIPELINE_STAGE_ASSEMBLY_OUTPUT_SCAFFOLDS_KEY = "scaffold";

    public static final String PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ = "vcf.gz";
    public static final String PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI = "vcf.tbi";

    public static final String PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ = "vcfGz";
    public static final String PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ_TBI = "vcfGzTbi";

    public static final String PIPELINE_STAGE_CONSENSUS_OUTPUT_CONSENSUSFA = "consensus";

    public static final String PIPELINE_STAGE_AMR_INPUT_SAMPLE = "reads";
    public static final String PIPELINE_STAGE_AMR_OUTPUT_RESULT = "amr_result";

    public static final String PIPELINE_STAGE_MLST_INPUT = "contigs";
    public static final String PIPELINE_STAGE_MLST_OUTPUT = "mlstResult";

    public static final int PIPELINE_STAGE_QC = 0; // 质控 fastp
    public static final int PIPELINE_STAGE_TAXONOMY = 10; // 物种鉴定 Kraken2/Mash

    // 比对 / 组装
    public static final int PIPELINE_STAGE_MAPPING = 20; // 有参比对 minimap2/bwa
    public static final int PIPELINE_STAGE_ASSEMBLY = 30; // 无参拼装 SPAdes/Flye
    public static final int PIPELINE_STAGE_ASSEMBLY_POLISH = 31; // 抛光 Pilon/Racon/Medaka

    // 变异 / 一致性 / 深度（病毒常用）
    public static final int PIPELINE_STAGE_VARIANT_CALL = 40; // 变异调用 bcftools/snippy
    public static final int PIPELINE_STAGE_CONSENSUS = 41; // 一致性序列 bcftools consensus
    public static final int PIPELINE_STAGE_DEPTH_COVERAGE = 42; // 覆盖度/深度图 mosdepth

    public static final String PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY = "bam";
    public static final String PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_INDEX_KEY = "bamIndex";
    public static final String PIPELINE_STAGE_VARIENT_CALL_INPUT_REFSEQ_KEY = "refseq";


    public static final String PIPELINE_STAGE_VIRULENCE_FACTOR_INPUT = "contigs";
    public static final String PIPELINE_STAGE_VIRULENCE_FACTOR_OUTPUT = "vfResult";

    public static final String PIPELINE_STAGE_SEROTYPING_INPUT = "contigs";
    public static final String PIPELINE_STAGE_SEROTYPING_OUTPUT = "serotypingResult";


    // 功能注释（可选通用）
    public static final int PIPELINE_STAGE_FUNC_ANNOTATION = 50; // Prokka/Bakta/eggNOG

    // 病原学特征（细菌模块）
    public static final int PIPELINE_STAGE_AMR = 60; // 耐药基因 AMRFinder/ResFinder
    public static final int PIPELINE_STAGE_VIRULENCE = 61; // 毒力因子 VFDB/abricate
    public static final int PIPELINE_STAGE_MLST = 62; // MLST 分型
    public static final int PIPELINE_STAGE_CGMLST = 63; // cgMLST chewBBACA
    public static final int PIPELINE_STAGE_SEROTYPE = 64; // 血清型（ECTyper/SeqSero2/Kaptive等）

    // SNP & 溯源
    public static final int PIPELINE_STAGE_SNP_SINGLE = 70; // 单样本对近邻参考的SNP
    public static final int PIPELINE_STAGE_SNP_CORE = 71; // 多样本核心SNP/建树

    public static final int PIPELINE_STAGE_READ_LENGTH_DETECT = 80;
    public static final String PIPELINE_STAGE_READ_LENGTH_DETECT_NAME = "读长与格式识别";

    public static final String PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY = "isLongRead";

    public static final String stageOutputFormat = "stageOutput/%d/%d/%s";

    private boolean isLegalPipelineType(int pipelineType) {
        return pipelineType == PIPELINE_VIRUS || pipelineType == PIPELINE_VIRUS_COVID
                || pipelineType == PIPELINE_VIRUS_BACKTERIA;
    }

    public int startStageExecute(BioPipelineStage pipelineStage){
        BioPipelineStage updateStage = new BioPipelineStage();
        int currentVersion = pipelineStage.getVersion();
        updateStage.setVersion(currentVersion+1);
        pipelineStage.setVersion(currentVersion+1);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        pipelineStage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        return this.updateStageFromVersion(updateStage, pipelineStage.getStageId(), currentVersion);
    }


    public int updateStageFromVersion(BioPipelineStage updateStage, long updateStageId, int currentVersion){
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria().andStageIdEqualTo(updateStageId).andVersionEqualTo(currentVersion);
        updateStage.setVersion(currentVersion+1);
        try {
            return this.bioPipelineStageMapper.updateByExampleSelective(updateStage, bioPipelineStageExample);
        } catch (Exception e) {
            logger.error("update stage id {} to {} from version {} exception: ",updateStageId, updateStage, currentVersion, e);
            return -1;
        }
    }





    private int mapSampleTypeToPipelineType(int sampleType) {
        if (sampleType == SampleService.SAMPLE_TYPE_VIRUS) {
            return PIPELINE_VIRUS;
        }
        if (sampleType == SampleService.SAMPLE_TYPE_BACTERIA) {
            return PIPELINE_VIRUS_BACKTERIA;
        }
        return PIPELINE_VIRUS_COVID;
    }



    //transaction required
    private int batchInsertStages(List<BioPipelineStage> insertStages){
        try {
            BioPipelineStageMapper stageMapper = batchSqlSessionTemplate.getMapper(BioPipelineStageMapper.class);

            for (BioPipelineStage bioPipelineStage : insertStages) {
                stageMapper.insertSelective(bioPipelineStage);
            }

            List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();
            int updateSuccessCount = 0;
            for(BatchResult batchResult: batchResults){
                for(int i:batchResult.getUpdateCounts()){
                    updateSuccessCount += i;
                }
            }
            return updateSuccessCount;
        }catch (Exception e){
            logger.error("insert expcetion", e);
            return -1;
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public Result<Long> createPipeline(BioSample bioSample,
            PipelineStageParameters pipelineStageParams) {

        BioAnalysisPipeline bioAnalysisPipeline = new BioAnalysisPipeline();
        bioAnalysisPipeline.setPipelineType(mapSampleTypeToPipelineType(bioSample.getSampleType()));
        bioAnalysisPipeline.setSampleId(bioSample.getSid());
        int insertRes = this.bioAnalysisPipelineMapper.insert(bioAnalysisPipeline);
        if (insertRes < 1) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        List<BioPipelineStage> stages = this.buildPipelineStages(bioSample, bioAnalysisPipeline, pipelineStageParams);
        if (stages == null || stages.isEmpty()) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        for(BioPipelineStage stage:stages){
            stage.setVersion(0);
        }

        insertRes = this.bioAnalysisStageMapperExtension.batchInsert(stages);
        if (insertRes != stages.size()) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        return new Result<Long>(Result.SUCCESS, bioAnalysisPipeline.getPipelineId(), null);
    }

    private List<BioPipelineStage> buildPipelineStages(BioSample bioSample, BioAnalysisPipeline bioAnalysisPipeline,
            PipelineStageParameters pipelineParams) {

        if (bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS
                || bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS_COVID) {
            try {
                List<BioPipelineStage> stages = BioPipelineStagesBuilder.buildVirusStages(
                        bioAnalysisPipeline.getPipelineId(), bioAnalysisPipeline.getPipelineType(), bioSample,
                        pipelineParams);
                return stages;
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                return null;
            }
        } else {
            return null;
        }
    }


    //TODO: this is an quick method for just test. Never Use it in production env or treat it as normal service method
    public Result<Boolean> restartStage(long stageId){

        List<BioPipelineStage> allStages = this.bioAnalysisStageMapperExtension.selectAllStagesByStageId(stageId);
        if(allStages == null || allStages.isEmpty()){
            return new Result<>(Result.BUSINESS_FAIL, false, "未能启动");
        }

        BioPipelineStage startStage = null;
        BioPipelineStage lastStage = null;
        for(BioPipelineStage stage:allStages){
            if(stage.getStageId() == stageId){
                startStage = stage;
                break;
            }
        }

        if(this.pipelineStageTaskDispatcher.isStageIn(stageId)){
            return new Result<Boolean>(Result.SUCCESS, true, null);
        }
        
        if(startStage.getStatus() == PIPELINE_STAGE_STATUS_QUEUING){
            this.addStageTask(startStage);
            return new Result<Boolean>(Result.SUCCESS, true, null);
        }

        int curStatus = startStage.getStatus();
        if(curStatus!=PIPELINE_STAGE_STATUS_PENDING && curStatus!=PIPELINE_STAGE_STATUS_FAIL){
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "不能启动状态为非等待的分析阶段");
        }

        if(startStage.getStageIndex() == 0){

            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setVersion(startStage.getVersion());
            updateStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            int res = this.updateStageFromVersion(updateStage, stageId, startStage.getVersion());
            if(res==1){
                startStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                startStage.setVersion(startStage.getVersion()+1);
                this.pipelineStageTaskDispatcher.addTask(startStage);
                return new Result<Boolean>(Result.SUCCESS, true, null);
            }

            if(res == -1){
                return new Result<Boolean>(Result.INTERNAL_FAIL, false, "启动失败");
            }else {
                return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未能启动");
            }
        }

        for(BioPipelineStage stage:allStages){
            if(startStage.getStageIndex() == stage.getStageIndex()+1){
                lastStage = stage;
                break;
            }
        }


        if(lastStage == null || lastStage.getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
            return new Result<>(Result.BUSINESS_FAIL, false, "未能启动");
        }

        Map<String,String> inputMap = null;
        String serializedInputMap = null;
        try {
            inputMap = bioStageUtil.createInputMapForNextStage(lastStage, startStage);
            serializedInputMap = JsonUtil.toJson(inputMap);
        } catch (JsonProcessingException e) {
            logger.error("parsing exception", e);
            return new Result<>(Result.INTERNAL_FAIL, false, "内部错误");
        }

        List<BioPipelineStage> updateStages = new ArrayList<>();
        final BioPipelineStage startStageRef = startStage;
        List<BioPipelineStage> followingStages = allStages.stream().filter((stage)->{return stage.getStageIndex() >= startStageRef.getStageIndex();}).toList();

        if(lastStage.getStageType() == PIPELINE_STAGE_ASSEMBLY){
            try {
                Map<String,String> outputMap = JsonUtil.toMap(lastStage.getOutputUrl(),String.class);
                String contigUrl = outputMap.get(PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY);
                for(BioPipelineStage followingStage: followingStages){
                    BioPipelineStage updateStage = new BioPipelineStage();

                    Map<String,Object> params = JsonUtil.toMap(followingStage.getParameters());
                    RefSeqConfig refSeqConfig = new RefSeqConfig(contigUrl);
                    params.put(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refSeqConfig);
                    String serializedUpdatedParams = JsonUtil.toJson(params);
                    updateStage.setParameters(serializedUpdatedParams);

                    updateStage.setVersion(followingStage.getVersion());
                    updateStage.setStageId(followingStage.getStageId());

                    if(followingStage.getStageIndex() == startStage.getStageIndex()){
                        updateStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                    }
                    updateStages.add(updateStage);
                }
            } catch (JsonProcessingException e) {
                logger.error("parsing exception", e);
                return new Result<>(Result.INTERNAL_FAIL, false, "内部错误");
            }
        } else {
            BioPipelineStage updateStartStage = new BioPipelineStage();
            updateStartStage.setStageId(stageId);
            updateStartStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            updateStartStage.setVersion(startStageRef.getVersion());
            updateStartStage.setInputUrl(serializedInputMap);
            updateStages.add(updateStartStage);
        }

        Map<String,String> inputMapForNextStage = null;
        try {
            inputMapForNextStage = bioStageUtil.createInputMapForNextStage(lastStage, startStage);
            serializedInputMap = JsonUtil.toJson(inputMapForNextStage);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            logger.error("parsing exception",e);
            return new Result<Boolean>(Result.INTERNAL_FAIL, false, "unable to restart");
        }

        for(BioPipelineStage bioPipelineStage: updateStages){
            if(bioPipelineStage.getStageId() == stageId){
                bioPipelineStage.setInputUrl(serializedInputMap);
                break;
            }
        }

        

        int updateRes = this.batchUpdateStages(updateStages);
        if(updateRes==1){
            startStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            startStage.setVersion(startStage.getVersion()+1);
            startStage.setInputUrl(serializedInputMap);
            this.addStageTask(startStage);
            return new Result<>(Result.SUCCESS, true, null);
        }
        return new Result<>(Result.BUSINESS_FAIL, false, "unable to start");
    }




    @Transactional(rollbackFor = Exception.class)
    public Result<Boolean> pipelineStart(long sampleId) {

        

        List<BioPipelineStage> stages = this.bioAnalysisStageMapperExtension.selectStagesBySampleId(sampleId);
        if (stages == null || stages.isEmpty()) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到流水线");
        }
        BioPipelineStage firstStage = null;
        for(BioPipelineStage stage:stages){
            if(stage.getStageIndex() == 0){
                firstStage = stage;
                break;
            }
        }

        if(firstStage == null){
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未能找到初始任务");
        }
        if(firstStage.getStatus() != PIPELINE_STAGE_STATUS_PENDING){
            return new Result<Boolean>(Result.SUCCESS, null, null);
        }

        int curVersion = firstStage.getVersion();

        BioPipelineStage updatedFirstStage = new BioPipelineStage();
        updatedFirstStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        firstStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

        updatedFirstStage.setVersion(firstStage.getVersion() + 1);
        firstStage.setVersion(firstStage.getVersion()+1);
        int updateRes = this.updateStageFromVersion(updatedFirstStage, firstStage.getStageId(), curVersion);
        if (updateRes < 0) {
            return new Result<Boolean>(Result.INTERNAL_FAIL, false, "流水线启动失败");
        }


        if(updateRes==1){
            this.pipelineStageTaskDispatcher.addTask(firstStage);
        }
        return new Result<Boolean>(Result.SUCCESS, true, null);
    }

    @Async
    public void pipelineStageDone(StageRunResult stageRunResult) {
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        int stageType = bioPipelineStage.getStageType();
        StageDoneHandler stageDoneHandler = stageDoneHandlerMap.get(stageType);

        if (!stageRunResult.isSuccess()) {
            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            updateStage.setVersion(bioPipelineStage.getVersion()+1);
            int res = this.updateStageFromVersion(updateStage, bioPipelineStage.getStageId(),
                    bioPipelineStage.getVersion());
            logger.info("{} execute failed {}", stageRunResult.getStage(), stageRunResult.getErrorLog());
            return;
        }

        boolean handleRes = stageDoneHandler.handleStageDone(stageRunResult);
        if(!handleRes){return;}
        
    }


    private void doNextStages(long curStageId){

        


    }


    

    public int markStageFinish(BioPipelineStage bioPipelineStage, String outputUrl) {
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setOutputUrl(outputUrl);
        updateStage.setEndTime(new Date());
        updateStage.setVersion(bioPipelineStage.getVersion()+1);
        return this.updateStageFromVersion(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion());
    }


    public List<BioPipelineStage> getStagesFromExample(BioPipelineStageExample bioPipelineStageExample){

        List<BioPipelineStage> stages = null;
        try {
            stages = bioPipelineStageMapper.selectByExampleWithBLOBs(bioPipelineStageExample);
        }catch (Exception e){
            logger.error("", e);
        }
        return stages;
    }


    //如果更新成功的数量!=list.size()回滚
    //0: fail. 1: success
    //remember to remain current version in stage here
    public int batchUpdateStages(List<BioPipelineStage> updateStages){

        int res = 0;

        try {
            res = rcTransactionTemplate.execute((status) -> {
                BioPipelineStageMapper batchUpdateMapper = batchSqlSessionTemplate.getMapper(BioPipelineStageMapper.class);
                for(BioPipelineStage updateStage: updateStages){
                    BioPipelineStageExample conditionExample = new BioPipelineStageExample();
                    int currentVersion = updateStage.getVersion();
                    updateStage.setVersion(currentVersion+1);
                    long stageId = updateStage.getStageId();
                    updateStage.setStageId(null);
                    conditionExample.createCriteria().andStageIdEqualTo(stageId).andVersionEqualTo(currentVersion);
                    batchUpdateMapper.updateByExampleSelective(updateStage, conditionExample);
                }
                List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();
                int count = 0;
                for(BatchResult result:batchResults){
                    for(int i:result.getUpdateCounts()){
                        count+=i;
                    }
                }

                if(count!=updateStages.size()){
                    status.setRollbackOnly();
                    return 0;
                }
                return 1;
            });
        }catch (Exception e){
            return -1;
        }

        return res;

    }


    public boolean addStageTask(BioPipelineStage stage){
        return this.pipelineStageTaskDispatcher.addTask(stage);
    }


}
