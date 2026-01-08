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

import org.apache.commons.io.FileUtils;
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
import com.xjtlu.bio.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.ConsensusStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.QCStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.ReadLengthDetectStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.VariantStageOutput;
import com.xjtlu.bio.utils.BioStageUtil;

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

        HashMap<String, Object> refseqConfig = new HashMap<>();

        if (refseqId >= 0) {
            refseqConfig.put(PipelineService.PIPLEINE_STAGE_PARAMETERS_REFSEQ_KEY, refseqId);
            refseqConfig.put(PipelineService.PIPELINE_STAGE_PARAMETERS_REFSEQ_IS_INNER, true);
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
        

        qcInputMap.put(PipelineService.PIPELINE_STAGE_INPUT_READ1_KEY, qcInputRead1);
        qcInputMap.put(PipelineService.PIPELINE_STAGE_INPUT_READ2_KEY, qcInputRead2);
        String qcInputMapStr = null;
        qc.setStageIndex(index);
        qc.setStageName("质控(QC)");
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
            assembly.setStageName("组装");
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

    private ObjectMapper jsonMapper = new ObjectMapper();

    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;

    //public static final int PIPELINE_STAGE_STATUS_NOT_READY = -1;
    public static final int PIPELINE_STAGE_STATUS_PENDING = 0;
    public static final int PIPELINE_STAGE_STATUS_QUEUING = 1;
    public static final int PIPELINE_STAGE_STATUS_RUNNING = 2;
    public static final int PIPELINE_STAGE_STATUS_FAIL = 3;
    public static final int PIPELINE_STAGE_STATUS_FINISHED = 4;

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
    public static final String PIPELINE_STAGE_QC_OUTPUI_JSON = "qc_json";
    public static final String PIPELINE_STAGE_QC_OUTPUT_HTML = "qc_html";

    public static final String PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY = "bam";
    public static final String PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY = "bamIndex";

    public static final String PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY = "contigs";
    public static final String PIPELINE_STAGE_ASSEMBLY_OUTPUT_SCAFFOLDS_KEY = "scaffold";

    public static final String PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ = "vcf.gz";
    public static final String PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI = "vcf.tbi";

    public static final String PIPELINE_STAGE_CONSENSUS_OUTPUT_CONSENSUSFA = "consensus";

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
    public static final String PIPELINE_STAGE_VARIENT_CALL_INPUT_REFSEQ_KEY = "refseq";

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

    // 0: success
    // -1: error
    @Transactional(rollbackFor = Exception.class)
    public int updateStageFromOldToNew(long stageId, int oldStatus, int newStatus) {
        try {
            // 0: running
            // 1: success
            BioPipelineStage update = new BioPipelineStage();
            update.setStatus(newStatus);
            return this.updateStageFromStatus(update, stageId, oldStatus);
        } catch (Exception e) {
            // represent error
            return -1;
        }

    }

    public int updateStageFromStatus(BioPipelineStage updateStage, long updateStageId, int status) {
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria().andStageIdEqualTo(updateStageId).andStatusEqualTo(status);
        try {
            return this.bioPipelineStageMapper.updateByExampleSelective(updateStage, bioPipelineStageExample);
        } catch (Exception e) {
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

    private String createStoreObjectName(BioPipelineStage pipelineStage, String name) {
        return bioStageUtil.createStoreObjectName(pipelineStage, name);
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


        BioPipelineStage updatedFirstStage = new BioPipelineStage();
        updatedFirstStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        int updateRes = this.updateStageFromStatus(updatedFirstStage, firstStage.getStageId(),
                PIPELINE_STAGE_STATUS_PENDING);
        if (updateRes < 0) {
            return new Result<Boolean>(Result.INTERNAL_FAIL, false, "流水线启动失败");
        }

        firstStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        this.pipelineStageTaskDispatcher.addTask(firstStage);
        return new Result<Boolean>(Result.SUCCESS, true, null);
    }

    @Async
    public void pipelineStageDone(StageRunResult stageRunResult) {
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        if (!stageRunResult.isSuccess()) {
            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            int res = this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(),
                    PIPELINE_STAGE_STATUS_RUNNING);
            return;
        }

        int stageType = bioPipelineStage.getStageType();
        if (stageType == PIPELINE_STAGE_QC) {
            handleQcStageDone(stageRunResult);
        } else if (stageType == PIPELINE_STAGE_ASSEMBLY) {
            handleAssemblyDone(stageRunResult);
        } else if (stageType == PIPELINE_STAGE_MAPPING) {
            handleMappingStageDone(stageRunResult);
        } else if (stageType == PIPELINE_STAGE_VARIANT_CALL) {
            handleVarientStageDone(stageRunResult);
        } else if (stageType == PIPELINE_STAGE_DEPTH_COVERAGE) {

        } else if (stageType == PIPELINE_STAGE_SNP_SINGLE) {

        } else if (stageType == PIPELINE_STAGE_CONSENSUS) {
            handleConsensusStageDone(stageRunResult);
        } else if (stageType == PIPELINE_STAGE_READ_LENGTH_DETECT) {
            handleReadLengthDetectDone(stageRunResult);
        }
    }

    // params[1]: object name
    private boolean batchUploadObjectsFromLocal(Map<String, String> params) {
        for (Map.Entry<String, String> objectConfig : params.entrySet()) {
            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(objectConfig.getValue());
                PutResult putResult = this.storageService.putObject(objectConfig.getKey(), fileInputStream);
                if (!putResult.success()) {
                    return false;
                }
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            } finally {
                if (fileInputStream != null) {
                    try {
                        fileInputStream.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

        }

        return true;

    }

    private boolean batchUploadObjectsFromLocal(String... objectNamesAndPaths) {
        HashMap<String, String> params = new HashMap<>();
        for (int i = 0; i < objectNamesAndPaths.length; i += 2) {
            params.put(objectNamesAndPaths[i], objectNamesAndPaths[i + 1]);
        }
        return this.batchUploadObjectsFromLocal(params);
    }

    private boolean deleteStageResultDir(String deleteDir) {
        try {
            FileUtils.deleteDirectory(new File(deleteDir));
            return true;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
    }

    private void handleUnsuccessUpload(BioPipelineStage bioPipelineStage, String deleteDir) {
        this.deleteStageResultDir(deleteDir);
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
        this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_RUNNING);
    }

    private int markStageFinish(BioPipelineStage bioPipelineStage, String outputUrl) {
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setOutputUrl(outputUrl);
        updateStage.setEndTime(new Date());

        return this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_RUNNING);
    }

    private void handleConsensusStageDone(StageRunResult stageRunResult) {
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        ConsensusStageOutput consensusStageOutput = (ConsensusStageOutput) stageRunResult.getStageOutput();

        String consesusOutputObjName = createStoreObjectName(bioPipelineStage, consensusStageOutput.getConsensusFa());

        boolean uploadRes = this.batchUploadObjectsFromLocal(consesusOutputObjName,
                consensusStageOutput.getConsensusFa());
        if (!uploadRes) {
            try {
                Files.delete(Path.of(consensusStageOutput.getConsensusFa()));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            this.updateStageFromStatus(updateStage, bioPipelineStage.getPipelineId(), PIPELINE_STAGE_STATUS_RUNNING);
            return;
        }

        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setEndTime(new Date());
        this.updateStageFromStatus(updateStage, bioPipelineStage.getPipelineId(), PIPELINE_STAGE_STATUS_RUNNING);

        try {
            Files.delete(Path.of(consensusStageOutput.getConsensusFa()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void handleVarientStageDone(StageRunResult stageRunResult) {

        VariantStageOutput variantStageOutput = (VariantStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        String vcfGzObjctName = String.format(
                stageOutputFormat,
                bioPipelineStage.getStageId(),
                bioPipelineStage.getStageName(),
                variantStageOutput.getVcfGz().substring(variantStageOutput.getVcfGz().lastIndexOf("/") + 1));

        String vcfTbiObjectName = String.format(
                stageOutputFormat,
                bioPipelineStage.getStageId(),
                bioPipelineStage.getStageName(),
                variantStageOutput.getVcfTbi().substring(variantStageOutput.getVcfTbi().lastIndexOf("/") + 1));

        boolean uploadSuccess = this.batchUploadObjectsFromLocal(
                vcfGzObjctName,
                variantStageOutput.getVcfGz(),
                vcfTbiObjectName,
                variantStageOutput.getVcfTbi());

        Path resultDirPath = Path.of(variantStageOutput.getVcfGz()).getParent();

        if (!uploadSuccess) {
            this.handleUnsuccessUpload(bioPipelineStage, resultDirPath.toString());
            return;
        }
        this.deleteStageResultDir(resultDirPath.toString());

        String outputUrl = String.format(
                "{\"%s\": \"%s\", \"%s\":\"%s\"}",
                PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ,
                vcfGzObjctName,
                PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI,
                vcfTbiObjectName);

        int updateRes = this.markStageFinish(bioPipelineStage, outputUrl);

        if (updateRes != 1) {
            return;
        }

        BioPipelineStageExample consensusStageExample = new BioPipelineStageExample();
        consensusStageExample.createCriteria()
                .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                .andStageTypeEqualTo(PIPELINE_STAGE_CONSENSUS);

        List<BioPipelineStage> consensusStageList = this.bioPipelineStageMapper.selectByExample(consensusStageExample);

        if (consensusStageList == null || consensusStageList.isEmpty()) {
            return;
        }

        BioPipelineStage consensusStage = consensusStageList.get(0);
        String inputUrl = String.format(
                "{}");

        BioPipelineStage updateConsensusStage = new BioPipelineStage();

        consensusStage.setInputUrl(inputUrl);
        consensusStage.setParameters(bioPipelineStage.getParameters());
        consensusStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

        updateConsensusStage.setInputUrl(inputUrl);
        updateConsensusStage.setParameters(bioPipelineStage.getParameters());
        updateConsensusStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

        int res = this.updateStageFromStatus(updateConsensusStage, bioPipelineStage.getStageId(),
                PIPELINE_STAGE_STATUS_PENDING);
        if (res == 1) {
            this.pipelineStageTaskDispatcher.addTask(updateConsensusStage);
        }

    }

    private void handleMappingStageDone(StageRunResult stageRunResult) {
        MappingStageOutput mappingStageOutput = (MappingStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        String bamObjectName = createStoreObjectName(bioPipelineStage,
                substractFileNameFromPath(mappingStageOutput.getBamPath()));

        String bamIndexObjectName = createStoreObjectName(bioPipelineStage,
                substractFileNameFromPath(mappingStageOutput.getBamIndexPath()));

        HashMap<String, String> outputStoreMap = new HashMap<>();
        outputStoreMap.put(bamObjectName, mappingStageOutput.getBamPath());
        outputStoreMap.put(bamIndexObjectName, mappingStageOutput.getBamIndexPath());

        boolean storeSuccss = this.batchUploadObjectsFromLocal(outputStoreMap);
        Path outputDirPath = Path.of(mappingStageOutput.getBamPath()).getParent();
        if (!storeSuccss) {

            this.handleUnsuccessUpload(bioPipelineStage, outputDirPath.toString());
            return;
        }

        BioPipelineStageExample nextStagesExample = new BioPipelineStageExample();
        nextStagesExample.createCriteria()
                .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                .andStageIndexGreaterThanOrEqualTo(bioPipelineStage.getStageIndex() + 1);

        List<BioPipelineStage> nextStages = this.bioPipelineStageMapper.selectByExample(nextStagesExample);

        if (nextStages == null || nextStages.isEmpty()) {
            return;
        }

        // 这里主要是用来看是否后续阶段。
        BioPipelineStage varientStage = nextStages.stream()
                .filter(stage -> stage.getStageType() == PIPELINE_STAGE_VARIANT_CALL).findFirst().orElse(null);

        if (varientStage != null) {
            BioPipelineStage updateVarientStage = new BioPipelineStage();
            HashMap<String, Object> inputMap = new HashMap<>();
            inputMap.put(PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY, bamIndexObjectName);
            String serializedInputUrl = null;
            try {
                serializedInputUrl = this.jsonMapper.writeValueAsString(inputMap);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            updateVarientStage.setInputUrl(serializedInputUrl);
            varientStage.setInputUrl(serializedInputUrl);

            updateVarientStage.setParameters(bioPipelineStage.getParameters());
            varientStage.setParameters(updateVarientStage.getParameters());
            updateVarientStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            varientStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

            int updateRes = this.updateStageFromStatus(updateVarientStage, bioPipelineStage.getStageId(),
                    PIPELINE_STAGE_STATUS_PENDING);
            if (updateRes != 1) {
                return;
            }
            this.pipelineStageTaskDispatcher.addTask(varientStage);
            return;
        }
        // todo: bacterial part. do it later
    }

    private void handleReadLengthDetectDone(StageRunResult stageRunResult) {

        boolean longRead = ((ReadLengthDetectStageOutput) stageRunResult.getStageOutput()).isLongRead();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setEndTime(new Date());
        updateStage.setOutputUrl(String.valueOf(longRead));
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);

        int updateRes = this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(),
                PIPELINE_STAGE_STATUS_RUNNING);
        if (updateRes != 1) {
            return;
        }
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria()
                .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                .andStageIndexGreaterThan(bioPipelineStage.getStageIndex());

        List<BioPipelineStage> nextStages = this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample);
        if (nextStages == null || nextStages.isEmpty()) {
            return;
        }

        List<BioPipelineStage> updateNextStages = new ArrayList<>();
        BioPipelineStage nextRunStage = null;

        for (BioPipelineStage nextStage : nextStages) {
            Map<String, Object> params = null;
            try {
                params = StringUtils.isNotBlank(nextStage.getParameters())
                        ? jsonMapper.readValue(nextStage.getParameters(), Map.class)
                        : null;
            } catch (JsonMappingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

            params = params == null ? new HashMap<>() : params;
            params.put(PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY, longRead);
            String serializedParams = null; 
            try {
                serializedParams = jsonMapper.writeValueAsString(params);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

            BioPipelineStage nextUpdateStage = new BioPipelineStage();

            nextUpdateStage.setParameters(serializedParams);
            if(bioPipelineStage.getStageIndex()+1 == nextStage.getStageIndex()){
                nextRunStage = nextStage;
                nextUpdateStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                nextRunStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                nextRunStage.setParameters(serializedParams);
            }

            updateNextStages.add(nextUpdateStage);

        }

        try {updateRes = rcTransactionTemplate.execute((status) -> {
            for(int i = 0;i<updateNextStages.size();i++){
                int res = this.updateStageFromStatus(updateNextStages.get(i),nextStages.get(i).getStageId(), PIPELINE_STAGE_STATUS_PENDING);
                if(res<1){
                    status.setRollbackOnly();
                    return 0;
                }
            }
            return 1;
        });}catch(TransactionException transactionException){
            //todo
            updateRes = -1;
        }

        if(updateRes >= 1){
            this.pipelineStageTaskDispatcher.addTask(nextRunStage);
        }

    }

    private void handleAssemblyDone(StageRunResult stageRunResult) {
        AssemblyStageOutput assemblyStageOutput = (AssemblyStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        String contigOutputKey = createStoreObjectName(bioPipelineStage,
                substractFileNameFromPath(assemblyStageOutput.getContigPath()));

        String scaffoldOuputKey = createStoreObjectName(bioPipelineStage,
                substractFileNameFromPath(assemblyStageOutput.getScaffoldPath()));

        HashMap<String, String> outputMap = new HashMap<>();

        boolean hasScaffold = assemblyStageOutput.getScaffoldPath() != null;

        Path resultDirPath = Path.of(assemblyStageOutput.getContigPath()).getParent();

        outputMap.put(contigOutputKey, assemblyStageOutput.getContigPath());
        if (hasScaffold) {
            outputMap.put(scaffoldOuputKey, assemblyStageOutput.getScaffoldPath());
        }
        boolean success = this.batchUploadObjectsFromLocal(outputMap);

        if (!success) {
            this.handleUnsuccessUpload(bioPipelineStage, resultDirPath.toString());
            return;
        }
        HashMap<String, String> outputPathMap = new HashMap<>();
        outputPathMap.put(PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY, contigOutputKey);
        outputPathMap.put(PIPELINE_STAGE_ASSEMBLY_OUTPUT_SCAFFOLDS_KEY, hasScaffold ? scaffoldOuputKey : null);
        String serializedOutputPath = null;
        try {
            serializedOutputPath = this.jsonMapper.writeValueAsString(outputPathMap);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setOutputUrl(serializedOutputPath);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setEndTime(new Date());

        int updateRes = this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(),
                PIPELINE_STAGE_STATUS_RUNNING);
        if (updateRes != 1) {
            return;
        }

        BioPipelineStageExample nextStagesExample = new BioPipelineStageExample();
        nextStagesExample.createCriteria().andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                .andStageIndexGreaterThanOrEqualTo(bioPipelineStage.getStageIndex() + 1);

        List<BioPipelineStage> nextStages = this.bioPipelineStageMapper.selectByExample(nextStagesExample);
        if (nextStages == null || nextStages.isEmpty()) {
            return;
        }

        BioPipelineStage mappingStage = nextStages.stream()
                .filter(stage -> stage.getStageType() == PIPELINE_STAGE_MAPPING).findFirst().orElse(null);
        if (mappingStage != null) {
            BioPipelineStage updateMappingStage = new BioPipelineStage();
            updateMappingStage.setInputUrl(bioPipelineStage.getInputUrl());
            Map<String, Object> mappingStageParamMap = null;
            try {
                mappingStageParamMap = this.jsonMapper.readValue(mappingStage.getParameters(), Map.class);
            } catch (JsonMappingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

            HashMap<String, Object> refseqConfig = new HashMap<>();
            refseqConfig.put(PIPLEINE_STAGE_PARAMETERS_REFSEQ_KEY, contigOutputKey);
            refseqConfig.put(PIPELINE_STAGE_PARAMETERS_REFSEQ_IS_INNER, false);

            mappingStageParamMap.put(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
            String serializedMappingStageParameters = null;
            try {
                serializedMappingStageParameters = this.jsonMapper.writeValueAsString(mappingStageParamMap);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            updateMappingStage.setParameters(serializedMappingStageParameters);
            this.updateStageFromStatus(updateMappingStage, mappingStage.getStageId(), PIPELINE_STAGE_STATUS_PENDING);
            return;
        }
        // todo: bacteria part. do it later
    }

    private String substractFileNameFromPath(String path) {
        return Path.of(path).getFileName().toString();
    }

    private void handleQcStageDone(StageRunResult stageRunResult) {

        QCStageOutput qcStageOutput = (QCStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        String qcR1Path = qcStageOutput.getR1Path();
        String qcR2Path = qcStageOutput.getR2Path();
        boolean hasR2 = qcR2Path != null;

        String qcJsonPath = qcStageOutput.getJsonPath();
        String qcHTMLPath = qcStageOutput.getHtmlPath();

        String r1OutputPath = createStoreObjectName(bioPipelineStage, substractFileNameFromPath(qcR1Path));
        String r2OutputPath = hasR2 ? createStoreObjectName(bioPipelineStage, substractFileNameFromPath(qcR2Path))
                : null;
        String jsonOutputPath = createStoreObjectName(bioPipelineStage, substractFileNameFromPath(qcJsonPath));
        String htmlOutputPath = createStoreObjectName(bioPipelineStage, substractFileNameFromPath(qcHTMLPath));

        Path resultDirPath = Path.of(qcR1Path).getParent();
        Map<String, String> params = new HashMap<>();
        params.put(qcR1Path, r1OutputPath);
        if (hasR2) {
            params.put(qcR2Path, r2OutputPath);
        }
        params.put(qcJsonPath, jsonOutputPath);
        params.put(qcHTMLPath, htmlOutputPath);

        boolean uploadSuccess = this.batchUploadObjectsFromLocal(params);
        if (!uploadSuccess) {
            this.handleUnsuccessUpload(bioPipelineStage, resultDirPath.toString());
            return;
        }

        Map<String, String> outputPathMap = new HashMap<>();
        outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_R1, r1OutputPath);
        outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_R2, r2OutputPath);
        outputPathMap.put(PIPELINE_STAGE_QC_OUTPUI_JSON, jsonOutputPath);
        outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_HTML, htmlOutputPath);
        try {
            String outputPathMapJson = this.jsonMapper.writeValueAsString(outputPathMap);
            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
            updateStage.setOutputUrl(outputPathMapJson);
            updateStage.setEndTime(new Date());
            int updateRes = this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(),
                    PIPELINE_STAGE_STATUS_RUNNING);
            if (updateRes != 1) {
                return;
            }

            BioPipelineStageExample nextStageExample = new BioPipelineStageExample();
            nextStageExample.createCriteria().andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                    .andStageIndexEqualTo(bioPipelineStage.getStageIndex() + 1);
            List<BioPipelineStage> nextStages = this.bioPipelineStageMapper.selectByExample(nextStageExample);
            if (nextStages == null || nextStages.isEmpty()) {
                return;
            }

            BioPipelineStage nextStage = nextStages.get(0);
            BioPipelineStage updateNextStage = new BioPipelineStage();
            updateNextStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            nextStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            HashMap<String, String> inputMap = new HashMap<>();
            inputMap.put(PIPELINE_STAGE_INPUT_READ1_KEY, r1OutputPath);
            inputMap.put(PIPELINE_STAGE_INPUT_READ2_KEY, r2OutputPath);
            String nextStageInput = this.jsonMapper.writeValueAsString(inputMap);
            updateNextStage.setInputUrl(nextStageInput);
            nextStage.setInputUrl(nextStageInput);

            updateRes = this.updateStageFromStatus(updateNextStage, nextStage.getStageId(),
                    PIPELINE_STAGE_STATUS_PENDING);
            if (updateRes != 1) {
                return;
            }

            this.pipelineStageTaskDispatcher.addTask(nextStage);

        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        this.deleteStageResultDir(resultDirPath.toString());

    }

}
