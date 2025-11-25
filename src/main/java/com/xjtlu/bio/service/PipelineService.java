package com.xjtlu.bio.service;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.javassist.tools.framedump;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.entity.BioSampleExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioSampleMapper;
import com.xjtlu.bio.utils.ParameterUtil;

import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.DeleteError;
import jakarta.annotation.Resource;

private class BioPipelineStagesBuilder {

    public static List<BioPipelineStage> buildVirusStages(long pid, BioSample bioSample, Map<String,Map<String,Object>> pipelineStageParam) throws JsonProcessingException{

        ArrayList<BioPipelineStage> stages = new ArrayList<>(8);
        String qcInputRead1 = bioSample.getRead1Url();
        String qcInputRead2 = bioSample.getRead2Url();

        Map<String, Object> stageParams = new HashMap<>();
        String refSeqKey = "refSeq";
        String refSeq = ParameterUtil.getStrFromMap(refSeqKey, pipelineStageParams);
        stageParams.put(refSeqKey, refSeq);

        int index = 0;
        BioPipelineStage qc = new BioPipelineStage();
        

        Map<String,String> qcInputMap = new HashMap<>();

        Map<String, Object> qcStageParams = pipelineStageParams.get("qc");

        qcInputMap.put("r1", qcInputRead1);
        qcInputMap.put("r2", qcInputRead2);
        String qcInputMapStr = this.jsonMapper.writeValueAsString(qcInputMap);
        qc.setStageIndex(index);
        qc.setStageName("质控(QC)");
        qc.setStageType(PIPELINE_STAGE_QC);
        qc.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        qc.setPipelineId(pid);
        qc.setInputUrl(qcInputMapStr);
        stages.add(qc);

        index++;

        BioPipelineStage assembly = new BioPipelineStage();

        Map<String,Object> assemblyParams = pipelineStageParams.get("assembly");
        String assemlyParamsStr = assemblyParams == null? null: this.jsonMapper.writeValueAsString(assemblyParams);
        assembly.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        assembly.setPipelineId(pid);
        assembly.setStageIndex(index);
        assembly.setParameters(assemlyParamsStr);

        if(assemblyParams!=null)

        String jsonedParams = jsonMapper.writeValueAsString(stageParams);
        if (StringUtils.isBlank(refSeq)) {
            assembly.setStageType(PIPELINE_STAGE_ASSEMBLY);
            assembly.setStageName("组装");
        } else {
            assembly.setStageType(PIPELINE_STAGE_MAPPING);
            assembly.setParameters(jsonedParams);
            assembly.setStageName("参考");
        }

        stages.add(assembly);
        index++;

        if (refSeq == null && type != PIPELINE_VIRUS_COVID) {
            return stages;
        }

        BioPipelineStage varient = new BioPipelineStage();
        varient.setStageName("变异检测");
        varient.setPipelineId(pid);
        varient.setStageIndex(index);
        varient.setParameters(jsonedParams);
        varient.setStageType(PIPELINE_STAGE_VARIANT_CALL);
        varient.setStatus(PIPELINE_STAGE_STATUS_PENDING);

        stages.add(varient);

        index++;

        BioPipelineStage consensus = new BioPipelineStage();
        consensus.setStageName("生成一致性序列");
        consensus.setPipelineId(pid);
        consensus.setStageIndex(index);
        consensus.setParameters(jsonedParams);
        consensus.setStageType(PIPELINE_STAGE_VARIANT_CALL);
        consensus.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stages.add(consensus);

        if (type != PIPELINE_VIRUS_COVID) {
            return stages;
        }

        BioPipelineStage snp = new BioPipelineStage();
        snp.setStageName("SNP注释");
        snp.setPipelineId(pid);
        snp.setStageIndex(index);
        snp.setParameters(jsonedParams);
        snp.setStageType(PIPELINE_STAGE_SNP_CORE);
        snp.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);

        index++;

        BioPipelineStage depthConverage = new BioPipelineStage();
        depthConverage.setStageName("深度分布图");
        depthConverage.setPipelineId(pid);
        depthConverage.setStageIndex(index);
        depthConverage.setParameters(jsonedParams);
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
    private SampleService sampleService;

    @Resource
    private MinioService minioService;

    @Value("${analysisPipeline.stage.baseOutputPath}")
    private String stagesOutputBasePath;

    private Set<Integer> bioAnalysisPipelineLockSet = ConcurrentHashMap.newKeySet();

    private ObjectMapper jsonMapper = new ObjectMapper();

    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;

    public static final int PIPELINE_STAGE_STATUS_PENDING = 0;
    public static final int PIPELINE_STAGE_STATUS_RUNNING = 1;
    public static final int PIPELINE_STAGE_STATUS_FAIL = 2;
    public static final int PIPELINE_STAGE_STATUS_FINISHED = 3;

    public static final String PIPELINE_REFSEQ_ACCESSION_KEY = "refSeq";

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

    private boolean isLegalPipelineType(int pipelineType) {
        return pipelineType == PIPELINE_VIRUS || pipelineType == PIPELINE_VIRUS_COVID
                || pipelineType == PIPELINE_VIRUS_BACKTERIA;
    }

    private Result<BioSample> createVirusPipeline(int type, boolean sampleIsPair, String sampleName,
            long sampleProjectId, int sampleType, Map<String, Object> pipelineStageParams) {

        BioAnalysisPipeline bAnalysisPipeline = new BioAnalysisPipeline();
        bAnalysisPipeline.setPipelineType(type);
        int res = this.bioAnalysisPipelineMapper.insertSelective(bAnalysisPipeline);
        if (res <= 0) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<BioSample>(Result.INTERNAL_FAIL, null, "内部错误");
        }

        Result<BioSample> createSampleResult = sampleService.createSample(sampleIsPair, sampleName, sampleProjectId,
                bAnalysisPipeline.getPipelineId(), sampleType);

        int status = createSampleResult.getStatus();
        if (status != Result.SUCCESS) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return createSampleResult;
        }

        long sid = createSampleResult.getData().getSid();

        List<BioPipelineStage> stages = null;
        try {
            stages = createVirusPipelineStages(bAnalysisPipeline.getPipelineId(),
                    createSampleResult.getData(), type, pipelineStageParams);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<BioSample>(Result.INTERNAL_FAIL, null, "内部错误");
        }

        res = bioAnalysisStageMapperExtension.batchInsert(stages);
        if (res != stages.size()) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<BioSample>(Result.INTERNAL_FAIL, null, "内部错误");
        }

        return createSampleResult;
    }

    private List<BioPipelineStage> createVirusPipelineStages(long pid, BioSample bioSample, int type,
            Map<String, Map<String, Object>> pipelineStageParams) throws JsonProcessingException {

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
            Map<String, Object> pipelineStageParams) {

        BioAnalysisPipeline bioAnalysisPipeline = new BioAnalysisPipeline();
        bioAnalysisPipeline.setPipelineType(mapSampleTypeToPipelineType(bioSample.getSampleType()));
        bioAnalysisPipeline.setSampleId(bioSample.getSid());
        int insertRes = this.bioAnalysisPipelineMapper.insert(bioAnalysisPipeline);
        if (insertRes < 1) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        List<BioPipelineStage> stages = this.buildPipelineStages(bioSample, bioAnalysisPipeline.getPipelineId());
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
            Map<String, Object> pipelineParams) {

        if (bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS) {
            try {
                List<BioPipelineStage> stages = this.createVirusPipelineStages(bioAnalysisPipeline.getPipelineId(),
                        bioSample, bioAnalysisPipeline.getPipelineType(), pipelineParams);
                return stages;
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                return null;
            }
        }

    }

    @Transactional
    public Result<Boolean> pipelineStart(long pid) {
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria().andPipelineIdEqualTo(pid);
        List<BioPipelineStage> stages = this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample);
        if (stages == null || stages.isEmpty()) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到流水线");
        }
    }

    @Async
    @Transactional
    public void pipelineStageDone(StageRunResult stageRunResult) {
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        if (!stageRunResult.isSuccess()) {
            bioPipelineStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
            int res = this.bioPipelineStageMapper.updateByPrimaryKey(bioPipelineStage);
            // assume success here;
            return;
        }

        if (bioPipelineStage.getStageType() == PIPELINE_STAGE_QC) {
            handleQcStage(stageRunResult);
        }
    }

    private static void batchDeleteFileFromTmpPath(List<File> deleteFiles) {
        for (File f : deleteFiles) {
            if (f != null) {
                f.delete();
            }
        }
    }

    private List<DeleteError> batchDeleteFromStorage(List<String> deleteFiles) {

        List<DeleteError> deleteErrors = null;

        try {
            deleteErrors = this.minioService.batchDelete(deleteFiles);
        } catch (InvalidKeyException | ErrorResponseException | IllegalArgumentException | InsufficientDataException
                | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException
                | XmlParserException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            return deleteErrors;
        }
    }

    private void pushToScheduledDeleteService(List<String> deleteObjects) {
        // todo
    }

    // params[0]: local path
    // params[1]: object name
    private boolean batchUploadObjectsFromLocal(Map<String, String> params) {

        for (Map.Entry<String, String> entry : params.entrySet()) {

            String localPath = entry.getKey();
            String objectName = entry.getValue();
            try {
                minioService.uploadObject(objectName, localPath);
            } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                    | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                    | IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }

        return true;

    }

    private void handleQcStage(StageRunResult stageRunResult) {

        Map<String, String> outputPathMap = stageRunResult.getOutputPath();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        String qcR1Path = outputPathMap.get(PIPELINE_STAGE_QC_OUTPUT_R1);
        String qcR2Path = outputPathMap.get(PIPELINE_STAGE_QC_OUTPUT_R2);
        boolean hasR2 = qcR2Path != null;

        String qcJsonPath = outputPathMap.get(PIPELINE_STAGE_QC_OUTPUI_JSON);
        String qcHTMLPath = outputPathMap.get(PIPELINE_STAGE_QC_OUTPUT_HTML);

        String format = "%s/%d/%d/%s";
        String r1OutputPath = String.format(format, this.stagesOutputBasePath, bioPipelineStage.getStageId(),
                bioPipelineStage.getStageIndex(), qcR1Path.substring(qcR1Path.lastIndexOf("/") + 1));
        String r2OutputPath = !hasR2 ? null
                : String.format(format, this.stagesOutputBasePath, bioPipelineStage.getStageId(),
                        bioPipelineStage.getStageIndex(), qcR2Path.substring(qcR2Path.lastIndexOf("/") + 1));
        String jsonOutputPath = String.format(format, this.stagesOutputBasePath, bioPipelineStage.getStageId(),
                bioPipelineStage.getStageIndex(), "qc.json");
        String htmlOutputPath = String.format(format, this.stagesOutputBasePath, bioPipelineStage.getStageId(),
                bioPipelineStage.getStageIndex(), "qc.html");

        Map<String, String> params = new HashMap<>();
        params.put(qcR1Path, r1OutputPath);
        if (hasR2) {
            params.put(qcR2Path, r2OutputPath);
        }
        params.put(qcJsonPath, jsonOutputPath);
        params.put(qcHTMLPath, htmlOutputPath);

        boolean uploadSuccess = this.batchUploadObjectsFromLocal(params);
        if (!uploadSuccess) {
            // todo
        } else {
            outputPathMap.clear();
            outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_R1, r1OutputPath);
            outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_R2, r2OutputPath);
            outputPathMap.put(PIPELINE_STAGE_QC_OUTPUI_JSON, jsonOutputPath);
            outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_HTML, htmlOutputPath);
            try {
                String outputPathMapJson = this.jsonMapper.writeValueAsString(outputPathMap);
                bioPipelineStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
                bioPipelineStage.setOutputUrl(outputPathMapJson);
                this.bioPipelineStageMapper.updateByPrimaryKey(bioPipelineStage);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

}
