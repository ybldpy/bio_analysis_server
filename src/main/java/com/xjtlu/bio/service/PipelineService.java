package com.xjtlu.bio.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.javassist.tools.framedump;
import org.bouncycastle.jcajce.provider.asymmetric.ec.GMSignatureSpi.sha256WithSM2;
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
import com.xjtlu.bio.taskrunner.PipelineStageTaskDispatcher;
import com.xjtlu.bio.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.QCStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.VariantStageOutput;
import com.xjtlu.bio.utils.ParameterUtil;


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


    public static List<BioPipelineStage> buildBacteriaStages(){
        //todo
        return null;
    }



    public static List<BioPipelineStage> buildVirusStages(long pid, int pipelineType, BioSample bioSample, Map<String,Object> pipelineStageParams) throws JsonProcessingException{

        ArrayList<BioPipelineStage> stages = new ArrayList<>(8);
        String qcInputRead1 = bioSample.getRead1Url();
        String qcInputRead2 = bioSample.getRead2Url();

    
        String refSeqKey = "refSeq";
        String refSeq = ParameterUtil.getStrFromMap(refSeqKey, pipelineStageParams);



        int index = 0;
        BioPipelineStage qc = new BioPipelineStage();
        

        Map<String,String> qcInputMap = new HashMap<>();


        qcInputMap.put("r1", qcInputRead1);
        qcInputMap.put("r2", qcInputRead2);
        String qcInputMapStr = null;
        qc.setStageIndex(index);
        qc.setStageName("质控(QC)");
        qc.setStageType(PipelineService.PIPELINE_STAGE_QC);
        qc.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        qc.setPipelineId(pid);
        qc.setInputUrl(qcInputMapStr);
        stages.add(qc);

        index++;

        BioPipelineStage assembly = new BioPipelineStage();

        Map<String,Object> assemblyParams = substractStageParams("assembly", pipelineStageParams);
        String assemlyParamsStr = assemblyParams == null? null: objectMapper.writeValueAsString(assemblyParams);
        assembly.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        assembly.setPipelineId(pid);
        assembly.setStageIndex(index);
        assembly.setParameters(assemlyParamsStr);

    
        if (StringUtils.isBlank(refSeq)) {
            assembly.setStageType(PipelineService.PIPELINE_STAGE_ASSEMBLY);
            assembly.setStageName("组装");
        } else {
            assembly.setStageType(PipelineService.PIPELINE_STAGE_MAPPING);
            assemblyParams.put("refSeq", refSeq);
            assembly.setParameters(objectMapper.writeValueAsString(assemlyParamsStr));
            assembly.setStageName("参考");
        }

        stages.add(assembly);
        index++;

        if (refSeq == null && pipelineType != PipelineService.PIPELINE_VIRUS_COVID) {
            return stages;
        }



        Map<String,Object> varientStageParams = substractStageParams("varient", pipelineStageParams);
        varientStageParams.put("refSeq", refSeq);
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
        Map<String,Object> consesusParams = substractStageParams("consensus", pipelineStageParams);
        consesusParams.put("refSeq", refSeq);
        consensus.setStageName("生成一致性序列");
        consensus.setPipelineId(pid);
        consensus.setStageIndex(index);
        consensus.setParameters(objectMapper.writeValueAsString(consesusParams));
        consensus.setStageType(PipelineService.PIPELINE_STAGE_VARIANT_CALL);
        consensus.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(consensus);

        if (pipelineType != PipelineService.PIPELINE_VIRUS_COVID) {
            return stages;
        }


        Map<String,Object> snpParams = substractStageParams("snp", pipelineStageParams);
        snpParams.put("refSeq", refSeq);
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
        Map<String,Object> depthParams = substractStageParams("depth", pipelineStageParams);
        depthParams.put("refSeq",refSeq);
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
    private SampleService sampleService;

    @Resource
    private MinioService minioService;

    @Resource
    private PipelineStageTaskDispatcher pipelineStageTaskDispatcher;

    @Value("${analysisPipeline.stage.baseOutputPath}")
    private String stagesOutputBasePath;

    private Set<Integer> bioAnalysisPipelineLockSet = ConcurrentHashMap.newKeySet();

    private ObjectMapper jsonMapper = new ObjectMapper();

    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;

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



    public static final String stageOutputFormat = "stageOutput/%d/%d/%s";

    private boolean isLegalPipelineType(int pipelineType) {
        return pipelineType == PIPELINE_VIRUS || pipelineType == PIPELINE_VIRUS_COVID
                || pipelineType == PIPELINE_VIRUS_BACKTERIA;
    }

    
    // 0: success
    //-1: error
    @Transactional(rollbackFor = Exception.class)
    public int updateStageFromOldToNew(long stageId, int oldStatus, int newStatus){
        try{
            // 0: running
            // 1: success
            return this.bioAnalysisStageMapperExtension.updateStatusTo(stageId, oldStatus, newStatus);
        }catch(Exception e){
            //represent error
            return -1;
        }

    }

    public int updateStageFromStatus(BioPipelineStage bioPipelineStage, long updateStageId,int status){
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria().andStageIdEqualTo(updateStageId).andStatusEqualTo(status);
        try{
            return this.bioPipelineStageMapper.updateByExampleSelective(bioPipelineStage, bioPipelineStageExample);
        }catch(Exception e){
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
            Map<String, Object> pipelineStageParams) {

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
            Map<String, Object> pipelineParams) {

        if (bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS || bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS_COVID) {
            try {
                List<BioPipelineStage> stages = BioPipelineStagesBuilder.buildVirusStages(bioAnalysisPipeline.getPipelineId(), bioAnalysisPipeline.getPipelineType(), bioSample, pipelineParams);
                return stages;
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                return null;
            }
        }else {
            return null;
        }

    }

    @Transactional(rollbackFor = Exception.class)
    public Result<Boolean> pipelineStart(long runSampleId) {


        
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria().andPipelineIdEqualTo().andStageIndexEqualTo(0);
        List<BioPipelineStage> stages = this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample);
        if (stages == null || stages.isEmpty()) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到流水线");
        }
        BioPipelineStage firstStage = stages.get(0);
        BioPipelineStage updatedFirstStage = new BioPipelineStage();
        updatedFirstStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        updatedFirstStage.setStageId(firstStage.getStageId());
        int updateRes = this.bioPipelineStageMapper.updateByPrimaryKeySelective(updatedFirstStage);
        if(updateRes < 1){
            return new Result<Boolean>(Result.INTERNAL_FAIL, false, "流水线启动失败");
        }


        boolean addRes = this.pipelineStageTaskDispatcher.addTask(firstStage);
        

        return new Result<Boolean>(Result.SUCCESS, true, null);
    }



    @Async
    public void pipelineStageDone(StageRunResult stageRunResult) {
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        if (!stageRunResult.isSuccess()) {
            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            int res = this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_RUNNING);
            // assume success here;
            return;
        }


        int stageType = bioPipelineStage.getStageType();
        if (stageType == PIPELINE_STAGE_QC) {
            handleQcStageDone(stageRunResult);
        }else if (stageType == PIPELINE_STAGE_ASSEMBLY) {
            handleAssemblyDone(stageRunResult);
        }else if (stageType == PIPELINE_STAGE_MAPPING) {
            
        }else if (stageType == PIPELINE_STAGE_VARIANT_CALL) {
            
        }else if (stageType == PIPELINE_STAGE_DEPTH_COVERAGE) {
            
        }else if (stageType == PIPELINE_STAGE_SNP_SINGLE) {
            
        }
    }





    
    // params[1]: object name
    private boolean batchUploadObjectsFromLocal(Map<String, String> params) {
        return false;
    }



    private boolean batchUploadObjectsFromLocal(String... objectNamesAndPaths){
        HashMap<String,String> params = new HashMap<>();
        for(int i = 0;i<objectNamesAndPaths.length;i+=2){
            params.put(objectNamesAndPaths[i], objectNamesAndPaths[i+1]);
        }
        return this.batchUploadObjectsFromLocal(params);
    }


    private void handleUnsuccessUpload(BioPipelineStage bioPipelineStage, String... toDeleteFiles) throws IOException{

        BioPipelineStage updateStage = new BioPipelineStage();
        for(String path: toDeleteFiles){
            Files.delete(Path.of(path));
        }
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
        this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_RUNNING);

    }


    private int markStageFinish(BioPipelineStage bioPipelineStage, String outputUrl){
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setOutputUrl(outputUrl);
        updateStage.setEndTime(new Date());

        return this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_RUNNING);
    }


    private void handleVarientStageDone(StageRunResult stageRunResult){

        VariantStageOutput variantStageOutput = (VariantStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        String vcfGzObjctName = String.format(
            stageOutputFormat,
            bioPipelineStage.getStageId(),
            bioPipelineStage.getStageName(),
            variantStageOutput.getVcfGz().substring(variantStageOutput.getVcfGz().lastIndexOf("/")+1)
        );

        String vcfTbiObjectName = String.format(
            stageOutputFormat,
            bioPipelineStage.getStageId(),
            bioPipelineStage.getStageName(),
            variantStageOutput.getVcfTbi().substring(variantStageOutput.getVcfTbi().lastIndexOf("/")+1)
        );

        boolean uploadSuccess = this.batchUploadObjectsFromLocal(
            vcfGzObjctName,
            variantStageOutput.getVcfGz(),
            vcfTbiObjectName,
            variantStageOutput.getVcfTbi()
        );

        if(!uploadSuccess){
            try {
                this.handleUnsuccessUpload(bioPipelineStage,variantStageOutput.getVcfGz(), variantStageOutput.getVcfTbi());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return;
        }


        String outputUrl = String.format(
            "{\"%s\": \"%s\", \"%s\":\"%s\"}",
            PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ,
            vcfGzObjctName,
            PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI,
            vcfTbiObjectName
        );

        int updateRes = this.markStageFinish(bioPipelineStage, outputUrl);

        if (updateRes!=1) {
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
            "{}"
        );

        BioPipelineStage updateConsensusStage = new BioPipelineStage();

        consensusStage.setInputUrl(inputUrl);
        consensusStage.setParameters(bioPipelineStage.getParameters());
        consensusStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

        updateConsensusStage.setInputUrl(inputUrl);
        updateConsensusStage.setParameters(bioPipelineStage.getParameters());
        updateConsensusStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

        int res = this.updateStageFromStatus(updateConsensusStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_PENDING);
        if(res==1){
            this.pipelineStageTaskDispatcher.addTask(updateConsensusStage);
        }

    }



    private void handleMappingStageDone(StageRunResult stageRunResult){
        MappingStageOutput mappingStageOutput = (MappingStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        String bamObjectName = String.format(
            stageOutputFormat,
            bioPipelineStage.getStageId(),
            bioPipelineStage.getStageName(),
            mappingStageOutput.getBamPath().substring(mappingStageOutput.getBamPath().lastIndexOf("/")+1)
        );

        String bamIndexObjectName = String.format(
            stageOutputFormat,
            bioPipelineStage.getStageId(),
            bioPipelineStage.getStageName(),
            mappingStageOutput.getBamIndexPath().substring(mappingStageOutput.getBamIndexPath().lastIndexOf("/")+1)
        );

        HashMap<String,String> outputStoreMap = new HashMap<>();
        outputStoreMap.put(bamObjectName, mappingStageOutput.getBamPath());
        outputStoreMap.put(bamIndexObjectName, mappingStageOutput.getBamIndexPath());

        boolean storeSuccss = this.batchUploadObjectsFromLocal(outputStoreMap);
        if(!storeSuccss){
            try {
                this.handleUnsuccessUpload(bioPipelineStage, mappingStageOutput.getBamPath(), mappingStageOutput.getBamIndexPath());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return;
        }

        BioPipelineStageExample nextStagesExample = new BioPipelineStageExample();
        nextStagesExample.createCriteria()
        .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
        .andStageIndexGreaterThanOrEqualTo(bioPipelineStage.getStageIndex()+1);

        List<BioPipelineStage> nextStages = this.bioPipelineStageMapper.selectByExample(nextStagesExample);
        
        if(nextStages==null || nextStages.isEmpty()){
            return;
        }


        //这里主要是用来看是否后续阶段。
        BioPipelineStage varientStage = nextStages.stream().filter(stage->stage.getStageType() == PIPELINE_STAGE_VARIANT_CALL).findFirst().orElse(null);

        if (varientStage!=null) {
            BioPipelineStage updateVarientStage = new BioPipelineStage();
            HashMap<String,Object> inputMap = new HashMap<>();
            inputMap.put(PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY, bamIndexObjectName);
            String serializedInputUrl = this.jsonMapper.writeValueAsString(inputMap);
            updateVarientStage.setInputUrl(serializedInputUrl);
            varientStage.setInputUrl(serializedInputUrl);
            
            updateVarientStage.setParameters(bioPipelineStage.getParameters());
            varientStage.setParameters(updateVarientStage.getParameters());
            updateVarientStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            varientStage.setStatus(updateVarientStage.getStatus());

            int updateRes = this.updateStageFromStatus(updateVarientStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_PENDING);
            if(updateRes!=1){
                return;
            }
            this.pipelineStageTaskDispatcher.addTask(varientStage);
            return;
        }

        //bacterial part. do it later

    }



    private void handleAssemblyDone(StageRunResult stageRunResult){
        AssemblyStageOutput assemblyStageOutput = (AssemblyStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        String format = "stageOutput/%d/%s/%s";
        String contigOutputKey = String.format(
            format,
            bioPipelineStage.getStageId(),
            bioPipelineStage.getStageName(),
            "contigs.fasta"
        );
        
        String scaffoldOuputKey = String.format(
            format,
            bioPipelineStage.getStageId(),
            bioPipelineStage.getStageName(),
            "scaffold.fasta"
        );

        HashMap<String,String> outputMap = new HashMap<>();

        boolean hasScaffold = assemblyStageOutput.getScaffoldPath()!=null;

        outputMap.put(contigOutputKey, assemblyStageOutput.getContigPath());
        if (hasScaffold) {
            outputMap.put(scaffoldOuputKey, assemblyStageOutput.getScaffoldPath());
        }
        boolean success = this.batchUploadObjectsFromLocal(outputMap);

        if(!success){
            try {
                this.handleUnsuccessUpload(bioPipelineStage, assemblyStageOutput.getContigPath(), assemblyStageOutput.getScaffoldPath());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return;
        }


        HashMap<String,String> outputPathMap = new HashMap<>();
        outputPathMap.put(PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY, contigOutputKey);
        outputPathMap.put(PIPELINE_STAGE_ASSEMBLY_OUTPUT_SCAFFOLDS_KEY, hasScaffold?scaffoldOuputKey:null);

        String serializedOutputPath = this.jsonMapper.writeValueAsString(outputPathMap);


        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setOutputUrl(serializedOutputPath);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setEndTime(new Date());

        int updateRes = this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_RUNNING);
        if (updateRes!=1) {
            return;
        }

        BioPipelineStageExample nextStagesExample = new BioPipelineStageExample();
        nextStagesExample.createCriteria().
        andPipelineIdEqualTo(bioPipelineStage.getPipelineId()).
        andStageIndexGreaterThanOrEqualTo(bioPipelineStage.getStageIndex()+1);



        List<BioPipelineStage> nextStages = this.bioPipelineStageMapper.selectByExample(nextStagesExample);
        if(nextStages==null || nextStages.isEmpty()){
            return;
        }

        BioPipelineStage mappingStage = nextStages.stream().filter(stage->stage.getStageType() == PIPELINE_STAGE_MAPPING).findFirst().orElse(null);
        if (mappingStage != null) {
            BioPipelineStage updateMappingStage = new BioPipelineStage();
            updateMappingStage.setInputUrl(bioPipelineStage.getInputUrl());
            
            HashMap<String,String> mappingStageParamMap = new HashMap<>();
            mappingStageParamMap.put(PIPLEINE_STAGE_PARAMETERS_REFSEQ_KEY, contigOutputKey);
            String serializedMappingStageParameters = this.jsonMapper.writeValueAsString(mappingStageParamMap);
            updateMappingStage.setParameters(serializedMappingStageParameters);
            this.updateStageFromStatus(updateMappingStage, mappingStage.getStageId(), PIPELINE_STAGE_STATUS_PENDING);
            return;
        }

        //todo: bacteria part. do it later




    }

    private void handleQcStageDone(StageRunResult stageRunResult) {

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
                BioPipelineStage updateStage = new BioPipelineStage();
                updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
                updateStage.setOutputUrl(outputPathMapJson);
                updateStage.setEndTime(new Date());
                int updateRes = this.updateStageFromStatus(updateStage, bioPipelineStage.getStageId(), PIPELINE_STAGE_STATUS_RUNNING);
                if(updateRes!=1){return;}

                BioPipelineStageExample nextStageExample = new BioPipelineStageExample();
                nextStageExample.createCriteria().andPipelineIdEqualTo(bioPipelineStage.getPipelineId()).andStageIndexEqualTo(bioPipelineStage.getStageIndex()+1);
                List<BioPipelineStage> nextStages = this.bioPipelineStageMapper.selectByExample(nextStageExample);
                if(nextStages == null || nextStages.isEmpty()){
                    return;
                }


                
                BioPipelineStage nextStage = nextStages.get(0);
                BioPipelineStage updateNextStage = new BioPipelineStage();
                updateNextStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                nextStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                if(nextStage.getStageType() == PIPELINE_STAGE_ASSEMBLY){
                    HashMap<String, String> inputMap = new HashMap<>();
                    inputMap.put("r1", r1OutputPath);
                    inputMap.put("r2", r2OutputPath);
                    String assemblyInput = this.jsonMapper.writeValueAsString(inputMap);
                    updateNextStage.setInputUrl(assemblyInput);
                    nextStage.setInputUrl(assemblyInput);  
                }

                updateRes = this.updateStageFromStatus(updateNextStage, nextStage.getStageId(), PIPELINE_STAGE_STATUS_PENDING);  
                if(updateRes!=1){
                    return;
                }

                this.pipelineStageTaskDispatcher.addTask(nextStage);

            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

}
