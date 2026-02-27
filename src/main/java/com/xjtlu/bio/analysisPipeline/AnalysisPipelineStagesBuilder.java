package com.xjtlu.bio.analysisPipeline;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.requestParameters.CreateSampleRequest.PipelineStageParameters;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.utils.JsonUtil;

public class AnalysisPipelineStagesBuilder {

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

    public static List<BioPipelineStage> buildBacteriaPipeline(long pid, BioSample bioSample, PipelineStageParameters pipelineStageParameters) throws JsonProcessingException{

        ArrayList<BioPipelineStage> stages = new ArrayList<>();
        

        BioPipelineStage qc = new BioPipelineStage();
        QcStageInputUrls qcStageInputUrls = new QcStageInputUrls();
        qcStageInputUrls.setRead1(bioSample.getRead1Url());
        qcStageInputUrls.setRead2(bioSample.getRead2Url());
        qc.setStageType(PIPELINE_STAGE_QC);
        qc.setInputUrl(JsonUtil.toJson(qcStageInputUrls));
        stages.add(qc);

        BioPipelineStage assembly = new BioPipelineStage();
        assembly.setStageType(PIPELINE_STAGE_ASSEMBLY);
        stages.add(assembly);


        BioPipelineStage taxonomy = new BioPipelineStage();
        taxonomy.setStageType(PIPELINE_STAGE_TAXONOMY);
        stages.add(taxonomy);

        BioPipelineStage amr = new BioPipelineStage();
        amr.setStageType(PIPELINE_STAGE_AMR);
        stages.add(amr);

        BioPipelineStage vf = new BioPipelineStage();
        vf.setStageType(PIPELINE_STAGE_VIRULENCE);
        stages.add(vf);

        BioPipelineStage mlst = new BioPipelineStage();
        mlst.setStageType(PIPELINE_STAGE_MLST);
        stages.add(mlst);

        BioPipelineStage serotype = new BioPipelineStage();
        serotype.setStageType(PIPELINE_STAGE_SEROTYPE);
        stages.add(serotype);


        BaseStageParams baseStageParams = new BaseStageParams();
        String serializedPamras = JsonUtil.toJson(baseStageParams);
        stages.forEach(s->{
            if(s.getStageType() == PIPELINE_STAGE_QC){
                //start point
                s.setStageIndex(0);
            }else {
                s.setStageIndex(-1);
            }
            s.setStatus(PIPELINE_STAGE_STATUS_PENDING);
            // TODO: now all use base params here. To be improved later 
            s.setParameters(serializedPamras);
            s.setStageName(STAGE_NAME_MAP.get(s.getStageType()));
            s.setPipelineId(pid);
        });

        return stages;

        
    }

    public static List<BioPipelineStage> buildVirusStages(long pid, boolean requireSNP, boolean requiredDepth, BioSample bioSample,
            PipelineStageParameters pipelineStageParams) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>(16);
        String qcInputRead1 = bioSample.getRead1Url();
        String qcInputRead2 = bioSample.getRead2Url();

        Long refseqId = pipelineStageParams.getRefseq();
        RefSeqConfig refSeqConfig = new RefSeqConfig();
        refSeqConfig.setRefseqId(refseqId == null || refseqId == -1?-1:refseqId);
        BaseStageParams baseStageParams = new BaseStageParams(refSeqConfig, null);
        

        BioPipelineStage qc = new BioPipelineStage();
        QcStageInputUrls qcStageInputUrls = new QcStageInputUrls();
        qcStageInputUrls.setRead1(qcInputRead1);
        qcStageInputUrls.setRead2(qcInputRead2);
        qc.setStageType(PIPELINE_STAGE_QC);
        qc.setInputUrl(JsonUtil.toJson(qcStageInputUrls));
        
        stages.add(qc);

        if(refSeqConfig.getRefseqId()==-1){
            BioPipelineStage assembly = new BioPipelineStage();
            assembly.setStageType(PIPELINE_STAGE_ASSEMBLY);
            stages.add(assembly);
        }

        BioPipelineStage mapping = new BioPipelineStage();
        mapping.setStageType(PIPELINE_STAGE_MAPPING);
        stages.add(mapping);

        BioPipelineStage varientCall = new BioPipelineStage();
        varientCall.setStageType(PIPELINE_STAGE_VARIANT_CALL);
        stages.add(varientCall);

        BioPipelineStage consensus = new BioPipelineStage();
        consensus.setStageType(PIPELINE_STAGE_CONSENSUS);
        stages.add(consensus);



        if(requireSNP){
            BioPipelineStage snp = new BioPipelineStage();
            snp.setStageType(PIPELINE_STAGE_SNP_CORE);
            stages.add(snp);
        }

        if(requiredDepth){
            BioPipelineStage depth = new BioPipelineStage();
            depth.setStageType(PIPELINE_STAGE_DEPTH_COVERAGE);
            stages.add(depth);
        }


        String serializedParams = JsonUtil.toJson(baseStageParams);
        for(BioPipelineStage stage: stages){
            if(stage.getStageType() == PIPELINE_STAGE_QC){
                stage.setStageIndex(0);
            }else {
                stage.setStageIndex(-1);
            }
            stage.setPipelineId(pid);
            stage.setStageName(STAGE_NAME_MAP.get(stage.getStageType()));
            stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
            stage.setParameters(serializedParams);
        }

        return stages;

    }
}
