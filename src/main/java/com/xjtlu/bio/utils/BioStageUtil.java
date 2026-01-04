package com.xjtlu.bio.utils;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.stereotype.Component;

import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.ConsensusStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.QCStageOutput;
import com.xjtlu.bio.taskrunner.stageOutput.VariantStageOutput;

@Component
public class BioStageUtil {

    private String stageWorkDirPath;
    private String stageInputDirPath;

    public Path stageExecutorWorkDir(BioPipelineStage bioPipelineStage){
        //todo
        return Paths.get(stageWorkDirPath, String.valueOf(bioPipelineStage.getStageId()));
    }

    public Path stageExecutorInputDir(BioPipelineStage bioPipelineStage){
        //todo
        return Paths.get(stageInputDirPath, String.valueOf(bioPipelineStage.getStageId()));
    }

    public String createStoreObjectName(BioPipelineStage pipelineStage, String name){
        return String.format(
            "stageOutput/%d/%s",
            pipelineStage.getStageId(),
            name
        );
    }

    public QCStageOutput qcStageOutput(Path dir, boolean hasR2){


        return new QCStageOutput(
            dir.resolve(QCStageOutput.R1).toString(),
            hasR2?dir.resolve(QCStageOutput.R2).toString():null,
            dir.resolve(QCStageOutput.JSON).toString(),
            dir.resolve(QCStageOutput.HTML).toString()
        );
    }

    public AssemblyStageOutput assemblyOutput(BioPipelineStage bioPipelineStage, Path dir){
        Path config = dir.resolve(AssemblyStageOutput.CONTIG);
        Path scaffold = dir.resolve(AssemblyStageOutput.SCAFFOLD);
        return new AssemblyStageOutput(config.toString(), scaffold.toString());
    }

    public MappingStageOutput mappingOutput(BioPipelineStage bioPipelineStage, Path dir){
        Path bamPath = dir.resolve(MappingStageOutput.BAM);
        Path bamIndex = dir.resolve(MappingStageOutput.BAM_INDEX);
        return new MappingStageOutput(bamPath.toString(), bamIndex.toString());
    }

    public VariantStageOutput varientOutput(BioPipelineStage bioPipelineStage, Path dir){
        Path vcfGz = dir.resolve(VariantStageOutput.VCF_GZ);
        Path vcfTbi = dir.resolve(VariantStageOutput.VCF_TBI);
        return new VariantStageOutput(vcfGz.toString(), vcfTbi.toString());
    }

    public ConsensusStageOutput consensusOutput(BioPipelineStage bioPipelineStage, Path dir){
        Path consesnusFa = dir.resolve(ConsensusStageOutput.CONSENSUS);
        return new ConsensusStageOutput(consesnusFa.toString());
    }

}
