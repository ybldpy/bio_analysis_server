package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants.SequenceInput;
import com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AssemblyStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AssemblyStageOutput;

@Component
public class AssemblyExecutor extends AbstractPipelineStageExector<AssemblyStageOutput, AssemblyInputUrls, AssemblyStageParameters> {


    @Value("${analysis-pipeline.stage.assembly.contigsFileName}")
    private String contigsFileName;

    @Value("${analysis-pipeline.stage.assembly.scaffoldFileName}")
    private String scaffoldsFileName;

    // These are configurable defaults, not an inference of raw/HQ/HiFi read subtype.
    @Value("${analysis-pipeline.stage.assembly.flye.ont-input-mode:nano-raw}")
    private String flyeOntInputMode = "nano-raw";

    @Value("${analysis-pipeline.stage.assembly.flye.pacbio-input-mode:pacbio-raw}")
    private String flyePacbioInputMode = "pacbio-raw";

    @Override
    protected Class<AssemblyInputUrls> stageInputType() {
        return AssemblyInputUrls.class;
    }

    @Override
    protected Class<AssemblyStageParameters> stageParameterType() {
        return AssemblyStageParameters.class;
    }

    @Override
    public StageRunResult<AssemblyStageOutput> _execute(StageExecutionInput stageExecutionInput) throws JsonMappingException, JsonProcessingException, LoadFailException {
        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        
        AssemblyInputUrls assemblyInputUrls = stageExecutionInput.input;

        String r1 = assemblyInputUrls.getRead1Url();
        String r2 = assemblyInputUrls.getRead2Url();

        Path tempInputDir = stageExecutionInput.inputDir;
        Path workDir = stageExecutionInput.workDir;

        SequenceMeta readMeta = stageExecutionInput.stageParameters == null
                ? null : stageExecutionInput.stageParameters.getReadMeta();
        boolean longRead = readMeta != null && readMeta.getReadLenType() == SequenceInput.READ_LEN_TYPE_LONG;
        String flyeInputMode = null;
        List<String> flyeCommand = null;
        if (longRead) {
            // R1/R2 are paired-end slots, not an interface for mixing long-read libraries.
            if (r2 != null && !r2.isBlank()) {
                return runFail(bioPipelineStage,
                        "Flye long-read assembly expects a single read input, not paired R1/R2", workDir);
            }
            flyeInputMode = resolveFlyeInputMode(readMeta.getSequencingPlatform());
            flyeCommand = analysisPipelineToolsConfig.getFlye();
            if (flyeCommand == null || flyeCommand.isEmpty()
                    || flyeCommand.stream().anyMatch(part -> part == null || part.isBlank())) {
                return runFail(bioPipelineStage,
                        "Flye command is not configured: analysis-pipeline.tools.flye", workDir);
            }
            // Treat an optional blank R2 as absent on the single-input Flye route.
            r2 = null;
        }

        Path r1Path = tempInputDir.resolve(r1.substring(r1.lastIndexOf("/")+1));

        Path r2Path = null;
        if (r2 != null) {
            r2Path = tempInputDir.resolve(r2.substring(r2.lastIndexOf("/")+1));
        }

        this.loadInput(r2!=null?Map.of(r1, r1Path, r2, r2Path):Map.of(r1,r1Path));

        List<String> cmd = new ArrayList<>();
        if (longRead) {
            cmd.addAll(flyeCommand);
            cmd.add("--" + flyeInputMode);
            cmd.add(r1Path.toString());
            cmd.add("--threads");
            cmd.add("3");
            cmd.add("--out-dir");
            cmd.add(workDir.toString());
            logger.info("Assembly uses Flye: sequencingPlatform={}, inputMode={}",
                    readMeta.getSequencingPlatform(), flyeInputMode);
        } else {
            // Keep the existing short-read (and legacy missing-metadata) route unchanged.
            cmd.addAll(this.analysisPipelineToolsConfig.getSpades());
            cmd.add("-t");
            cmd.add(String.valueOf(3));
            if (r2Path != null) {
                cmd.add("-1");
                cmd.add(r1Path.toString());
                cmd.add("-2");
                cmd.add(r2Path.toString());
            } else {
                cmd.add("-s");
                cmd.add(r1Path.toString());
            }
            cmd.add("--phred-offset");
            cmd.add("33");
            cmd.add("-o");
            cmd.add(workDir.toString());
        }

        ExecuteResult executeResult = _execute(cmd, workDir);
        if(!executeResult.success()){
            return this.runFail(bioPipelineStage, longRead ? "运行Flye tool失败" : "运行spades tool失败",
                    executeResult.ex, workDir);
        }

        Path contigs = workDir.resolve(longRead ? "assembly.fasta" : contigsFileName);

        List<StageOutputValidationResult> errOutputValidationResults = validateOutputFiles(contigs);
        
        if(!errOutputValidationResults.isEmpty()){
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errOutputValidationResults), stageExecutionInput.workDir);
        }

        // Flye's final assembly uses the same downstream contigs contract; no separate scaffold is required.
        if (longRead) {
            return OK(new AssemblyStageOutput(contigs.toString(), null), stageExecutionInput);
        }

        Path scaffolds = workDir.resolve(scaffoldsFileName);
        AssemblyStageOutput assemblyStageOutput = new AssemblyStageOutput(contigs.toString(), scaffolds.toString());
        boolean hasScaffold = true;
        try{
            if(!this.requireNonEmpty(scaffolds)){
                hasScaffold = false;
            }
        }catch(IOException e){
            //if error happen here, just ingore. The callback will know it and handle
            hasScaffold = false;
        }

        if(!hasScaffold){
            assemblyStageOutput.setScaffoldPath(null);
        }
        return OK(assemblyStageOutput, stageExecutionInput);
    }

    private String resolveFlyeInputMode(int platform) {
        String mode;
        List<String> supportedModes;
        if (platform == SequencingPlatform.ONT || platform == SequencingPlatform.UNKNOWN) {
            if (platform == SequencingPlatform.UNKNOWN) {
                logger.warn("Long-read sequencing platform is UNKNOWN; using the configured ONT Flye mode");
            }
            mode = flyeOntInputMode;
            supportedModes = List.of("nano-raw", "nano-hq", "nano-corr");
        } else if (platform == SequencingPlatform.PACBIO) {
            mode = flyePacbioInputMode;
            supportedModes = List.of("pacbio-raw", "pacbio-hifi", "pacbio-corr");
        } else {
            throw new IllegalArgumentException("Unsupported sequencingPlatform for Flye long-read assembly: " + platform);
        }
        if (mode == null || !supportedModes.contains(mode)) {
            throw new IllegalArgumentException("Invalid Flye input mode for sequencingPlatform " + platform
                    + ": " + mode + "; expected one of " + supportedModes);
        }
        return mode;
    }

    @Override
    public int id() {
        return PIPELINE_STAGE_ASSEMBLY;
    }

}
