package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SeroTypingStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.utils.JsonUtil;

@Component
public class SeroTypingStageExectuor extends AbstractPipelineStageExector<SeroTypingStageOutput>
        implements PipelineStageExecutor<SeroTypingStageOutput> {

    private static final int TAX_ID_SALMONELLA = 28901;
    private static final int TAX_ID_ESCHERICHIA_COLI = 562;
    private static final int TAX_ID_KLEBSIELLA_PNEUMONIAE = 573;
    private static final int TAX_ID_STREPTOCOCCUS_PNEUMONIAE = 1313;

    public static final int INPUT_TYPE_READS = 0;
    public static final int INPUT_TYPE_CONTIGS = 1;

    public static final Set<Integer> SUPPORTED_SEROTYPE_TAXID = Set.of(
            TAX_ID_ESCHERICHIA_COLI, // Salmonella enterica
            TAX_ID_KLEBSIELLA_PNEUMONIAE, // E. coli
            TAX_ID_SALMONELLA, // Klebsiella pneumoniae
            TAX_ID_STREPTOCOCCUS_PNEUMONIAE // Streptococcus pneumoniae
    );

    private static boolean isSpeciesOrBelow(String rank) {
        if (rank == null)
            return false;
        String r = rank.toLowerCase();
        return r.contains("species") || r.contains("strain") || r.contains("sub");
    }

    public static boolean canDoSeroType(TaxonomyContext ctx) {
        // 2. 必须至少到 species
        if (!isSpeciesOrBelow(ctx.getRank())) {
            return false;
        }

        // 3. taxid 在支持列表
        Integer taxid = ctx.getTaxid();
        if (taxid == null) {
            return false;
        }

        return SUPPORTED_SEROTYPE_TAXID.contains(taxid);
    }

    public static int inputType(TaxonomyContext ctx) {

        int taxid = ctx.getTaxid();

        switch (taxid) {

            case TAX_ID_SALMONELLA: // SeqSero2 推荐 reads
            case TAX_ID_STREPTOCOCCUS_PNEUMONIAE:
                return INPUT_TYPE_READS; // seroBA 必须 reads

            case TAX_ID_ESCHERICHIA_COLI:
            case TAX_ID_KLEBSIELLA_PNEUMONIAE:
                return INPUT_TYPE_CONTIGS; // contig 工具
            default:
                return INPUT_TYPE_CONTIGS;
        }
    }

    private StageRunResult<SeroTypingStageOutput> executeSalmonellaType(StageExecutionInput stageExecutionInput,
            Path r1Path, Path r2Path) {
        List<String> cmd = new ArrayList<>();

        cmd.addAll(this.analysisPipelineToolsConfig.getSeqsero2());
        cmd.add("-i");
        cmd.add(r1Path.toString());
        if (r2Path != null) {
            cmd.add(r2Path.toString());
        }
        cmd.add("-t");
        cmd.add(r2Path == null ? "1" : "2");
        cmd.add("-d");
        cmd.add(stageExecutionInput.workDir.toString());

        Path resultPath = stageExecutionInput.workDir.resolve("SeqSero_result.txt");
        boolean res = _execute(cmd, null, stageExecutionInput, resultPath);

        if (!res) {
            return this.runFail(stageExecutionInput.bioPipelineStage, "未成功执行");
        }
        return StageRunResult.OK(new SeroTypingStageOutput(resultPath), stageExecutionInput.bioPipelineStage);
    }

    private StageRunResult<SeroTypingStageOutput> executeECoilType(StageExecutionInput stageExecutionInput,
            Path contigPath) {
        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getEctyper());

        cmd.add("-i");
        cmd.add(contigPath.toString());
        cmd.add("-o");
        cmd.add(stageExecutionInput.workDir.toString());

        Path resultPath = stageExecutionInput.workDir.resolve("Ectyper_result.txt");
        boolean res = _execute(cmd, null, stageExecutionInput, resultPath);

        if (!res) {
            return this.runFail(stageExecutionInput.bioPipelineStage, "未成功执行");
        }
        return StageRunResult.OK(new SeroTypingStageOutput(resultPath), stageExecutionInput.bioPipelineStage);

    }

    private StageRunResult<SeroTypingStageOutput> executeKlebsiellaType(StageExecutionInput stageExecutionInput,
            Path contigPath) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getKaptive());

        cmd.add("-a");
        cmd.add(contigPath.toString());
        cmd.add("-o");
        cmd.add(stageExecutionInput.workDir.toString());

        Path resultPath = stageExecutionInput.workDir.resolve("kaptive_results.tsv");

        boolean res = _execute(cmd, null, stageExecutionInput, resultPath);
        if (!res) {
            return this.runFail(stageExecutionInput.bioPipelineStage, "未成功执行");
        }
        return StageRunResult.OK(new SeroTypingStageOutput(resultPath), stageExecutionInput.bioPipelineStage);
    }

    private StageRunResult<SeroTypingStageOutput> executeStreptococcusType(StageExecutionInput stageExecutionInput,
            Path r1Path, Path r2Path) {
        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getSeroBA());

        cmd.add(r1Path.toString());
        if (r2Path != null) {
            cmd.add(r2Path.toString());
        }
        cmd.add(stageExecutionInput.workDir.toString());

        Path resultFile = stageExecutionInput.workDir.resolve("pred.tsv");

        return _execute(cmd, null, stageExecutionInput, resultFile)
                ? this.runFail(stageExecutionInput.bioPipelineStage, "未执行成功")
                : StageRunResult.OK(new SeroTypingStageOutput(resultFile), stageExecutionInput.bioPipelineStage);

    }

    @Override
    protected StageRunResult<SeroTypingStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException {
        // TODO Auto-generated method stub
        BioPipelineStage stage = stageExecutionInput.bioPipelineStage;
        Path inputDir = stageExecutionInput.inputDir;
        Path outputDir = stageExecutionInput.workDir;

        SeroTypeStageInputUrls seroTypeStageInputUrls = JsonUtil.toObject(stage.getInputUrl(),
                SeroTypeStageInputUrls.class);

        String contigUrl = seroTypeStageInputUrls.getContigsUrl();
        String r1Url = seroTypeStageInputUrls.getR1Url();
        String r2Url = seroTypeStageInputUrls.getR2Url();

        SeroTypingStageParameters parameters = JsonUtil.toObject(stage.getParameters(),
                SeroTypingStageParameters.class);

        Path contigLocalPath = inputDir.resolve("input.contig");
        Path r1Path = inputDir.resolve("r1.fastq.gz");
        Path r2Path = inputDir.resolve("r2.fastq.gz");
        Map<String, Path> loadInputMap = contigUrl != null ? Map.of(contigUrl, contigLocalPath)
                : (r2Path != null ? Map.of(r1Url, r1Path) : Map.of(r1Url, r1Path, r2Url, r2Path));
        boolean loadRes = loadInput(loadInputMap);
        if (!loadRes) {
            return this.runFail(stage, "failed to load");
        }

        TaxonomyContext taxonomyCtx = parameters.getTaxonomyContext();
        int taxId = taxonomyCtx.getTaxid();

        switch (taxId) {
            case TAX_ID_SALMONELLA:

                return executeSalmonellaType(stageExecutionInput, r1Path, r2Url == null ? null : r2Path);

            case TAX_ID_ESCHERICHIA_COLI:
                return executeECoilType(stageExecutionInput, contigLocalPath);
            case TAX_ID_KLEBSIELLA_PNEUMONIAE:
                return executeKlebsiellaType(stageExecutionInput, contigLocalPath);
            case TAX_ID_STREPTOCOCCUS_PNEUMONIAE:
                return executeStreptococcusType(stageExecutionInput, r1Path, r2Url == null ? null : r2Path);
            default:
                logger.warn(
                        "No suitable serotyping tool matched. stageId={}, pipelineId={}, stageName={}, taxId={}, species={}, rank={}, status={}",
                        stage.getStageId(),
                        stage.getPipelineId(),
                        stage.getStageName(),
                        taxId,
                        taxonomyCtx.getSpeciesName(),
                        taxonomyCtx.getRank(),
                        taxonomyCtx.getStatus());
                break;
        }

        return this.runFail(stage, "未匹配到适用的血清型分析工具");
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_SEROTYPE;
    }

}
