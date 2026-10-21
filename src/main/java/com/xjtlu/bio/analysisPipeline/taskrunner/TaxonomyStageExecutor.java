package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.service.TaxonomyClassificationService;
import com.xjtlu.bio.analysisPipeline.service.TaxonomyClassificationService.ReportParseException;
import com.xjtlu.bio.analysisPipeline.service.TaxonomyClassificationService.TaxonomyClassificationItem;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput.TaxonomyClassificationOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.TaxonomicContigBinner;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.TaxonomicContigBinner.Result;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.TaxonomicContigBinner.TaxonSummary;

import jakarta.annotation.Resource;

@Component
public class TaxonomyStageExecutor
        extends AbstractPipelineStageExector<TaxonomyStageOutput, TaxonomyStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<TaxonomyStageOutput> {

    @Resource
    private TaxonomyClassificationService taxonomyClassificationService;

    // private Set<Integer> supportedFamilys;

    // private boolean loadFastANIDBMetaSuccess;

    // private static class FastANIMeta {
    // private String id;
    // private String name;
    // private int taxId;
    // private String speciesName;
    // private int speciesTaxId;

    // }

    // private Map<String, FastANIMeta> fastANIMetaQueryMap;

    private static final double KRAKEN2_FAMILY_ROUTE_THRESHOLD = 50.0d;
    private static final double KRAKEN2_SPECIES_CONFIDENT_DIRECT_PASS_THRESHOLD = 90.0d;

    private static final double CLASSIFICATION_COMPARISON_OFFSET = 0.00001d;

    private static final double FASTANI_CONFIDENT_THRESHOLD = 95.0d;

    private static final double FASTANI_CONFIDENT_DELTA = 1.5d;

    private static Path localInputPath(Path inputDirectory, String prefix, String objectName) {
        String fileName = objectName.substring(objectName.lastIndexOf("/") + 1);
        return inputDirectory.resolve(prefix + fileName);
    }

    private static boolean isAssemblyInput(BaseStageParams parameters) {
        return parameters != null
                && parameters.getSequenceMeta() != null
                && parameters.getSequenceMeta().getSequenceLevel()
                        == Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
    }

    private static List<TaxonomyClassificationItem> classificationItemsFromBins(
            List<TaxonSummary> bins,
            String rankCode) {
        return bins.stream()
                .map(bin -> new TaxonomyClassificationItem(
                        bin.getAssemblyFraction() * 100.0d,
                        bin.getContigCount(),
                        bin.getContigCount(),
                        rankCode,
                        bin.getTaxId(),
                        bin.getName()))
                .toList();
    }

    private StageRunResult<TaxonomyStageOutput> ok(
            TaxonomyStageOutput output,
            StageExecutionInput executionInput,
            Result binningResult) {
        if (binningResult != null) {
            output.setBinningMetrics(binningResult.getMetrics());
            output.setMainBinFastaPath(binningResult.getMainBinFastaPath());
            output.setContigAssignmentReportPath(binningResult.getAssignmentReportPath());
        }
        return OK(output, executionInput);
    }

    @Override
    protected Class<TaxonomyStageInputUrls> stageInputType() {
        return TaxonomyStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        return BaseStageParams.class;
    }

    private List<TaxonomyClassificationItem> doClassifyByFastANI(StageExecutionInput stageExecutionInput,
            Path queryPath, List<Integer> familyIds) throws IOException, ReportParseException {

        List<String> fastANIRefRealPathList = new ArrayList<>();
        for (int familyId : familyIds) {
            List<String> currentFamilyFastANIRefRealPathLists = taxonomyClassificationService
                    .getfastANIReferenceAccessionPaths(familyId);
            fastANIRefRealPathList.addAll(currentFamilyFastANIRefRealPathLists);
        }

        if (fastANIRefRealPathList.isEmpty()) {
            return Collections.emptyList();
        }

        Path tmpFastANIRefListPath = stageExecutionInput.workDir.resolve("query_refs.txt");
        Files.write(tmpFastANIRefListPath, fastANIRefRealPathList);
        List<String> fastANICmd = analysisPipelineToolsConfig.getFastANI();
        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(fastANICmd);

        Path outPath = stageExecutionInput.workDir.resolve("classfication.out");
        runCmd.add("-q");
        runCmd.add(queryPath.toAbsolutePath().toString());
        runCmd.add("--rl");
        runCmd.add(tmpFastANIRefListPath.toAbsolutePath().toString());
        runCmd.add("-o");
        runCmd.add(outPath.toAbsolutePath().toString());
        runCmd.add("-t");
        runCmd.add("2");

        ExecuteResult executeResult = _execute(runCmd, stageExecutionInput.workDir);
        if (!executeResult.success()) {

            logger.error(
                    "FastANI execution exception, stage = {}, cmd = {}, log = {}",
                    stageExecutionInput.stageContext.getRunStageId(),
                    String.join(" ", runCmd),
                    stageExecutionInput.workDir,
                    executeResult.ex);

            return Collections.emptyList();
        }

        List<TaxonomyClassificationItem> results = taxonomyClassificationService.parseFastANIReport(outPath);

        return results;
    }

    private static List<TaxonomyClassificationOutput> buildTaxonomyClassificationOutputFromClassificationItem(
            List<TaxonomyClassificationItem> items) {

        return items.stream().map(i -> {
            return new TaxonomyClassificationOutput(
                    i.getTaxid(),
                    i.getScientificName(),
                    i.getTaxid(),
                    i.getScientificName(),
                    i.getPercentage());
        }).toList();

    }

    @Override
    protected StageRunResult<TaxonomyStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {

        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        Path inputDirPath = stageExecutionInput.inputDir;
        Path workDirPath = stageExecutionInput.workDir;
        TaxonomyStageInputUrls taxonomyStageInputUrls = stageExecutionInput.input;
        BaseStageParams parameters = stageExecutionInput.stageParameters;
        boolean assemblyLevelInput = isAssemblyInput(parameters);

        String r1Url = taxonomyStageInputUrls.getR1();
        String r2Url = taxonomyStageInputUrls.getR2();
        String contigsUrl = taxonomyStageInputUrls.getContigs();
        if (assemblyLevelInput && StringUtils.isBlank(contigsUrl)) {
            // Backward compatibility for older assembly-level tasks which stored the
            // FASTA in r1 before TaxonomyStageInputUrls gained a dedicated field.
            contigsUrl = r1Url;
        }
        // The stage can receive both cleaned reads and the upstream assembly. When
        // contigs are available, taxonomy is intentionally performed at contig level
        // so that the per-contig calls can be grouped before FastANI. Reads remain a
        // supported fallback for standalone/read-only taxonomy tasks.
        boolean classifyContigs = StringUtils.isNotBlank(contigsUrl);

        if (assemblyLevelInput && !classifyContigs) {
            return this.runFail(bioPipelineStage, "assembly contigs input is required", workDirPath);
        }
        if (!classifyContigs && StringUtils.isBlank(r1Url)) {
            return this.runFail(bioPipelineStage, "R1 input is required for read-level taxonomy", workDirPath);
        }

        Map<String, Path> loadMap = new LinkedHashMap<>();
        Path r1Path = null;
        Path r2Path = null;
        Path contigsPath = null;

        if (!classifyContigs) {
            r1Path = localInputPath(inputDirPath, "r1_", r1Url);
            loadMap.put(r1Url, r1Path);
            if (StringUtils.isNotBlank(r2Url)) {
                r2Path = localInputPath(inputDirPath, "r2_", r2Url);
                loadMap.put(r2Url, r2Path);
            }
        }

        if (StringUtils.isNotBlank(contigsUrl)) {
            if (contigsUrl.equals(r1Url) && r1Path != null) {
                contigsPath = r1Path;
            } else {
                contigsPath = localInputPath(inputDirPath, "contigs_", contigsUrl);
                loadMap.put(contigsUrl, contigsPath);
            }
        }

        this.loadInput(loadMap);

        try {
            if (r1Path != null) {
                r1Path = uncompressIfCompressedFormat(r1Path);
            }
            if (r2Path != null) {
                r2Path = uncompressIfCompressedFormat(r2Path);
            }
            if (contigsPath != null && !contigsPath.equals(r1Path)) {
                contigsPath = uncompressIfCompressedFormat(contigsPath);
            } else if (contigsPath != null) {
                contigsPath = r1Path;
            }
        } catch (IOException e) {
            String failReason = "Failed to uncompress taxonomy input: " + e.getMessage();
            this.logger.error(failReason, e);
            return this.runFail(stageExecutionInput.stageContext, failReason, stageExecutionInput.workDir);
        }

        Path krakenInputPath = classifyContigs ? contigsPath : r1Path;

        Path reportPath = workDirPath.resolve("taxonomny.report");
        Path outputPath = workDirPath.resolve("taxonomy.output");

        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getKraken2());
        runCmd.add("--db");
        runCmd.add(taxonomyClassificationService.getKraken2DB());
        runCmd.add("--report");
        runCmd.add(reportPath.toString());
        runCmd.add("--output");
        runCmd.add(outputPath.toString());
        if (!classifyContigs && r2Path != null) {
            runCmd.add("--paired");
        }
        runCmd.add(krakenInputPath.toString());
        if (!classifyContigs && r2Path != null) {
            runCmd.add(r2Path.toString());
        }

        logger.info("stage = {} start to run", bioPipelineStage);
        ExecuteResult executeResult = _execute(runCmd, workDirPath);

        if (executeResult.runCode != 0 || executeResult.ex != null) {
            logger.error("stage = {} run failed. Code = {}", bioPipelineStage, executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "execution failed", stageExecutionInput.workDir);
        }

        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(reportPath, outputPath);
        if (!stageOutputValidationResults.isEmpty()) {
            logger.error("stage = {}. Validate file = {} failed", bioPipelineStage,
                    stageOutputValidationResults.get(0).path.toString(),
                    stageOutputValidationResults.get(0).ioException);
            return this.runFail(bioPipelineStage, "validate output file failed", stageExecutionInput.workDir);
        }

        List<TaxonomyClassificationItem> kraken2ClassificationItemList = null;
        try {
            kraken2ClassificationItemList = taxonomyClassificationService.parseKraken2Report(reportPath);
        } catch (ReportParseException e) {
            this.logger.error(
                    "Failed to parse Kraken2 report. reportPath={}",
                    reportPath == null ? null : reportPath.toAbsolutePath(),
                    e);
            return this.runFail(bioPipelineStage, "Failed to parse Kraken2 report", e, workDirPath);
        }

        List<TaxonomyClassificationItem> familyLevelList = new ArrayList<>();
        List<TaxonomyClassificationItem> speciesCandicates = new ArrayList<>();
        Result binningResult = null;
        Path fastAniQueryPath = contigsPath;

        if (classifyContigs) {
            try {
                binningResult = TaxonomicContigBinner.bin(
                        contigsPath,
                        outputPath,
                        reportPath,
                        workDirPath);
            } catch (IOException e) {
                logger.error(
                        "Failed to build assembly taxonomic bins. stage={}, contigs={}, output={}, report={}",
                        bioPipelineStage,
                        contigsPath,
                        outputPath,
                        reportPath,
                        e);
                return this.runFail(bioPipelineStage, "Failed to build assembly taxonomic bins", e, workDirPath);
            }
            speciesCandicates.addAll(classificationItemsFromBins(
                    binningResult.getSpeciesBins(),
                    TaxonomyClassificationService.RANK_CODE_SPECIES));
            familyLevelList.addAll(classificationItemsFromBins(
                    binningResult.getFamilyBins(),
                    TaxonomyClassificationService.RANK_CODE_FAMILY));
        } else {
            for (TaxonomyClassificationItem candicate : kraken2ClassificationItemList) {
                if (TaxonomyClassificationService.RANK_CODE_FAMILY.equals(candicate.getRankCode())) {
                    familyLevelList.add(candicate);
                } else if (TaxonomyClassificationService.RANK_CODE_SPECIES.equals(candicate.getRankCode())) {
                    speciesCandicates.add(candicate);
                }
            }
        }

        if (familyLevelList.isEmpty()) {
            TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput();
            taxonomyStageOutput.setEvidenceResource(Constants.TaxonomyClassification.EVIDENCE_KRAKEN2);
            taxonomyStageOutput.setStatus(Constants.TaxonomyClassification.STATUS_OUT_PANEL);
            taxonomyStageOutput.setCandicates(Collections.emptyList());
            return ok(taxonomyStageOutput, stageExecutionInput, binningResult);
        }

        speciesCandicates.sort((s1, s2) -> Double.compare(s2.getPercentage(), s1.getPercentage()));

        List<TaxonomyClassificationOutput> candicates = new ArrayList<>();

        if (!speciesCandicates.isEmpty()) {
            TaxonomyClassificationItem kraken2BestSpecies = speciesCandicates.get(0);
            if (kraken2BestSpecies.getPercentage() >= KRAKEN2_SPECIES_CONFIDENT_DIRECT_PASS_THRESHOLD
                    && (speciesCandicates.size() < 2
                            || kraken2BestSpecies.getPercentage()
                                    / (CLASSIFICATION_COMPARISON_OFFSET + speciesCandicates.get(1).getPercentage()) >= 2)) {

                for (TaxonomyClassificationItem candicate : speciesCandicates) {
                    TaxonomyClassificationOutput taxonomyClassificationOutput = new TaxonomyClassificationOutput(
                            candicate.getTaxid(),
                            candicate.getScientificName(),
                            candicate.getTaxid(),
                            candicate.getScientificName(),
                            candicate.getPercentage());

                    candicates.add(taxonomyClassificationOutput);
                }

                TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput(
                        candicates,
                        candicates.stream()
                                .filter(s -> s.getTaxId() == kraken2BestSpecies.getTaxid())
                                .findAny()
                                .orElse(null),
                        Constants.TaxonomyClassification.STATUS_CONFIDENT,
                        Constants.TaxonomyClassification.EVIDENCE_KRAKEN2);
                return ok(taxonomyStageOutput, stageExecutionInput, binningResult);
            }
        }

        familyLevelList.sort((t1, t2) -> {
            return Double.compare(t2.getPercentage(), t1.getPercentage());
        });

        TaxonomyClassificationItem bestFamily = familyLevelList.get(0);

        if (bestFamily.getPercentage() >= KRAKEN2_FAMILY_ROUTE_THRESHOLD) {

            int familyTaxId = bestFamily.getTaxid();
            // out panel. However, it is almost impossible since we use built db to run and
            // the result should be located the range we made in the db.
            if (!taxonomyClassificationService.isSupported(familyTaxId,
                    TaxonomyClassificationService.SUPPORT_QUERY_FAMILY_LEVEL)) {
                for (TaxonomyClassificationItem taxonomyClassificationItem : speciesCandicates) {
                    candicates.add(new TaxonomyClassificationOutput(
                            taxonomyClassificationItem.getTaxid(),
                            taxonomyClassificationItem.getScientificName(),
                            taxonomyClassificationItem.getTaxid(),
                            taxonomyClassificationItem.getScientificName(),
                            taxonomyClassificationItem.getPercentage()));
                }
                TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput(candicates, null,
                        Constants.TaxonomyClassification.STATUS_OUT_PANEL,
                        Constants.TaxonomyClassification.EVIDENCE_KRAKEN2);
                return ok(taxonomyStageOutput, stageExecutionInput, binningResult);
            }

            List<Integer> queryFamilies = familyLevelList.stream()
                    .filter(f -> f.getPercentage() / bestFamily.getPercentage() >= 0.85)
                    .map(TaxonomyClassificationItem::getTaxid)
                    .filter(family -> taxonomyClassificationService.isSupported(
                            family,
                            TaxonomyClassificationService.SUPPORT_QUERY_FAMILY_LEVEL))
                    .toList();

            if (binningResult != null
                    && binningResult.getMainBinFastaPath() != null
                    && binningResult.getDominantSpeciesFamilyTaxId() != null
                    && queryFamilies.contains(binningResult.getDominantSpeciesFamilyTaxId())) {
                fastAniQueryPath = binningResult.getMainBinFastaPath();
            }

            if (fastAniQueryPath == null) {
                logger.warn(
                        "FastANI requires assembly contigs but none were provided. Use Kraken2 result only. stage={}",
                        bioPipelineStage);
                TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput(
                        buildTaxonomyClassificationOutputFromClassificationItem(speciesCandicates),
                        null,
                        Constants.TaxonomyClassification.STATUS_LOW_CONFIDENCE,
                        Constants.TaxonomyClassification.EVIDENCE_KRAKEN2);
                return ok(taxonomyStageOutput, stageExecutionInput, binningResult);
            }

            List<TaxonomyClassificationItem> fastANIClassificationCandicates = null;
            try {
                fastANIClassificationCandicates = doClassifyByFastANI(
                        stageExecutionInput,
                        fastAniQueryPath,
                        queryFamilies);
            } catch (IOException | ReportParseException e) {
                logger.error(
                        "Failed to classify taxonomy by FastANI. stage={}, queryPath={}, queryFamilies={}, workDir={}",
                        bioPipelineStage,
                        fastAniQueryPath.toAbsolutePath(),
                        queryFamilies,
                        workDirPath == null ? null : workDirPath.toAbsolutePath(),
                        e);

                return this.runFail(bioPipelineStage, "FastANI classification failed", e, workDirPath);
            }

            if (fastANIClassificationCandicates == null || fastANIClassificationCandicates.isEmpty()) {
                logger.warn(
                        "FastANI finished but no classification candidates were found. Use kraken2 result instead.  stage={}, queryPath={}, queryFamilies={}, workDir={}",
                        bioPipelineStage,
                        fastAniQueryPath.toAbsolutePath(),
                        queryFamilies,
                        workDirPath == null ? null : workDirPath.toAbsolutePath());

                candicates = buildTaxonomyClassificationOutputFromClassificationItem(speciesCandicates);
                TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput();
                taxonomyStageOutput.setCandicates(candicates);
                taxonomyStageOutput.setComfirmedTaxonomy(null);
                taxonomyStageOutput.setStatus(Constants.TaxonomyClassification.STATUS_LOW_CONFIDENCE);
                taxonomyStageOutput.setEvidenceResource(Constants.TaxonomyClassification.EVIDENCE_KRAKEN2);

                return ok(taxonomyStageOutput, stageExecutionInput, binningResult);
            }

            Map<Integer, List<TaxonomyClassificationItem>> groupedClassificationItems = fastANIClassificationCandicates
                    .stream().collect(
                            Collectors.groupingBy(
                                    TaxonomyClassificationItem::getTaxid));

            double top1 = -1;
            int top1TaxId = -1;

            double top2 = -1;

            for (Map.Entry<Integer, List<TaxonomyClassificationItem>> entry : groupedClassificationItems.entrySet()) {

                double max = -1;
                TaxonomyClassificationItem bestHitInGroup = null;
                for (TaxonomyClassificationItem i : entry.getValue()) {
                    if (i.getPercentage() > max) {
                        max = i.getPercentage();
                        bestHitInGroup = i;
                    }
                }

                candicates.add(
                        new TaxonomyClassificationOutput(
                                bestHitInGroup.getTaxid(),
                                bestHitInGroup.getScientificName(),
                                bestHitInGroup.getTaxid(),
                                bestHitInGroup.getScientificName(),
                                bestHitInGroup.getPercentage()));
                if (max > top1) {
                    top2 = top1;
                    top1TaxId = entry.getKey();
                    top1 = max;
                } else if (max > top2) {
                    top2 = max;
                }
            }

            TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput();
            taxonomyStageOutput.setCandicates(candicates);
            taxonomyStageOutput.setEvidenceResource(TaxonomyClassification.EVIDENCE_FASTANI);

            if (top1 < FASTANI_CONFIDENT_THRESHOLD) {
                taxonomyStageOutput.setStatus(TaxonomyClassification.STATUS_LOW_CONFIDENCE);

                return ok(taxonomyStageOutput, stageExecutionInput, binningResult);
            }

            if (top1 - top2 < FASTANI_CONFIDENT_DELTA) {
                taxonomyStageOutput.setStatus(TaxonomyClassification.STATUS_AMBIGUOUS);
                return ok(taxonomyStageOutput, stageExecutionInput, binningResult);
            }

            taxonomyStageOutput.setStatus(TaxonomyClassification.STATUS_CONFIDENT);

            for (TaxonomyClassificationOutput taxonomyClassificationOutput : candicates) {
                if (taxonomyClassificationOutput.getTaxId() == top1TaxId) {
                    taxonomyStageOutput.setComfirmedTaxonomy(taxonomyClassificationOutput);
                    break;
                }
            }
            return ok(taxonomyStageOutput, stageExecutionInput, binningResult);
        }

        for (TaxonomyClassificationItem species : speciesCandicates) {

            candicates.add(
                    new TaxonomyClassificationOutput(
                            species.getTaxid(),
                            species.getScientificName(),
                            species.getTaxid(),
                            species.getScientificName(),
                            species.getPercentage()));
        }

        TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput();
        taxonomyStageOutput.setCandicates(candicates);
        taxonomyStageOutput.setEvidenceResource(Constants.TaxonomyClassification.EVIDENCE_KRAKEN2);
        taxonomyStageOutput.setStatus(Constants.TaxonomyClassification.STATUS_LOW_CONFIDENCE);

        return ok(taxonomyStageOutput, stageExecutionInput, binningResult);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_TAXONOMY;
    }

}
