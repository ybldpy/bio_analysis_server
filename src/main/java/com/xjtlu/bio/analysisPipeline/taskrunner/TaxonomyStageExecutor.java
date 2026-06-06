package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private static final double FASTANI_CONFIDENT_THRESHOLD = 80.0d;

    @Override
    protected Class<TaxonomyStageInputUrls> stageInputType() {
        return TaxonomyStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        return BaseStageParams.class;
    }

    // private void initFastANIMeta() {

    // supportedFamilys = new HashSet<>();
    // fastANIMetaQueryMap = new HashMap<>();

    // if (fastANIDB == null || fastANIDB.isBlank()) {
    // this.logger.error("FastANI DB path is empty. Skip loading FastANI
    // metadata.");
    // return;
    // }

    // Path fastANIDBPath = Path.of(fastANIDB);
    // Path metaDirPath = fastANIDBPath.resolve("meta");

    // if (!Files.exists(metaDirPath)) {
    // this.logger.error("FastANI meta directory does not exist: {}",
    // metaDirPath.toAbsolutePath());
    // return;
    // }

    // if (!Files.isDirectory(metaDirPath)) {
    // this.logger.error("FastANI meta path is not a directory: {}",
    // metaDirPath.toAbsolutePath());
    // return;
    // }

    // String[] refsQueryListFiles = metaDirPath.toFile().list();

    // if (refsQueryListFiles == null) {
    // this.logger.error(
    // "Failed to list FastANI meta directory. Please check permission. path={}",
    // metaDirPath.toAbsolutePath());
    // return;
    // }

    // Pattern pattern = Pattern.compile("^refs_(\\d+)\\.txt$");

    // for (String fname : refsQueryListFiles) {
    // Matcher matcher = pattern.matcher(fname);

    // if (!matcher.matches()) {
    // continue;
    // }

    // int familyId = Integer.parseInt(matcher.group(1));
    // supportedFamilys.add(familyId);
    // }

    // Path metaDataPath = metaDirPath.resolve("metaData.tsv");

    // if (!Files.exists(metaDataPath)) {
    // this.logger.error("FastANI metadata file does not exist: {}",
    // metaDataPath.toAbsolutePath());
    // return;
    // }

    // if (!Files.isRegularFile(metaDataPath)) {
    // this.logger.error("FastANI metadata path is not a regular file: {}",
    // metaDataPath.toAbsolutePath());
    // return;
    // }

    // try (BufferedReader bufferedReader = Files.newBufferedReader(metaDataPath)) {
    // String header = bufferedReader.readLine().strip();
    // // Map<String, Integer> headerNameIndexMap = new HashMap<>();
    // // String[] headerParts = header.split("\t");
    // // for(int i = 0;i<headerParts.length;i++){
    // // headerParts[i] = headerParts[i].strip();
    // // }

    // String line = null;
    // while ((line = bufferedReader.readLine()) != null) {
    // if (StringUtils.isBlank(line)) {
    // continue;
    // }

    // String[] metaRow = line.strip().split("\t");
    // FastANIMeta fastANIMeta = new FastANIMeta();
    // fastANIMeta.id = metaRow[0];
    // fastANIMeta.name = metaRow[1];
    // fastANIMeta.taxId = Integer.parseInt(metaRow[2]);
    // fastANIMeta.speciesName = metaRow[3];
    // fastANIMeta.speciesTaxId = Integer.parseInt(metaRow[4]);
    // fastANIMetaQueryMap.put(fastANIMeta.id, fastANIMeta);
    // }

    // loadFastANIDBMetaSuccess = true;

    // } catch (Exception e) {

    // }

    // }

    // @PostConstruct
    // public void init() {

    // try {
    // initFastANIMeta();
    // } catch (Exception e) {
    // // TODO: print a log
    // logger.error("Failed to initialize FastANI metadata.", e);

    // }
    // }

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
            // TODO: do log here
        }

        List<TaxonomyClassificationItem> results = taxonomyClassificationService.parseFastANIReport(outPath);

        return results;
    }

    private static List<TaxonomyClassificationOutput> buildTaxonomyClassificationOutputFromClassificationItem(List<TaxonomyClassificationItem> items){

        return items.stream().map(i->{
            return new TaxonomyClassificationOutput(
                i.getTaxid(),
                i.getScientificName(),
                i.getTaxid(),
                i.getScientificName(),
                i.getPercentage()
            );
        }).toList();


    }

    @Override
    protected StageRunResult<TaxonomyStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {

        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        Path inputDirPath = stageExecutionInput.inputDir;
        Path workDirPath = stageExecutionInput.workDir;
        TaxonomyStageInputUrls taxonomyStageInputUrls = stageExecutionInput.input;

        String r1Url = taxonomyStageInputUrls.getR1();
        Path r1Path = inputDirPath.resolve("r1.fastq");

        this.loadInput(Map.of(r1Url, r1Path));

        Path reportPath = workDirPath.resolve("taxonomny.report");
        Path outputPath = workDirPath.resolve("taxonomy.output");

        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getKraken2());
        runCmd.add("--db");
        runCmd.add(taxonomyClassificationService.getKraken2DB());
        runCmd.add(r1Path.toString());
        runCmd.add("--report");
        runCmd.add(reportPath.toString());
        runCmd.add("--output");
        runCmd.add(outputPath.toString());

        logger.info("stage = {} start to run", bioPipelineStage);
        ExecuteResult executeResult = _execute(runCmd, workDirPath);

        if (executeResult.runCode != 0 || executeResult.ex != null) {
            logger.error("stage = {} run failed. Code = {}", bioPipelineStage, executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "execution failed", stageExecutionInput.workDir);
        }

        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(reportPath);
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
            // fail

            this.logger.error(
                    "Failed to parse Kraken2 report. reportPath={}",
                    reportPath == null ? null : reportPath.toAbsolutePath(),
                    e);
            this.runFail(bioPipelineStage, null, workDirPath);
        }

        List<TaxonomyClassificationItem> familyLevelList = new ArrayList<>();
        List<TaxonomyClassificationItem> speciesCandicates = new ArrayList<>();

        for (TaxonomyClassificationItem candicate : kraken2ClassificationItemList) {
            if (TaxonomyClassificationService.RANK_CODE_FAMILY.equals(candicate.getRankCode())) {
                familyLevelList.add(candicate);
            } else if (TaxonomyClassificationService.RANK_CODE_SPECIES.equals(candicate.getRankCode())) {
                speciesCandicates.add(candicate);
            }
        }

        speciesCandicates.sort((s1, s2) -> Double.compare(s2.getPercentage(), s1.getPercentage()));

        List<TaxonomyClassificationOutput> candicates = new ArrayList<>();

        TaxonomyClassificationItem bestSpecies = speciesCandicates.get(0);
        if (bestSpecies.getPercentage() >= KRAKEN2_SPECIES_CONFIDENT_DIRECT_PASS_THRESHOLD
                && (speciesCandicates.size() < 2
                        || bestSpecies.getPercentage()
                                / (CLASSIFICATION_COMPARISON_OFFSET + speciesCandicates.get(1).getPercentage()) >= 2)) {

            List<TaxonomyClassificationOutput> taxonomyClassificationOutputs = new ArrayList<>();
            for (TaxonomyClassificationItem candicate : speciesCandicates) {
                TaxonomyClassificationOutput taxonomyClassificationOutput = new TaxonomyClassificationOutput(
                        candicate.getTaxid(),
                        candicate.getScientificName(),
                        candicate.getTaxid(),
                        candicate.getScientificName(),
                        candicate.getPercentage());

                taxonomyClassificationOutputs.add(taxonomyClassificationOutput);
            }

            return OK(new TaxonomyStageOutput(candicates,
                    candicates.stream().filter(s -> s.getTaxId() == bestSpecies.getTaxid()).findAny().orElse(null),
                    Constants.TaxonomyClassification.STATUS_CONFIDENT,
                    Constants.TaxonomyClassification.EVIDENCE_KRAKEN2), stageExecutionInput);
        }

        familyLevelList.sort((t1, t2) -> {
            return Double.compare(t2.getPercentage(), t1.getPercentage());
        });

        TaxonomyClassificationItem bestFamily = familyLevelList.get(0);

        if (bestFamily.getPercentage() >= KRAKEN2_FAMILY_ROUTE_THRESHOLD) {

            List<Integer> queryFamilies = familyLevelList.stream()
                    .filter(f -> f.getPercentage() / bestFamily.getPercentage() >= 0.85)
                    .map(TaxonomyClassificationItem::getTaxid).toList();

            int familyTaxId = bestFamily.getTaxid();
            // out panel. However, it is almost impossible since we use built db to run and
            // the result should be located the range we made in the db.
            if (false || !taxonomyClassificationService.isSupported(familyTaxId,
                    TaxonomyClassificationService.SUPPORT_QUERY_FAMILY_LEVEL)) {
                for (TaxonomyClassificationItem taxonomyClassificationItem : kraken2ClassificationItemList) {
                    if (TaxonomyClassificationService.RANK_CODE_SPECIES
                            .equals(taxonomyClassificationItem.getRankCode())) {
                        TaxonomyClassificationOutput taxonomyClassificationOutput = new TaxonomyClassificationOutput(
                                taxonomyClassificationItem.getTaxid(),
                                taxonomyClassificationItem.getScientificName(),
                                taxonomyClassificationItem.getTaxid(),
                                taxonomyClassificationItem.getScientificName(),
                                taxonomyClassificationItem.getPercentage());

                        candicates.add(taxonomyClassificationOutput);
                    }
                    TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput(candicates, null,
                            Constants.TaxonomyClassification.STATUS_OUT_PANEL,
                            Constants.TaxonomyClassification.EVIDENCE_KRAKEN2);
                    return OK(taxonomyStageOutput, stageExecutionInput);
                }
            }

            List<TaxonomyClassificationItem> fastANIClassificationCandicates = null;
            try {
                fastANIClassificationCandicates = doClassifyByFastANI(stageExecutionInput, r1Path, queryFamilies);
            } catch (IOException | ReportParseException e) {
                logger.error(
                        "Failed to classify taxonomy by FastANI. stage={}, queryPath={}, queryFamilies={}, workDir={}",
                        bioPipelineStage,
                        r1Path == null ? null : r1Path.toAbsolutePath(),
                        queryFamilies,
                        workDirPath == null ? null : workDirPath.toAbsolutePath(),
                        e);

                return this.runFail(
                        bioPipelineStage,
                        "FastANI classification failed",
                        workDirPath);
            }

            if (fastANIClassificationCandicates == null || fastANIClassificationCandicates.isEmpty()) {
                logger.warn(
                        "FastANI finished but no classification candidates were found. Use kraken2 result instead.  stage={}, queryPath={}, queryFamilies={}, workDir={}",
                        bioPipelineStage,
                        r1Path == null ? null : r1Path.toAbsolutePath(),
                        queryFamilies,
                        workDirPath == null ? null : workDirPath.toAbsolutePath());

                candicates = buildTaxonomyClassificationOutputFromClassificationItem(kraken2ClassificationItemList);
                TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput();
                taxonomyStageOutput.setCandicates(candicates);
                taxonomyStageOutput.setComfirmedTaxonomy(null);
                taxonomyStageOutput.setStatus(Constants.TaxonomyClassification.STATUS_LOW_CONFIDENCE);
                taxonomyStageOutput.setEvidenceResource(Constants.TaxonomyClassification.EVIDENCE_KRAKEN2);

                return OK(taxonomyStageOutput, stageExecutionInput);
            }

            Map<Integer, List<TaxonomyClassificationItem>> groupedClassificationItems = fastANIClassificationCandicates
                    .stream().collect(
                            Collectors.groupingBy(
                                    TaxonomyClassificationItem::getTaxid));

            double top1 = -1;
            int top1TaxId = -1;

            double top2 = -1;
            int top2TaxId = -1;

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
                    top2TaxId = top1TaxId;
                    top1TaxId = entry.getKey();
                    top1 = max;
                } else if (max > top2) {
                    top2 = max;
                    top2TaxId = entry.getKey();
                }
            }

            TaxonomyStageOutput taxonomyStageOutput = new TaxonomyStageOutput();
            taxonomyStageOutput.setCandicates(candicates);
            taxonomyStageOutput.setEvidenceResource(TaxonomyClassification.EVIDENCE_FASTANI);

            if (top1 < FASTANI_CONFIDENT_THRESHOLD) {
                taxonomyStageOutput.setStatus(TaxonomyClassification.STATUS_LOW_CONFIDENCE);

                return OK(taxonomyStageOutput, stageExecutionInput);
            }

            if (top1 / (CLASSIFICATION_COMPARISON_OFFSET + Math.max(top2, 0)) < 2) {
                taxonomyStageOutput.setStatus(TaxonomyClassification.STATUS_AMBIGUOUS);
                return OK(taxonomyStageOutput, stageExecutionInput);
            }

            taxonomyStageOutput.setStatus(TaxonomyClassification.STATUS_CONFIDENT);

            for (TaxonomyClassificationOutput taxonomyClassificationOutput : candicates) {
                if (taxonomyClassificationOutput.getTaxId() == top1TaxId) {
                    taxonomyStageOutput.setComfirmedTaxonomy(taxonomyClassificationOutput);
                    break;
                }
            }
            return OK(taxonomyStageOutput, stageExecutionInput);
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

        return OK(taxonomyStageOutput, stageExecutionInput);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_TAXONOMY;
    }

}
