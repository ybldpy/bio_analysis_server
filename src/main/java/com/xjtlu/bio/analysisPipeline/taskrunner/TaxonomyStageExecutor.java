package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.service.TaxonomyClassificationService;
import com.xjtlu.bio.analysisPipeline.service.TaxonomyClassificationService.FastANIMeta;
import com.xjtlu.bio.analysisPipeline.service.TaxonomyClassificationService.ReportParseException;
import com.xjtlu.bio.analysisPipeline.service.TaxonomyClassificationService.TaxonomyClassificationItem;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput.TaxonomyClassificationOutput;


import jakarta.annotation.PostConstruct;
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
    //     private String id;
    //     private String name;
    //     private int taxId;
    //     private String speciesName;
    //     private int speciesTaxId;

    // }

    // private Map<String, FastANIMeta> fastANIMetaQueryMap;

    private static final double KRAKEN2_FAMILY_CONDIFENT_THRESHOLD = 0.8d;
    private static final double KRAKEN2_SPECIES_CONFIDENT_DIRECT_PASS_THRESHOLD = 0.9d;

    private static final double CLASSIFICATION_OFFSET = 0.001d;

    @Override
    protected Class<TaxonomyStageInputUrls> stageInputType() {
        return TaxonomyStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        return BaseStageParams.class;
    }

    // private void initFastANIMeta() {

    //     supportedFamilys = new HashSet<>();
    //     fastANIMetaQueryMap = new HashMap<>();

    //     if (fastANIDB == null || fastANIDB.isBlank()) {
    //         this.logger.error("FastANI DB path is empty. Skip loading FastANI metadata.");
    //         return;
    //     }

    //     Path fastANIDBPath = Path.of(fastANIDB);
    //     Path metaDirPath = fastANIDBPath.resolve("meta");

    //     if (!Files.exists(metaDirPath)) {
    //         this.logger.error("FastANI meta directory does not exist: {}", metaDirPath.toAbsolutePath());
    //         return;
    //     }

    //     if (!Files.isDirectory(metaDirPath)) {
    //         this.logger.error("FastANI meta path is not a directory: {}", metaDirPath.toAbsolutePath());
    //         return;
    //     }

    //     String[] refsQueryListFiles = metaDirPath.toFile().list();

    //     if (refsQueryListFiles == null) {
    //         this.logger.error(
    //                 "Failed to list FastANI meta directory. Please check permission. path={}",
    //                 metaDirPath.toAbsolutePath());
    //         return;
    //     }

    //     Pattern pattern = Pattern.compile("^refs_(\\d+)\\.txt$");

    //     for (String fname : refsQueryListFiles) {
    //         Matcher matcher = pattern.matcher(fname);

    //         if (!matcher.matches()) {
    //             continue;
    //         }

    //         int familyId = Integer.parseInt(matcher.group(1));
    //         supportedFamilys.add(familyId);
    //     }

    //     Path metaDataPath = metaDirPath.resolve("metaData.tsv");

    //     if (!Files.exists(metaDataPath)) {
    //         this.logger.error("FastANI metadata file does not exist: {}", metaDataPath.toAbsolutePath());
    //         return;
    //     }

    //     if (!Files.isRegularFile(metaDataPath)) {
    //         this.logger.error("FastANI metadata path is not a regular file: {}", metaDataPath.toAbsolutePath());
    //         return;
    //     }

    //     try (BufferedReader bufferedReader = Files.newBufferedReader(metaDataPath)) {
    //         String header = bufferedReader.readLine().strip();
    //         // Map<String, Integer> headerNameIndexMap = new HashMap<>();
    //         // String[] headerParts = header.split("\t");
    //         // for(int i = 0;i<headerParts.length;i++){
    //         // headerParts[i] = headerParts[i].strip();
    //         // }

    //         String line = null;
    //         while ((line = bufferedReader.readLine()) != null) {
    //             if (StringUtils.isBlank(line)) {
    //                 continue;
    //             }

    //             String[] metaRow = line.strip().split("\t");
    //             FastANIMeta fastANIMeta = new FastANIMeta();
    //             fastANIMeta.id = metaRow[0];
    //             fastANIMeta.name = metaRow[1];
    //             fastANIMeta.taxId = Integer.parseInt(metaRow[2]);
    //             fastANIMeta.speciesName = metaRow[3];
    //             fastANIMeta.speciesTaxId = Integer.parseInt(metaRow[4]);
    //             fastANIMetaQueryMap.put(fastANIMeta.id, fastANIMeta);
    //         }

    //         loadFastANIDBMetaSuccess = true;

    //     } catch (Exception e) {

    //     }

    // }

    // @PostConstruct
    // public void init() {

    //     try {
    //         initFastANIMeta();
    //     } catch (Exception e) {
    //         // TODO: print a log
    //         logger.error("Failed to initialize FastANI metadata.", e);

    //     }
    // }



    private List<TaxonomyClassificationItem> doClassifyByFastANI(StageExecutionInput stageExecutionInput, int familyId, Path queryPath) throws IOException, ReportParseException{

        List<String> fastANIRefRealPathList = taxonomyClassificationService.getfastANIReferenceAccessionPaths(familyId);
        if(fastANIRefRealPathList == null || fastANIRefRealPathList.isEmpty()){

            //TODO: do log here
            return null;
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
        if(!executeResult.success()){
            //TODO: do log here
        }


        List<TaxonomyClassificationItem> results = taxonomyClassificationService.parseFastANIReport(outPath);


        return results;
    }

    @Override
    protected StageRunResult<TaxonomyStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {

        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        Path inputDirPath = stageExecutionInput.inputDir;
        Path workDirPath = stageExecutionInput.workDir;

        // Map<String, String> inputMap = JsonUtil.toMap(bioPipelineStage.getInputUrl(),
        // String.class);
        TaxonomyStageInputUrls taxonomyStageInputUrls = stageExecutionInput.input;

        String r1Url = taxonomyStageInputUrls.getR1();
        // String r2Url = taxonomyStageInputUrls.getR2();

        Path r1Path = inputDirPath.resolve("r1.fastq");
        // Path r2Path = StringUtils.isBlank(r2Url) ? null :
        // inputDirPath.resolve("r2.fastq");

        // if (r2Path == null) {
        // this.loadInput(Map.of(r1Url, r1Path));
        // } else {
        // this.loadInput(Map.of(r1Url, r1Path, r2Url, r2Path));
        // }

        this.loadInput(Map.of(r1Url, r1Path));

        Path reportPath = workDirPath.resolve("taxonomny.report");
        Path outputPath = workDirPath.resolve("taxonomy.output");

        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getKraken2());
        runCmd.add("--db");
        runCmd.add(taxonomyClassificationService.getKraken2DB());
        runCmd.add(r1Path.toString());
        // if (r2Path != null) {
        // runCmd.add(r2Path.toString());
        // runCmd.add("--paired");
        // }
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

        familyLevelList.sort((t1, t2) -> {
            return Double.compare(t2.getPercentage(), t1.getPercentage());
        });

        TaxonomyClassificationItem best = familyLevelList.get(0);
        TaxonomyClassificationItem secondaryBest = familyLevelList.size() > 2 ? familyLevelList.get(1) : null;

        List<TaxonomyClassificationOutput> candicates = new ArrayList<>();

        if (best.getPercentage() >= KRAKEN2_FAMILY_CONDIFENT_THRESHOLD
                && best.getPercentage() / (CLASSIFICATION_OFFSET + secondaryBest.getPercentage()) >= 2) {
            // confident

            speciesCandicates.sort((t1, t2) -> {
                return Double.compare(t2.getPercentage(), t1.getPercentage());
            });

            TaxonomyClassificationItem bestSpecies = speciesCandicates.get(0);
            if (bestSpecies.getPercentage() >= KRAKEN2_SPECIES_CONFIDENT_DIRECT_PASS_THRESHOLD
                    && (speciesCandicates.size() < 2
                            || bestSpecies.getPercentage()
                                    / (CLASSIFICATION_OFFSET + speciesCandicates.get(1).getPercentage()) >= 2)) {

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

            int familyTaxId = best.getTaxid();
            // out panel. However, it is almost impossible since we use built db to run and
            // the result should be located the range we made in the db.
            if (!taxonomyClassificationService.isSupported(familyTaxId, TaxonomyClassificationService.SUPPORT_QUERY_FAMILY_LEVEL)) {
                for (TaxonomyClassificationItem taxonomyClassificationItem : kraken2ClassificationItemList) {
                    if (TaxonomyClassificationService.RANK_CODE_SPECIES.equals(taxonomyClassificationItem.getRankCode())) {
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



        }

        // return OK(new TaxonomyStageOutput(outputPath, reportPath),
        // stageExecutionInput);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_TAXONOMY;
    }

}
