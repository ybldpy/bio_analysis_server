package com.xjtlu.bio.analysisPipeline.service;

import java.io.BufferedReader;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

@Service
public class TaxonomyClassificationService {

    public static class ReportParseException extends Exception {

        public ReportParseException(String message) {
            super(message);
        }

        public ReportParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final String RANK_CODE_UNCLASSIFIED = "U";
    public static final String RANK_CODE_ROOT = "R";
    public static final String RANK_CODE_DOMAIN = "D";
    public static final String RANK_CODE_KINGDOM = "K";
    public static final String RANK_CODE_PHYLUM = "P";
    public static final String RANK_CODE_CLASS = "C";
    public static final String RANK_CODE_ORDER = "O";
    public static final String RANK_CODE_FAMILY = "F";
    public static final String RANK_CODE_GENUS = "G";
    public static final String RANK_CODE_SPECIES = "S";



    public static final int SUPPORT_QUERY_FAMILY_LEVEL = 0;

    public static class TaxonomyClassificationItem {

        private double percentage;
        private long cladeReads;
        private long directReads;
        private String rankCode;
        private int taxid;
        private String scientificName;

        public TaxonomyClassificationItem() {
        }

        public TaxonomyClassificationItem(
                double percentage,
                long cladeReads,
                long directReads,
                String rankCode,
                int taxid,
                String scientificName) {
            this.percentage = percentage;
            this.cladeReads = cladeReads;
            this.directReads = directReads;
            this.rankCode = rankCode;
            this.taxid = taxid;
            this.scientificName = scientificName;
        }

        public double getPercentage() {
            return percentage;
        }

        public void setPercentage(double percentage) {
            this.percentage = percentage;
        }

        public long getCladeReads() {
            return cladeReads;
        }

        public void setCladeReads(long cladeReads) {
            this.cladeReads = cladeReads;
        }

        public long getDirectReads() {
            return directReads;
        }

        public void setDirectReads(long directReads) {
            this.directReads = directReads;
        }

        public String getRankCode() {
            return rankCode;
        }

        public void setRankCode(String rankCode) {
            this.rankCode = rankCode;
        }

        public int getTaxid() {
            return taxid;
        }

        public void setTaxid(int taxid) {
            this.taxid = taxid;
        }

        public String getScientificName() {
            return scientificName;
        }

        public void setScientificName(String scientificName) {
            this.scientificName = scientificName;
        }
    }




    public List<TaxonomyClassificationItem> parseFastANIReport(Path report) throws ReportParseException{

        List<TaxonomyClassificationItem> items = new ArrayList<>();
        try(BufferedReader bufferedReader = Files.newBufferedReader(report)){

            String line = null;
            while((line = bufferedReader.readLine())!=null){
                String stripdLine = line.strip();
                String[] cols = stripdLine.split("\t");
                String referenceGenome = cols[1].substring(cols[1].lastIndexOf("/")+1);
                referenceGenome = referenceGenome.substring(0, referenceGenome.length()-".fna".length());
                double percentage = Double.parseDouble(cols[2]);
                int matchedFragments = Integer.parseInt(cols[3]);
                int totalFragments = Integer.parseInt(cols[4]);
                FastANIMeta correspondingAniMeta = this.fastANIMetaQueryMap.get(referenceGenome);

                TaxonomyClassificationItem  item = new TaxonomyClassificationItem(
                    percentage,
                    matchedFragments,
                    totalFragments,
                    RANK_CODE_SPECIES,
                    correspondingAniMeta.taxId,
                    correspondingAniMeta.name
                );
                items.add(item);
            }
            return items;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            throw new ReportParseException(
                    "Failed to read fastANI report file: " + report.toAbsolutePath(),
                    e);
        }

        

    }

    public List<TaxonomyClassificationItem> parseKraken2Report(Path report) throws ReportParseException {

        List<TaxonomyClassificationItem> items = new ArrayList<>();
        try (BufferedReader buffer = Files.newBufferedReader(report)) {

            String line = null;
            while ((line = buffer.readLine()) != null) {

                if (line.isBlank()) {
                    continue;
                }

                // 兼容 kraken2-inspect 那种以 # 开头的说明行
                if (line.startsWith("#")) {
                    continue;
                }

                String[] cols = line.strip().split("\t", 6);
                TaxonomyClassificationItem taxonomyClassificationItem = new TaxonomyClassificationItem(
                        Double.parseDouble(cols[0].strip()),
                        Long.parseLong(cols[1].strip()),
                        Long.parseLong(cols[2].strip()),
                        cols[3].strip(),
                        Integer.parseInt(cols[4].strip()),
                        cols[5].strip());
                items.add(taxonomyClassificationItem);
            }

        } catch (IOException e) {
            throw new ReportParseException(
                    "Failed to read Kraken2 report file: " + report.toAbsolutePath(),
                    e);
        }
        return items;
    }

    private static final Logger logger = LoggerFactory.getLogger(TaxonomyClassificationService.class);

    public String getKraken2DB() {
        return kraken2DB;
    }

    public void setKraken2DB(String kraken2db) {
        kraken2DB = kraken2db;
    }

    public String getFastANIDB() {
        return fastANIDB;
    }

    public void setFastANIDB(String fastANIDB) {
        this.fastANIDB = fastANIDB;
    }



    public boolean isSupported(int queryId, int level){

        return supportedFamilys.contains(queryId);


    }

    public static class FastANIMeta {
        private String id;
        private String name;
        private int taxId;
        private String speciesName;
        private int speciesTaxId;
    }

    @Value("${analysis-pipeline.stage.taxonomy.kraken2DB}")
    private String kraken2DB;

    @Value("${analysis-pipeline.stage.taxonomy.fastANIDB}")
    private String fastANIDB;

    private Map<String, FastANIMeta> fastANIMetaQueryMap;
    private Set<Integer> supportedFamilys;

    private boolean loadFastANIDBMetaSuccess;

    public boolean isServiceOk() {
        return this.loadFastANIDBMetaSuccess;
    }

    public FastANIMeta queryFastANIAccessionMeta(String accession) {
        return this.fastANIMetaQueryMap.get(accession);
    }

    public List<String> getfastANIReferenceAccessionPaths(int familyId) throws IOException {

        // TODO: able to use cache to avoid repeatable read here
        Path fastANIMetaPath = Path.of(fastANIDB, "meta", "refs_" + familyId + ".txt");
        Path fastANIRefAccessionPath = Path.of(fastANIDB, "accessions");
        List<String> realPathLists = new ArrayList<>();
        try (BufferedReader bf = Files.newBufferedReader(fastANIMetaPath)) {
            String line = null;
            while ((line = bf.readLine()) != null) {
                String stripedLine = line.strip();
                realPathLists.add(fastANIRefAccessionPath.resolve(stripedLine + ".fna").toString());
            }

            return realPathLists;
        }
    }

    private void initFastANIMeta() {

        supportedFamilys = new HashSet<>();
        fastANIMetaQueryMap = new HashMap<>();

        if (fastANIDB == null || fastANIDB.isBlank()) {
            this.logger.error("FastANI DB path is empty. Skip loading FastANI metadata.");
            return;
        }

        Path fastANIDBPath = Path.of(fastANIDB);
        Path metaDirPath = fastANIDBPath.resolve("meta");

        if (!Files.exists(metaDirPath)) {
            this.logger.error("FastANI meta directory does not exist: {}", metaDirPath.toAbsolutePath());
            return;
        }

        if (!Files.isDirectory(metaDirPath)) {
            this.logger.error("FastANI meta path is not a directory: {}", metaDirPath.toAbsolutePath());
            return;
        }

        String[] refsQueryListFiles = metaDirPath.toFile().list();

        if (refsQueryListFiles == null) {
            this.logger.error(
                    "Failed to list FastANI meta directory. Please check permission. path={}",
                    metaDirPath.toAbsolutePath());
            return;
        }

        Pattern pattern = Pattern.compile("^refs_(\\d+)\\.txt$");

        for (String fname : refsQueryListFiles) {
            Matcher matcher = pattern.matcher(fname);

            if (!matcher.matches()) {
                continue;
            }

            int familyId = Integer.parseInt(matcher.group(1));
            supportedFamilys.add(familyId);
        }

        Path metaDataPath = metaDirPath.resolve("metaData.tsv");

        if (!Files.exists(metaDataPath)) {
            this.logger.error("FastANI metadata file does not exist: {}", metaDataPath.toAbsolutePath());
            return;
        }

        if (!Files.isRegularFile(metaDataPath)) {
            this.logger.error("FastANI metadata path is not a regular file: {}", metaDataPath.toAbsolutePath());
            return;
        }

        try (BufferedReader bufferedReader = Files.newBufferedReader(metaDataPath)) {
            String header = bufferedReader.readLine().strip();
            // Map<String, Integer> headerNameIndexMap = new HashMap<>();
            // String[] headerParts = header.split("\t");
            // for(int i = 0;i<headerParts.length;i++){
            // headerParts[i] = headerParts[i].strip();
            // }

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if (StringUtils.isBlank(line)) {
                    continue;
                }

                String[] metaRow = line.strip().split("\t");
                FastANIMeta fastANIMeta = new FastANIMeta();
                fastANIMeta.id = metaRow[0];
                fastANIMeta.name = metaRow[1];
                fastANIMeta.taxId = Integer.parseInt(metaRow[2]);
                fastANIMeta.speciesName = metaRow[3];
                fastANIMeta.speciesTaxId = Integer.parseInt(metaRow[4]);
                fastANIMetaQueryMap.put(fastANIMeta.id, fastANIMeta);
            }

            loadFastANIDBMetaSuccess = true;

        } catch (Exception e) {

            this.logger.error(
                    "Failed to load FastANI metadata. fastANIDB={}, metaDataPath={}",
                    fastANIDB,
                    metaDataPath == null ? null : metaDataPath.toAbsolutePath(),
                    e
            );
        }

    }

    @PostConstruct
    public void init() {

        try {
            initFastANIMeta();
        } catch (Exception e) {
            // TODO: print a log
            logger.error("Failed to initialize FastANI metadata.", e);

        }
    }

}
