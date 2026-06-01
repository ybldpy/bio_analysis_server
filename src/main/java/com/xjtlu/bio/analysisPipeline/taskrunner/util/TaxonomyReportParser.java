// package com.xjtlu.bio.analysisPipeline.taskrunner.util;

// import java.io.BufferedReader;
// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.util.ArrayList;
// import java.util.List;

// public class TaxonomyReportParser {

//     public static class ReportParseException extends Exception {

//         public ReportParseException(String message) {
//             super(message);
//         }

//         public ReportParseException(String message, Throwable cause) {
//             super(message, cause);
//         }
//     }

//     public static class TaxonomyClassificationItem {

//         public static final String RANK_CODE_UNCLASSIFIED = "U";
//         public static final String RANK_CODE_ROOT = "R";
//         public static final String RANK_CODE_DOMAIN = "D";
//         public static final String RANK_CODE_KINGDOM = "K";
//         public static final String RANK_CODE_PHYLUM = "P";
//         public static final String RANK_CODE_CLASS = "C";
//         public static final String RANK_CODE_ORDER = "O";
//         public static final String RANK_CODE_FAMILY = "F";
//         public static final String RANK_CODE_GENUS = "G";
//         public static final String RANK_CODE_SPECIES = "S";

//         private double percentage;
//         private long cladeReads;
//         private long directReads;
//         private String rankCode;
//         private int taxid;
//         private String scientificName;

//         public TaxonomyClassificationItem() {
//         }

//         public TaxonomyClassificationItem(
//                 double percentage,
//                 long cladeReads,
//                 long directReads,
//                 String rankCode,
//                 int taxid,
//                 String scientificName) {
//             this.percentage = percentage;
//             this.cladeReads = cladeReads;
//             this.directReads = directReads;
//             this.rankCode = rankCode;
//             this.taxid = taxid;
//             this.scientificName = scientificName;
//         }

//         public double getPercentage() {
//             return percentage;
//         }

//         public void setPercentage(double percentage) {
//             this.percentage = percentage;
//         }

//         public long getCladeReads() {
//             return cladeReads;
//         }

//         public void setCladeReads(long cladeReads) {
//             this.cladeReads = cladeReads;
//         }

//         public long getDirectReads() {
//             return directReads;
//         }

//         public void setDirectReads(long directReads) {
//             this.directReads = directReads;
//         }

//         public String getRankCode() {
//             return rankCode;
//         }

//         public void setRankCode(String rankCode) {
//             this.rankCode = rankCode;
//         }

//         public int getTaxid() {
//             return taxid;
//         }

//         public void setTaxid(int taxid) {
//             this.taxid = taxid;
//         }

//         public String getScientificName() {
//             return scientificName;
//         }

//         public void setScientificName(String scientificName) {
//             this.scientificName = scientificName;
//         }
//     }



    



    

//     public static List<TaxonomyClassificationItem> parseKraken2Report(Path report) throws ReportParseException {

//         List<TaxonomyClassificationItem> items = new ArrayList<>();
//         try (BufferedReader buffer = Files.newBufferedReader(report)) {
//             // String header = buffer.readLine();

//             String line = null;
//             while ((line = buffer.readLine()) != null) {

//                 if (line.isBlank()) {
//                     continue;
//                 }

//                 // 兼容 kraken2-inspect 那种以 # 开头的说明行
//                 if (line.startsWith("#")) {
//                     continue;
//                 }

//                 String[] cols = line.strip().split("\t", 6);
//                 TaxonomyClassificationItem taxonomyClassificationItem = new TaxonomyClassificationItem(
//                         Double.parseDouble(cols[0].strip()),
//                         Long.parseLong(cols[1].strip()),
//                         Long.parseLong(cols[2].strip()),
//                         cols[3].strip(),
//                         Integer.parseInt(cols[4].strip()),
//                         cols[5].strip());
//                 items.add(taxonomyClassificationItem);
//             }

//         } catch (IOException e) {
//             throw new ReportParseException(
//                     "Failed to read Kraken2 report file: " + report.toAbsolutePath(),
//                     e);
//         }
//         return items;
//     }

// }
