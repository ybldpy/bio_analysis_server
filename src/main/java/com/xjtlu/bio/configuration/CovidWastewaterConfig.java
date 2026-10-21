package com.xjtlu.bio.configuration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Server-owned, versioned analysis profiles. No guessed preprocessing scheme is enabled.
 * preparationCommand consumes --request-json and produces the files specified in that request.
 */
@Component
@ConfigurationProperties(prefix = "analysis-pipeline.covid-wastewater")
public class CovidWastewaterConfig {
    private List<String> freyjaCommand = new ArrayList<>();
    private String configuredFreyjaVersion;
    private String defaultProfile;
    private Map<String, Profile> profiles = new LinkedHashMap<>();

    public List<String> getFreyjaCommand() { return freyjaCommand; }
    public void setFreyjaCommand(List<String> value) { freyjaCommand = value; }
    public String getConfiguredFreyjaVersion() { return configuredFreyjaVersion; }
    public void setConfiguredFreyjaVersion(String value) { configuredFreyjaVersion = value; }
    public String getDefaultProfile() { return defaultProfile; }
    public void setDefaultProfile(String value) { defaultProfile = value; }
    public Map<String, Profile> getProfiles() { return profiles; }
    public void setProfiles(Map<String, Profile> value) { profiles = value; }

    public static class Profile {
        private String version;
        private String libraryType = "AMPLICON";
        private List<String> preparationCommand = new ArrayList<>();
        private List<Integer> supportedPlatforms = new ArrayList<>();
        private String referenceFasta;
        private String barcodes;
        private String lineageHierarchy;
        private String lineageMetadata;
        private String dataVersion;

        public String getVersion() { return version; }
        public void setVersion(String value) { version = value; }
        public String getLibraryType() { return libraryType; }
        public void setLibraryType(String value) { libraryType = value; }
        public List<String> getPreparationCommand() { return preparationCommand; }
        public void setPreparationCommand(List<String> value) { preparationCommand = value; }
        public List<Integer> getSupportedPlatforms() { return supportedPlatforms; }
        public void setSupportedPlatforms(List<Integer> value) { supportedPlatforms = value; }
        public String getReferenceFasta() { return referenceFasta; }
        public void setReferenceFasta(String value) { referenceFasta = value; }
        public String getBarcodes() { return barcodes; }
        public void setBarcodes(String value) { barcodes = value; }
        public String getLineageHierarchy() { return lineageHierarchy; }
        public void setLineageHierarchy(String value) { lineageHierarchy = value; }
        public String getLineageMetadata() { return lineageMetadata; }
        public void setLineageMetadata(String value) { lineageMetadata = value; }
        public String getDataVersion() { return dataVersion; }
        public void setDataVersion(String value) { dataVersion = value; }
    }
}
