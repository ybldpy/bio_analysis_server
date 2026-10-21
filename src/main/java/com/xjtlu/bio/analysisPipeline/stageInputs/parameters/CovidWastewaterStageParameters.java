package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

/** Request-level options. Executables and server file paths must never come from this DTO. */
public class CovidWastewaterStageParameters extends BaseStageParams {
    private int threads = 2;
    private long timeoutSeconds = 7200;
    private String analysisProfile;

    public int getThreads() { return threads; }
    public void setThreads(int threads) { this.threads = threads; }
    public long getTimeoutSeconds() { return timeoutSeconds; }
    public void setTimeoutSeconds(long timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }
    public String getAnalysisProfile() { return analysisProfile; }
    public void setAnalysisProfile(String analysisProfile) { this.analysisProfile = analysisProfile; }

    public void validate() {
        if (threads < 1 || threads > 256) {
            throw new IllegalArgumentException("Wastewater threads must be between 1 and 256");
        }
        if (timeoutSeconds < 1 || timeoutSeconds > 604800) {
            throw new IllegalArgumentException("Wastewater timeoutSeconds must be between 1 and 604800");
        }
        if (analysisProfile != null && !analysisProfile.isBlank()
                && !analysisProfile.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
            throw new IllegalArgumentException("Wastewater analysisProfile must be a configured profile identifier");
        }
    }
}
