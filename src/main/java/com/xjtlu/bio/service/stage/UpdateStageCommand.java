package com.xjtlu.bio.service.stage;

import com.xjtlu.bio.entity.BioPipelineStage;

public class UpdateStageCommand {

    private final BioPipelineStage updateStage;
    private final long stageId;
    private final int currentVersion;
    public UpdateStageCommand(BioPipelineStage updateStage, long stageId, int currentVersion) {
        this.updateStage = updateStage;
        this.stageId = stageId;
        this.currentVersion = currentVersion;
    }
    public BioPipelineStage getUpdateStage() {
        return updateStage;
    }
    public long getStageId() {
        return stageId;
    }
    public int getCurrentVersion() {
        return currentVersion;
    }
    


}
