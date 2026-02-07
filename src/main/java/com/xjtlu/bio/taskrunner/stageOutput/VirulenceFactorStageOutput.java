package com.xjtlu.bio.taskrunner.stageOutput;

import java.nio.file.Path;

public class VirulenceFactorStageOutput implements StageOutput{

    private Path vfResult;

    public Path getVfResult() {
        return vfResult;
    }

    public VirulenceFactorStageOutput(Path vfResult) {
        this.vfResult = vfResult;
    }

    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return Path.of(vfResult).getParent();
    }
}
