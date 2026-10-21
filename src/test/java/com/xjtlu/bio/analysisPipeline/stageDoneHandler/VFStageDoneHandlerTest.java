package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.VFResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VirulenceFactorStageOutput;

class VFStageDoneHandlerTest {

    @Test
    void uploadsReportUnderVfFilenameAndExposesMatchingVfTsvUrl() {
        TestHandler handler = new TestHandler();
        Path localReport = Path.of("/work/run.v1/sample.v2.tsv");
        VirulenceFactorStageOutput output = new VirulenceFactorStageOutput(localReport);

        Pair<Map<String, String>, VFResult> plan = handler.plan(output);

        assertEquals(Map.of(localReport.toString(), "stored/vf.tsv"), plan.getLeft());
        assertEquals("stored/vf.tsv", plan.getRight().getVfTsv());
    }

    private static final class TestHandler extends VFStageDoneHandler {

        private Pair<Map<String, String>, VFResult> plan(VirulenceFactorStageOutput output) {
            StageRunResult<VirulenceFactorStageOutput> result = StageRunResult.OK(
                    output, new StageContext(1L, 0, getType(), 1), Path.of("/work/run.v1"));
            return buildUploadConfigAndOutputUrlMap(result);
        }

        @Override
        protected String createStoreObjectName(StageContext stageContext, String name) {
            return "stored/" + name;
        }
    }
}
