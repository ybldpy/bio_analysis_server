package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.meta.ReadMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;

@Component
public class ReadInspectStageExecutor
        extends AbstractPipelineStageExector<ReadInspectStageOutput, ReadInspectStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<ReadInspectStageOutput> {

    @Override
    protected Class<ReadInspectStageInputUrls> stageInputType() {
        // TODO Auto-generated method stub
        return ReadInspectStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        // TODO Auto-generated method stub
        return BaseStageParams.class;
    }

    private int[] checkQualityEncodingAndReadLen(Path p) throws IOException {

        int encoding = ReadMeta.QUALITY_ENCODING_33;
        int readLenType = ReadMeta.READ_LEN_TYPE_SHORT;

        int minQual = Integer.MAX_VALUE;
        int maxQual = Integer.MIN_VALUE;

        int totalLen = 0;
        int count = 0;

        try (BufferedReader br = Files.newBufferedReader(p)) {

            String line;
            int lineIndex = 0;

            while ((line = br.readLine()) != null && count < 1000) {

                int mod = lineIndex % 4;

                // sequence line
                if (mod == 1) {
                    totalLen += line.length();
                    count++;
                }

                // quality line
                if (mod == 3) {
                    for (char c : line.toCharArray()) {
                        int v = (int) c;
                        minQual = Math.min(minQual, v);
                        maxQual = Math.max(maxQual, v);
                    }
                }

                lineIndex++;
            }
        }

        // 判断 encoding
        if (minQual >= 33 && maxQual <= 74) {
            encoding = ReadMeta.QUALITY_ENCODING_33;
        } else if (minQual >= 64) {
            encoding = ReadMeta.QUALITY_ENCODING_64;
        }

        // 判断 read length
        if (count > 0) {
            int avgLen = totalLen / count;

            if (avgLen >= 500) {
                readLenType = ReadMeta.READ_LEN_TYPE_LONG;
            } else {
                readLenType = ReadMeta.READ_LEN_TYPE_SHORT;
            }
        }

        return new int[] { encoding, readLenType };
    }

    @Override
    protected StageRunResult<ReadInspectStageOutput> _execute(
            StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {
        // TODO Auto-generated method stub

        ReadInspectStageInputUrls readInspectStageInputUrls = stageExecutionInput.input;

        String readUrl = readInspectStageInputUrls.getReadUrl();
        Path readLocalPath = stageExecutionInput.inputDir.resolve(readUrl.substring(readUrl.lastIndexOf("/") + 1));

        Map<String, Path> loadMap = Map.of(readUrl, readLocalPath);
        loadInput(loadMap);

        try {
            int[] result = checkQualityEncodingAndReadLen(readLocalPath);

            int encoding = result[0];
            int readLEen = result[1];

            return StageRunResult.OK(new ReadInspectStageOutput(encoding, readLEen), stageExecutionInput.stageContext);

        } catch (IOException e) {

            logger.error("stage = {} exception", stageExecutionInput.stageContext.getRunStageId(), e);
            return this.runFail(stageExecutionInput.stageContext, "run exception");
        }

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
    }

}
