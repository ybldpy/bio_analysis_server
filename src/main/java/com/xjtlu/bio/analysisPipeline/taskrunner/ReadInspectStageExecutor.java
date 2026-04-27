package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.ReadMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;

class FastQIO {

    public static boolean isGzip(Path p) {
        String fileName = p.getFileName().toString();
        return fileName.endsWith(".gz");
    }

    public static BufferedReader getReader(Path in) throws IOException {

        InputStream is = Files.newInputStream(in);

        if (isGzip(in)) {
            is = new GZIPInputStream(is);
        }

        return new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    public static BufferedWriter getWriter(Path out) throws IOException {

        OutputStream os = Files.newOutputStream(out);

        if (isGzip(out)) {
            os = new GZIPOutputStream(os);
        }

        return new BufferedWriter(
                new OutputStreamWriter(os));
    }
}

@Component
public class ReadInspectStageExecutor
        extends AbstractPipelineStageExector<ReadInspectStageOutput, ReadInspectStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<ReadInspectStageOutput> {

    private static final double IS_INTERLEAVED_RATIO = 0.9;
    private static final double NON_INTERLEAVED_MAX_RATIO = 0.1;

    private static final int LONG_READ_THRESHOLD = 500;

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

    private static String substractRecordId(String header) {
        // 去掉开头 '@'
        String id = header.charAt(0) == '@' ? header.substring(1) : header;
        // 只取空格前（兼容 Illumina / SRA）
        int spaceIdx = id.indexOf(' ');
        if (spaceIdx > 0) {
            id = id.substring(0, spaceIdx);
        }
        // 处理 /1 /2（老式 Illumina）
        if (id.endsWith("/1") || id.endsWith("/2")) {
            id = id.substring(0, id.length() - 2);
        }

        // 处理 .1 .2（常见 SRA/转换格式）
        int lastDot = id.lastIndexOf('.');
        if (lastDot > 0) {
            String suffix = id.substring(lastDot + 1);
            if ("1".equals(suffix) || "2".equals(suffix)) {
                id = id.substring(0, lastDot);
            }
        }

        return id;
    }

    private static boolean checkInterleaved(double ratio) {
        return ratio > IS_INTERLEAVED_RATIO;
    }

    private static boolean checkSingleRead(double ratio) {
        return ratio < NON_INTERLEAVED_MAX_RATIO;
    }

    private StageRunResult<ReadInspectStageOutput> inspect(Path read1, boolean possibleInterleaved,
            StageExecutionInput stageExecutionInput) {

        String fileName = read1.getFileName().toString();
        String baseName = fileName;
        String format = null;

        if (baseName.endsWith(".fastq.gz")) {
            baseName = baseName.substring(0, baseName.length() - ".fastq.gz".length());
            format = ".fastq.gz";
        } else if (baseName.endsWith(".fq.gz")) {
            baseName = baseName.substring(0, baseName.length() - ".fq.gz".length());
            format = ".fq.gz";
        } else if (baseName.endsWith(".fastq")) {
            baseName = baseName.substring(0, baseName.length() - ".fastq".length());
            format = ".fastq";
        } else if (baseName.endsWith(".fq")) {
            baseName = baseName.substring(0, baseName.length() - ".fq".length());
            format = ".fq";
        }

        int qualityEncoding = ReadMeta.QUALITY_ENCODING_33;
        int readLenType = ReadMeta.READ_LEN_TYPE_SHORT;

        Path workDir = stageExecutionInput.workDir;
        Path r1 = workDir.resolve(baseName + "_r1" + format);
        Path r2 = workDir.resolve(baseName + "_r2" + format);

        boolean checkedLen = false;
        int checkReadLenTypePoint = 1000;
        int[] readLens = new int[checkReadLenTypePoint];
        int recordLenRecordIndex = 0;

        try (BufferedReader br = FastQIO.getReader(read1);
                BufferedWriter w1 = FastQIO.getWriter(r1);
                BufferedWriter w2 = FastQIO.getWriter(r2)) {

            int recordTravered = 0;
            int paired = 0;

            int checkPointRecordNum = 4000;

            String[][] recordBuffer = new String[2][4];

            int recordBufferIndex = 0;
            String r1HeaderId = null;

            boolean checkedInterleaved = false;
            boolean isInterleaved = false;

            while (true) {
                String header = br.readLine();
                if (header == null) {
                    break;
                }

                if (possibleInterleaved) {
                    recordBuffer[recordBufferIndex][0] = header;
                }

                for (int i = 0; i < 3; i++) {
                    String followingLine = br.readLine();
                    if (followingLine == null) {
                        return this.runFail(stageExecutionInput.stageContext, followingLine);
                    }

                    if (possibleInterleaved) {
                        recordBuffer[recordBufferIndex][1 + i] = followingLine;
                    }
                    if (!checkedLen && i == 0) {
                        int readLen = followingLine.length();
                        readLens[recordLenRecordIndex] = readLen;
                        recordLenRecordIndex++;
                        if (recordLenRecordIndex >= checkReadLenTypePoint) {
                            checkedLen = true;
                            Arrays.sort(readLens);
                            int medianLen = readLens[(readLens.length - 1) / 2];
                            if (medianLen >= LONG_READ_THRESHOLD) {
                                readLenType = ReadMeta.READ_LEN_TYPE_LONG;
                            }
                        }
                    }
                }

                if (possibleInterleaved) {
                    if (recordBufferIndex == 0) {
                        r1HeaderId = substractRecordId(header);

                    } else {
                        String r2HeaderId = substractRecordId(header);
                        if (Objects.equals(r1HeaderId, r2HeaderId)) {
                            String r1Record = String.join("\n", recordBuffer[0]);
                            String r2Record = String.join("\n", recordBuffer[1]);
                            w1.write(r1Record);
                            w1.newLine();
                            w2.write(r2Record);
                            w2.newLine();
                            paired++;
                        } else {
                            // not paired with previous, may be read 1 of next pair
                            recordBuffer[0] = recordBuffer[1];
                            recordBufferIndex = 0;
                        }
                    }
                }

                if (possibleInterleaved) {
                    recordBufferIndex = (recordBufferIndex + 1) % 2;
                    recordTravered += 1;
                }

                if (possibleInterleaved && !checkedInterleaved && recordTravered >= checkPointRecordNum) {
                    checkedInterleaved = true;
                    double ratio = (paired * 2.0) / recordTravered;
                    isInterleaved = checkInterleaved(ratio);
                    if (!isInterleaved) {
                        if (checkSingleRead(ratio)) {
                            return StageRunResult.OK(
                                    new ReadInspectStageOutput(qualityEncoding, readLenType, null, null, stageExecutionInput.workDir),
                                    stageExecutionInput.stageContext);
                        } else {
                            logger.warn(
                                    "stage = {}, ambiguous FASTQ input, unable to determine layout, " +
                                            "records = {}, paired = {}, ratio = {}",
                                    stageExecutionInput.stageContext.getRunStageId(),
                                    recordTravered,
                                    paired,
                                    String.format("%.4f", ratio));

                            return this.runFail(stageExecutionInput.stageContext, "无法识别输入类型");
                        }
                    }
                }
            }

            if (!checkedLen) {
                Arrays.sort(readLens, 0, recordLenRecordIndex);
                int median = readLens[(recordLenRecordIndex - 1) / 2];
                if (median >= LONG_READ_THRESHOLD) {
                    readLenType = ReadMeta.READ_LEN_TYPE_LONG;
                }
            }

            if (possibleInterleaved) {

                if (checkedInterleaved) {
                    return StageRunResult.OK(new ReadInspectStageOutput(qualityEncoding, checkReadLenTypePoint, r1, r2, stageExecutionInput.workDir),
                            stageExecutionInput.stageContext);
                }
                else {
                    double ratio = (paired * 2.0) / recordTravered;
                    if (!checkInterleaved(ratio)) {
                        if (checkSingleRead(ratio)) {
                            return StageRunResult.OK(
                                    new ReadInspectStageOutput(qualityEncoding, readLenType, null, null, stageExecutionInput.workDir),
                                    stageExecutionInput.stageContext);
                        } else {
                            logger.warn(
                                    "stage = {}, ambiguous FASTQ input, unable to determine layout, " +
                                            "records = {}, paired = {}, ratio = {}",
                                    stageExecutionInput.stageContext.getRunStageId(),
                                    recordTravered,
                                    paired,
                                    String.format("%.4f", ratio));

                            return this.runFail(stageExecutionInput.stageContext, "无法识别输入类型");
                        }
                    }

                    return StageRunResult.OK(new ReadInspectStageOutput(qualityEncoding, checkReadLenTypePoint, null, null, stageExecutionInput.workDir), stageExecutionInput.stageContext);
                }
            } else {

                return StageRunResult.OK(new ReadInspectStageOutput(qualityEncoding, readLenType, null, null, stageExecutionInput.workDir),
                        stageExecutionInput.stageContext);
            }

        } catch (Exception e) {
            logger.error("Inspection exception, run stage id = {}", stageExecutionInput.stageContext.getRunStageId(),
                    e);
            return this.runFail(stageExecutionInput.stageContext, "Inspection exception");
        }

    }

    @Override
    protected StageRunResult<ReadInspectStageOutput> _execute(
            StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {
        // TODO Auto-generated method stub

        ReadInspectStageInputUrls readInspectStageInputUrls = stageExecutionInput.input;

        String read1Url = readInspectStageInputUrls.getRead1Url();
        String read2Url = readInspectStageInputUrls.getRead2Url();

        Path readLocalPath = stageExecutionInput.inputDir.resolve(read1Url.substring(read1Url.lastIndexOf("/") + 1));

        Map<String, Path> loadMap = Map.of(read1Url, readLocalPath);
        loadInput(loadMap);

        return this.inspect(readLocalPath, StringUtils.isBlank(read2Url), stageExecutionInput);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
    }

}
