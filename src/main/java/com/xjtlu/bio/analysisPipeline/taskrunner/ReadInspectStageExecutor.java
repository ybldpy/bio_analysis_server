package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.Constants.SequenceInput;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;

// class FastQIO {

//     public static boolean isGzip(Path p) {
//         String fileName = p.getFileName().toString();
//         return fileName.endsWith(".gz");
//     }

//     public static BufferedReader getReader(Path in) throws IOException {

//         InputStream is = Files.newInputStream(in);

//         if (isGzip(in)) {
//             is = new GZIPInputStream(is);
//         }

//         return new BufferedReader(
//                 new InputStreamReader(is, StandardCharsets.UTF_8));
//     }

//     public static BufferedWriter getWriter(Path out) throws IOException {

//         OutputStream os = Files.newOutputStream(out);

//         if (isGzip(out)) {
//             os = new GZIPOutputStream(os);
//         }

//         return new BufferedWriter(
//                 new OutputStreamWriter(os));
//     }
// }

@Component
public class ReadInspectStageExecutor
        extends AbstractPipelineStageExector<ReadInspectStageOutput, ReadInspectStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<ReadInspectStageOutput> {

    private static final double IS_INTERLEAVED_RATIO = 0.9;
    private static final double NON_INTERLEAVED_MAX_RATIO = 0.1;

    private static final int LONG_READ_THRESHOLD = 500;

    @Value(value = "${analysis-pipeline.tools.bbmapPath}")
    private String bbmapPath;

    @Override
    protected Class<ReadInspectStageInputUrls> stageInputType() {
        return ReadInspectStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        return BaseStageParams.class;
    }

    private static String substractRecordId(String header) {
        // 去掉开头 '@'
        // String id = header.charAt(0) == '@' || header.charAt(0) == '>' ?
        // header.substring(1) : header;
        String id = header;
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

    private static String getFormatPostFix(String fname, int formatCode) {
        for (String fastaFormat : Constants.SequenceInput.FASTQ_FORMAT_SET) {
            if (fname.endsWith(fastaFormat)) {
                return fastaFormat;
            }
        }
        return null;
    }

    private static int inferQualityEncoding(int minQualAscii, int maxQualAscii) {

        /*
         * Phred+33:
         * 常见范围大概是 33 - 74
         *
         * Phred+64:
         * 常见范围大概是 64 - 104
         *
         * 两者在 64 - 74 有重叠。
         * 如果完全落在重叠区，无法严格判断，默认按现代 FASTQ 的 Phred+33。
         */

        if (minQualAscii < 64) {
            return Constants.SequenceInput.QUALITY_ENCODING_33;
        }

        if (maxQualAscii > 74) {
            return Constants.SequenceInput.QUALITY_ENCODING_64;
        }

        // ambiguous range: 64 - 74
        // modern FASTQ 默认 Phred+33
        return Constants.SequenceInput.QUALITY_ENCODING_33;
    }

    private boolean containsNotPairedMessage(String log) {
        if (log == null || log.isBlank()) {
            return false;
        }

        String lower = log.toLowerCase(Locale.ROOT);

        return lower.contains("not paired")
                || lower.contains("not appear to be paired")
                || lower.contains("do not appear to be paired")
                || lower.contains("names do not appear to be correctly paired")
                || lower.contains("do not appear to be correctly paired")
                || lower.contains("not appear to be correctly paired")
                || lower.contains("correctly paired");
    }

    private StageRunResult<ReadInspectStageOutput> inspect(
            Path originalSequenceLocalPath,
            StageExecutionInput stageExecutionInput) {

        int qualityEncoding = Constants.SequenceInput.QUALITY_ENCODING_33;
        int readLenType = Constants.SequenceInput.READ_LEN_TYPE_SHORT;
        int checksNum = 2000;
        int[] readLenBuf = new int[checksNum];
        int minQualAscii = Integer.MAX_VALUE;
        int maxQualAscii = Integer.MIN_VALUE;
        int recordCount = 0;

        try (BufferedReader bf = SequenceFileUtil.getReader(originalSequenceLocalPath)) {

            while (recordCount < checksNum) {
                String header = bf.readLine();

                if (header == null) {
                    break;
                }

                String sequence = bf.readLine();
                String plus = bf.readLine();
                String quality = bf.readLine();

                if (sequence == null || plus == null || quality == null) {
                    return this.runFail(
                            stageExecutionInput.stageContext,
                            "FASTQ 文件不完整",
                            stageExecutionInput.workDir);
                }

                // if (!header.startsWith("@")) {
                // return this.runFail(
                // stageExecutionInput.stageContext,
                // "FASTQ header 格式错误",
                // stageExecutionInput.workDir);
                // }

                // if (!plus.startsWith("+")) {
                // return this.runFail(
                // stageExecutionInput.stageContext,
                // "FASTQ plus line 格式错误",
                // stageExecutionInput.workDir);
                // }

                sequence = sequence.trim();
                quality = quality.trim();

                if (sequence.isEmpty()) {
                    return this.runFail(
                            stageExecutionInput.stageContext,
                            "FASTQ sequence 为空",
                            stageExecutionInput.workDir);
                }

                if (sequence.length() != quality.length()) {
                    return this.runFail(
                            stageExecutionInput.stageContext,
                            "FASTQ sequence 和 quality 长度不一致",
                            stageExecutionInput.workDir);
                }

                readLenBuf[recordCount] = sequence.length();

                for (int i = 0; i < quality.length(); i++) {
                    int ascii = quality.charAt(i);
                    minQualAscii = Math.min(minQualAscii, ascii);
                    maxQualAscii = Math.max(maxQualAscii, ascii);
                }

                recordCount++;
            }

            if (recordCount == 0) {
                return this.runFail(
                        stageExecutionInput.stageContext,
                        "FASTQ 文件为空",
                        stageExecutionInput.workDir);
            }

            Arrays.sort(readLenBuf, 0, recordCount);
            int medianReadLen = readLenBuf[Math.max(0, (recordCount - 1)) / 2];
            qualityEncoding = inferQualityEncoding(minQualAscii, maxQualAscii);

            if (medianReadLen >= LONG_READ_THRESHOLD) {
                readLenType = Constants.SequenceInput.READ_LEN_TYPE_LONG;
                ReadInspectStageOutput readInspectStageOutput = new ReadInspectStageOutput(qualityEncoding, readLenType,
                        true, stageExecutionInput.input.getRead1Url(), null, null, originalSequenceLocalPath);
            } else {
                readLenType = Constants.SequenceInput.READ_LEN_TYPE_SHORT;
            }

        } catch (IOException e) {
            logger.error(
                    "Read inspect failed, file = {}, stage = {}",
                    originalSequenceLocalPath,
                    stageExecutionInput.stageContext.getRunStageId(),
                    e);

            return this.runFail(
                    stageExecutionInput.stageContext,
                    "读取 FASTQ 文件失败",
                    stageExecutionInput.workDir);
        }

        String originalFormat = getFormatPostFix(originalSequenceLocalPath.getFileName().toString(), -1);

        Path bbmapReformatSHLog = stageExecutionInput.workDir.resolve("bbmapReformatSH.log");
        Path r1Path = stageExecutionInput.workDir.resolve("r1" + originalFormat);
        Path r2Path = stageExecutionInput.workDir.resolve("r2" + originalFormat);
        List<String> deinterleavedCmd = new ArrayList<>();
        deinterleavedCmd.add(Path.of(this.bbmapPath).resolve("reformat.sh").toAbsolutePath().toString());

        deinterleavedCmd.add("in=" + originalSequenceLocalPath.toAbsolutePath());
        deinterleavedCmd.add("out=" + r1Path.toAbsolutePath());
        deinterleavedCmd.add("out2=" + r2Path.toAbsolutePath());

        deinterleavedCmd.add("vint=t");
        deinterleavedCmd.add("ain=t");
        deinterleavedCmd.add("ow=t");

        ExecuteResult executeResult = _execute(deinterleavedCmd, stageExecutionInput.workDir, null, bbmapReformatSHLog);

        if (executeResult.ex != null) {

            String failReason = "BBMap reformat.sh 执行异常，无法判断 FASTQ 是否为 interleaved。"
                    + "错误信息：" + executeResult.ex.getMessage();

            logger.error(
                    "BBMap reformat.sh execution exception, stage = {}, cmd = {}, workDir = {}, logPath = {}, log = {}",
                    stageExecutionInput.stageContext.getRunStageId(),
                    String.join(" ", deinterleavedCmd),
                    stageExecutionInput.workDir,
                    bbmapReformatSHLog,
                    executeResult.ex);

        }

        if (executeResult.runCode != 0) {
            // check the log.
            String bbmapLog = "";
            try {
                if (Files.exists(bbmapReformatSHLog)) {
                    bbmapLog = Files.readString(bbmapReformatSHLog);
                }
            } catch (IOException e) {
                logger.warn(
                        "Read BBMap reformat.sh log failed, stage = {}, logPath = {}",
                        stageExecutionInput.stageContext.getRunStageId(),
                        bbmapReformatSHLog,
                        e);
            }

            if (containsNotPairedMessage(bbmapLog)) {
                // FASTQ 基础格式前面已经检查过；
                // BBMap 只是判断它不是 interleaved paired-end。
                // 所以按 single-end 保持原文件。
                return OK(
                        new ReadInspectStageOutput(
                                qualityEncoding,
                                readLenType,
                                true,
                                stageExecutionInput.input.getRead1Url(),
                                null,
                                null,
                                stageExecutionInput.workDir),
                        stageExecutionInput);
            } else {
                // real execution problem:
                // 可能是 BBMap 路径错误、Java 环境问题、权限问题、gzip 损坏、文件格式异常等。
                String failReason = "BBMap reformat.sh 执行失败，无法判断是否为 interleaved FASTQ。"
                        + " exitCode=" + executeResult.runCode;

                if (executeResult.ex != null) {
                    return this.runFail(
                            stageExecutionInput.stageContext,
                            failReason,
                            executeResult.ex,
                            stageExecutionInput.workDir);
                }

                return this.runFail(
                        stageExecutionInput.stageContext,
                        failReason,
                        stageExecutionInput.workDir);
            }

        }

        return OK(new ReadInspectStageOutput(qualityEncoding, readLenType, false, originalFormat, r1Path, r2Path,
                stageExecutionInput.workDir), stageExecutionInput);

    }

    private StageRunResult<ReadInspectStageOutput> inspect(Path originalSequenceLocalPath, boolean possibleInterleaved,
            StageExecutionInput stageExecutionInput) {

        String fileName = originalSequenceLocalPath.getFileName().toString();
        String baseName = fileName;
        String format = null;

        String originalSequenceUrl = stageExecutionInput.input.getRead1Url();

        boolean readLevelFasta = false && Constants.SequenceInput.isFasta(baseName);

        format = getFormatPostFix(baseName, readLevelFasta ? 1 : 0);
        baseName = baseName.substring(0, baseName.length() - format.length());

        int qualityEncoding = Constants.SequenceInput.QUALITY_ENCODING_33;
        int readLenType = Constants.SequenceInput.READ_LEN_TYPE_SHORT;

        Path workDir = stageExecutionInput.workDir;
        Path r1 = workDir.resolve(baseName + "_r1" + format);
        Path r2 = workDir.resolve(baseName + "_r2" + format);

        boolean checkedLen = false;
        int checkReadLenTypeThreshold = 1000;
        int[] readLens = new int[checkReadLenTypeThreshold];
        int recordLenRecordIndex = 0;

        try (BufferedReader br = SequenceFileUtil.getReader(originalSequenceLocalPath);
                BufferedWriter w1 = SequenceFileUtil.getWriter(r1);
                BufferedWriter w2 = SequenceFileUtil.getWriter(r2)) {

            int recordTravered = 0;
            int paired = 0;

            int checkPointRecordNum = 4000;

            String[][] recordBuffer = new String[2][readLevelFasta ? 2 : 4];

            int recordBufferIndex = 0;
            String r1HeaderId = null;

            boolean checkedInterleaved = false;
            boolean isInterleaved = false;

            String header = br.readLine();
            StringBuilder stringBuilder = null;
            if (readLevelFasta) {
                stringBuilder = new StringBuilder();
            }

            while (true) {

                if (header == null) {
                    break;
                }

                if (possibleInterleaved) {
                    recordBuffer[recordBufferIndex][0] = header;
                }

                int currentRecordLen = 0;

                if (true) {
                    for (int i = 0; i < 3; i++) {
                        String followingLine = br.readLine();
                        if (StringUtils.isBlank(followingLine)) {
                            return this.runFail(stageExecutionInput.stageContext, "不完整的输入",
                                    stageExecutionInput.workDir);
                        }

                        if (possibleInterleaved) {
                            recordBuffer[recordBufferIndex][1 + i] = followingLine;
                        }

                        if (i == 0) {
                            currentRecordLen = followingLine.length();
                        }
                    }

                }

                if (!checkedLen) {

                    readLens[recordLenRecordIndex] = currentRecordLen;
                    recordLenRecordIndex++;
                    if (recordLenRecordIndex >= checkReadLenTypeThreshold) {
                        checkedLen = true;
                        Arrays.sort(readLens);
                        int medianLen = readLens[(readLens.length - 1) / 2];
                        if (medianLen >= LONG_READ_THRESHOLD) {
                            readLenType = Constants.SequenceInput.READ_LEN_TYPE_LONG;
                            possibleInterleaved = false;
                            return OK(
                                    new ReadInspectStageOutput(qualityEncoding, readLenType, true, originalSequenceUrl,
                                            null, null,
                                            workDir),
                                    stageExecutionInput);
                        }
                    }
                }

                if (possibleInterleaved) {

                    String substractedId = substractRecordId(header);
                    if (recordBufferIndex == 0) {
                        r1HeaderId = substractedId;

                    } else {
                        String r2HeaderId = substractedId;
                        if (Objects.equals(r1HeaderId, r2HeaderId)) {

                            String r1Record = String.join("\n", recordBuffer[0]);
                            String r2Record = String.join("\n", recordBuffer[1]);
                            w1.write(r1Record);
                            w1.newLine();
                            w2.write(r2Record);
                            w2.newLine();
                            paired++;

                        } else {
                            // not paired with previous, probably start of new pair. Discard old and start a
                            // new one.
                            recordBuffer[0] = recordBuffer[1];
                            recordBufferIndex = 0;
                            r1HeaderId = substractedId;
                        }
                    }
                }

                if (possibleInterleaved) {
                    recordBufferIndex = (recordBufferIndex + 1) % 2;
                    recordTravered += 1;
                }

                if (possibleInterleaved && !checkedInterleaved && recordTravered >= checkPointRecordNum) {
                    checkedInterleaved = true;
                    double ratio = (paired * 2.0d) / recordTravered;
                    isInterleaved = checkInterleaved(ratio);
                    if (!isInterleaved) {
                        if (checkSingleRead(ratio)) {
                            return OK(
                                    new ReadInspectStageOutput(qualityEncoding, readLenType, true, originalSequenceUrl,
                                            null, null,
                                            stageExecutionInput.workDir),
                                    stageExecutionInput);
                        } else {
                            logger.warn(
                                    "stage = {}, ambiguous FASTQ input, unable to determine layout, " +
                                            "records = {}, paired = {}, ratio = {}",
                                    stageExecutionInput.stageContext.getRunStageId(),
                                    recordTravered,
                                    paired,
                                    String.format("%.4f", ratio));

                            return this.runFail(stageExecutionInput.stageContext, "无法识别输入类型",
                                    stageExecutionInput.workDir);
                        }
                    }
                }

                header = br.readLine();
            }

            if (!checkedLen) {
                Arrays.sort(readLens, 0, recordLenRecordIndex);
                int median = readLens[(recordLenRecordIndex - 1) / 2];
                if (median >= LONG_READ_THRESHOLD) {
                    readLenType = Constants.SequenceInput.READ_LEN_TYPE_LONG;
                }
            }

            if (possibleInterleaved) {

                if (checkedInterleaved) {
                    return OK(
                            new ReadInspectStageOutput(qualityEncoding, readLenType, false, null, r1, r2,
                                    stageExecutionInput.workDir),
                            stageExecutionInput);
                } else {
                    double ratio = (paired * 2.0) / recordTravered;
                    if (!checkInterleaved(ratio)) {
                        if (checkSingleRead(ratio)) {
                            return OK(
                                    new ReadInspectStageOutput(qualityEncoding, readLenType, true, originalSequenceUrl,
                                            null, null,
                                            stageExecutionInput.workDir),
                                    stageExecutionInput);
                        } else {
                            logger.warn(
                                    "stage = {}, ambiguous FASTQ input, unable to determine layout, " +
                                            "records = {}, paired = {}, ratio = {}",
                                    stageExecutionInput.stageContext.getRunStageId(),
                                    recordTravered,
                                    paired,
                                    String.format("%.4f", ratio));

                            return this.runFail(stageExecutionInput.stageContext, "无法识别输入类型",
                                    stageExecutionInput.workDir);
                        }
                    }

                    return OK(new ReadInspectStageOutput(qualityEncoding, readLenType, true, originalSequenceUrl, null,
                            null,
                            stageExecutionInput.workDir), stageExecutionInput);
                }
            } else {

                return OK(
                        new ReadInspectStageOutput(qualityEncoding, readLenType, true, originalSequenceUrl, null, null,
                                stageExecutionInput.workDir),
                        stageExecutionInput);
            }

        } catch (Exception e) {
            logger.error("Inspection exception, run stage id = {}", stageExecutionInput.stageContext.getRunStageId(),
                    e);
            return this.runFail(stageExecutionInput.stageContext, "Inspection exception", stageExecutionInput.workDir);
        }

    }

    @Override
    protected StageRunResult<ReadInspectStageOutput> _execute(
            StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {

        ReadInspectStageInputUrls readInspectStageInputUrls = stageExecutionInput.input;

        String read1Url = readInspectStageInputUrls.getRead1Url();
        String read2Url = readInspectStageInputUrls.getRead2Url();

        if (StringUtils.isNotBlank(read2Url)) {
            return OK(new ReadInspectStageOutput(
                    Constants.SequenceInput.QUALITY_ENCODING_33,
                    Constants.SequenceInput.READ_LEN_TYPE_SHORT,
                    true,
                    read1Url,
                    null, null,
                    stageExecutionInput.workDir),
                    stageExecutionInput);
        }

        Path readLocalPath = stageExecutionInput.inputDir.resolve(read1Url.substring(read1Url.lastIndexOf("/") + 1));

        Map<String, Path> loadMap = Map.of(read1Url, readLocalPath);
        loadInput(loadMap);

        return this.inspect(readLocalPath, stageExecutionInput);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
    }

}
