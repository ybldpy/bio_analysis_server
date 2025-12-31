package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.RefSeqService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.taskrunner.parameters.RefSeqConfig;
import com.xjtlu.bio.taskrunner.stageOutput.StageOutput;

import jakarta.annotation.Resource;

public abstract class AbstractPipelineStageExector implements PipelineStageExecutor {

    protected static class StageOutputValidationResult {

        public final Path path;
        /** true 表示产物不可用：不存在 或 size<=0 */
        public final boolean empty;
        /** 检查过程中发生的 IO 异常；没有则为 null */
        public final IOException ioException;

        public StageOutputValidationResult(Path path, boolean empty, IOException ioException) {
            this.path = path;
            this.empty = empty;
            this.ioException = ioException;
        }

        public boolean hasIoError() {
            return ioException != null;
        }
    }

    protected static class ExecuteResult{
        public final int runCode;
        public final Exception ex;
        public ExecuteResult(int runCode, Exception ex) {
            this.runCode = runCode;
            this.ex = ex;
        }

        public boolean success(){
            return runCode == 0 && ex == null;
        }
    }

    @Resource
    protected PipelineService pipelineService;
    @Resource
    protected ObjectMapper objectMapper;
    @Resource
    protected RefSeqService refSeqService;

    @Resource
    protected StorageService storageService;

    protected static final String PARSE_JSON_ERROR = "解析参数错误";

    protected String stageResultTmpBasePath;
    protected String stageInputTmpBasePath;

    protected static final String ERROR_LOAD_REFSEQ = "加载参考基因组失败";

    protected static boolean requireNonEmpty(Path p) throws IOException {

        if (!Files.exists(p) || Files.size(p) <= 0) {
            return false;
        }
        return true;
    }

    protected StageRunResult runException(BioPipelineStage bioPipelineStage, Exception e) {
        return runFail(bioPipelineStage, "异常\n" + e.getMessage());
    }

    protected StageRunResult runFail(BioPipelineStage bioPipelineStage, String errorMsg, Exception e) {
        return StageRunResult.fail(errorMsg, bioPipelineStage, e);
    }

    protected StageRunResult runFail(BioPipelineStage bioPipelineStage, String msg) {
        return runFail(bioPipelineStage, msg, null);
    }

    public Path stageInputPath(BioPipelineStage bioPipelineStage) {
        return Paths.get(this.stageInputTmpBasePath, String.valueOf(bioPipelineStage.getStageId()));
    }

    protected static List<StageOutputValidationResult> validateOutputFiles(Path... paths) {
        ArrayList<StageOutputValidationResult> errorValidationResults = new ArrayList<>();
        for (Path p : paths) {
            try {
                if (!requireNonEmpty(p)) {
                    errorValidationResults.add(new StageOutputValidationResult(p, true, null));
                }
            } catch (IOException e) {
                errorValidationResults.add(new StageOutputValidationResult(p, true, e));
            }
        }

        return errorValidationResults;

    }

    protected static String createStageOutputValidationErrorMessge(List<StageOutputValidationResult> errors) {

        StringBuilder sb = new StringBuilder(512);
        sb.append("=== STAGE_OUTPUT_VALIDATION_FAILED ===\n");
        sb.append("timestamp=").append(java.time.OffsetDateTime.now()).append('\n');

        int count = (errors == null) ? 0 : errors.size();
        sb.append("errorCount=").append(count).append('\n');

        if (count == 0) {
            sb.append("reason=errors list is null/empty (no details)\n");
            sb.append("====================================\n");
            return sb.toString();
        }

        int i = 1;
        for (StageOutputValidationResult r : errors) {
            sb.append("[").append(i++).append("] ");

            Path p = (r == null) ? null : r.path;
            boolean empty = (r != null) && r.empty;
            IOException ioe = (r == null) ? null : r.ioException;

            sb.append("path=").append(p == null ? "<null>" : p.toAbsolutePath())
                    .append(" empty=").append(empty);

            if (ioe != null) {
                sb.append(" ioEx=").append(ioe.getClass().getSimpleName())
                        .append(" msg=").append(ioe.getMessage()==null?"<null>": ioe.getMessage().replace("\n", " ").replace("\r", " ").replace("\t", " "));
            }

            sb.append('\n');
        }

        sb.append("hint=check subprocess exitCode and stderr/stdout logs for root cause\n");
        sb.append("====================================\n");
        return sb.toString();

    }


    protected StageRunResult runFail(BioPipelineStage bioPipelineStage, String errorMessge, Exception e,Path inputDir, Path workDir){
        this.deleteTmpFiles(List.of(inputDir.toFile(), workDir.toFile()));
        return this.runFail(bioPipelineStage, errorMessge,e);
    }

    protected static String appendSuffixBeforeExtensions(String fileName, String suffix) {
        if (fileName == null || fileName.isEmpty())
            return fileName;

        // 如果已经有相同后缀就不重复添加
        String stem = fileName;
        String ext = "";

        // 先处理双扩展 .fastq.gz / .fq.gz
        if (fileName.endsWith(".fastq.gz")) {
            stem = fileName.substring(0, fileName.length() - ".fastq.gz".length());
            ext = ".fastq.gz";
        } else if (fileName.endsWith(".fq.gz")) {
            stem = fileName.substring(0, fileName.length() - ".fq.gz".length());
            ext = ".fq.gz";
        } else if (fileName.endsWith(".fastq")) {
            stem = fileName.substring(0, fileName.length() - ".fastq".length());
            ext = ".fastq";
        } else if (fileName.endsWith(".fq")) {
            stem = fileName.substring(0, fileName.length() - ".fq".length());
            ext = ".fq";
        } else if (fileName.endsWith(".gz")) {
            // 泛化：如果只是 .gz，再往前找一次点
            String withoutGz = fileName.substring(0, fileName.length() - 3);
            int lastDot = withoutGz.lastIndexOf('.');
            if (lastDot >= 0) {
                stem = withoutGz.substring(0, lastDot);
                ext = withoutGz.substring(lastDot) + ".gz"; // .xxx + .gz
            } else {
                stem = withoutGz;
                ext = ".gz";
            }
        } else {
            // 普通扩展或无扩展
            int lastDot = fileName.lastIndexOf('.');
            if (lastDot >= 0) {
                stem = fileName.substring(0, lastDot);
                ext = fileName.substring(lastDot);
            } else {
                stem = fileName;
                ext = "";
            }
        }

        if (stem.endsWith(suffix))
            return stem + ext; // 已有后缀则直接返回
        return stem + suffix + ext;
    }

    public abstract int id();

    protected StageRunResult parseError(BioPipelineStage bioPipelineStage) {
        return runFail(bioPipelineStage, PARSE_JSON_ERROR);
    }

    protected static boolean isMap(Object obj) {
        return (obj != null) && obj instanceof Map;
    }

    protected Map<String, GetObjectResult> loadInput(Map<String, Path> objectAndWriteToPath) {

        HashMap<String, GetObjectResult> inputMap = new HashMap<>();
        for (Map.Entry<String, Path> item : objectAndWriteToPath.entrySet()) {
            GetObjectResult getObjectResult = this.storageService.getObject(item.getKey(), item.getValue().toString());
            inputMap.put(item.getKey(), getObjectResult);
            if (!getObjectResult.success()) {
                break;
            }
        }
        return inputMap;
    }

    protected String findFailedLoadingObject(Map<String, GetObjectResult> getResultMap) {

        for (Map.Entry<String, GetObjectResult> item : getResultMap.entrySet()) {
            if (!item.getValue().success()) {
                return item.getKey();
            }
        }

        return null;
    }

    protected RefSeqConfig getRefSeqConfigFromParams(Map<String, Object> params) {

        Object refseqConfigObj = params.get(PipelineService.PIPELINE_STAGE_PARAMETERS_REFSEQ_IS_INNER);
        if (!isMap(refseqConfigObj)) {
            return null;
        }

        Map<String, Object> refseqConfig = (Map) refseqConfigObj;

        Object refseq = refseqConfig.get(PipelineService.PIPLEINE_STAGE_PARAMETERS_REFSEQ_KEY);
        Object isInnerRefseq = refseqConfig.get(PipelineService.PIPELINE_STAGE_PARAMETERS_REFSEQ_IS_INNER);

        if (refseq == null) {
            return null;
        }
        if (isInnerRefseq == null && !(refseq instanceof Integer)) {
            return null;
        }

        if (isInnerRefseq != null && !(isInnerRefseq instanceof Boolean)) {
            return null;
        }

        boolean isInner = (Boolean) isInnerRefseq;

        if (isInner && !(refseq instanceof Integer)) {
            return null;
        }
        if (!isInner && !(refseq instanceof String)) {
            return null;
        }

        if (isInner) {
            return new RefSeqConfig(true, null, (Integer) refseq);
        }

        return new RefSeqConfig(false, (String) refseq, -1);

    }

    protected Object substractRefseqFromMap(Map<String, Object> params) {
        Object refseq = params.get(PipelineService.PIPLEINE_STAGE_PARAMETERS_REFSEQ_KEY);
        Object isInnerRefseq = params.get(PipelineService.PIPELINE_STAGE_PARAMETERS_REFSEQ_IS_INNER);

        if (refseq == null) {
            return null;
        }
        if (isInnerRefseq == null && !(refseq instanceof Integer)) {
            return null;
        }

        return refseq;

    }

    protected File[] moveSampleReadFilesToTmpPath(String r1Url, Path r1TmpPath, String r2Url, Path r2TmpPath) {

        GetObjectResult r1Result = storageService.getObject(r1Url, r1TmpPath.toString());
        GetObjectResult r2Result = null;
        if (r2Url != null) {
            r2Result = storageService.getObject(r2Url, r2TmpPath.toString());
        }
        return new File[] { r1Result.objectFile(), r2Url != null ? r2Result.objectFile() : null };

    }

    protected static int runSubProcess(List<String> cmd, Path workDir) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(workDir.toFile());

        // 不需要日志：直接丢弃 stdout/stderr
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process p = pb.start();
        int code = p.waitFor(); // 无超时：一直等到结束
        return code;

    }

    public Path workDirPath(BioPipelineStage bioPipelineStage) {
        return Paths.get(this.stageResultTmpBasePath, String.valueOf(bioPipelineStage.getStageId()));
    }

    protected StageRunResult ok(BioPipelineStage bioPipelineStage, StageOutput stageOutput, Path deleteDir){
        this.deleteTmpFiles(List.of(deleteDir.toFile()));
        return StageRunResult.OK(stageOutput, bioPipelineStage);
    }


    protected ExecuteResult execute(List<String> cmd, Path workDir){

        int runCode = -1;
        Exception runEx = null;
        try {
            runCode = runSubProcess(cmd, workDir);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            runEx = e;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            Thread.currentThread().interrupt();
            runEx = e;
        }
        return new ExecuteResult(runCode, runEx);
    }

    protected void deleteTmpFiles(List<File> tmpFiles) {
        for (File f : tmpFiles) {
            if (f != null && f.exists()) {
                if (f.isDirectory()) {
                    try {
                        FileUtils.deleteDirectory(f);
                    } catch (IOException e) {

                    }
                } else {
                    f.delete();
                }
            }
        }
    }

}
