package com.xjtlu.bio.analysisPipeline.taskrunner;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.StageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SeroTypingStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.RefSeqService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;

public abstract class AbstractPipelineStageExector<T extends StageOutput, Input extends StageInputUrls, StageParameters extends BaseStageParams>
        implements PipelineStageExecutor<T> {

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

    protected static class ExecuteResult {
        public final int runCode;
        public final Exception ex;

        public ExecuteResult(int runCode, Exception ex) {
            this.runCode = runCode;
            this.ex = ex;
        }

        public boolean success() {
            return runCode == 0 && ex == null;
        }
    }

    protected class StageExecutionInput {
        StageContext stageContext;
        Input input;
        StageParameters stageParameters;
        Path workDir;
        Path inputDir;
    }

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    protected PipelineService pipelineService;
    @Resource
    protected ObjectMapper objectMapper;
    @Resource
    protected RefSeqService refSeqService;

    @Resource
    protected StorageService storageService;

    @Resource
    protected BioStageUtil bioStageUtil;

    @Resource
    protected AnalysisPipelineToolsConfig analysisPipelineToolsConfig;

    protected static final String PARSE_JSON_ERROR = "解析参数错误";

    protected String stageResultTmpBasePath;
    protected String stageInputTmpBasePath;

    protected static final String ERROR_LOAD_REFSEQ = "加载参考基因组失败";
    protected static final String ERROR_EXECUTE_FAIL = "运行失败";

    protected static boolean requireNonEmpty(Path p) throws IOException {

        if (!Files.exists(p) || Files.size(p) <= 0) {
            return false;
        }
        return true;
    }

    protected void preExecute(StageContext bioPipelineStage) throws IOException {
        resetDirectory(this.workDirPath(bioPipelineStage));
        resetDirectory(this.stageInputPath(bioPipelineStage));
    }

    protected static class LoadFailException extends Exception {

        public Exception causeException;

        public LoadFailException(String message, Exception causeException) {
            super(message);
            this.causeException = causeException;
        }

        public LoadFailException(Exception caException) {
            this(null, caException);
        }

        public LoadFailException() {

        }
    }

    protected static class NotGetRefSeqException extends Exception {

        private static final long serialVersionUID = 1L;

        private String refSeqId;
        private String reason;

        public NotGetRefSeqException(String refSeqId, String reason) {
            super("Failed to get RefSeq: " + refSeqId + ", reason: " + reason);
            this.refSeqId = refSeqId;
            this.reason = reason;
        }

        public String getRefSeqId() {
            return refSeqId;
        }

        public String getReason() {
            return reason;
        }

    }

    protected abstract Class<Input> stageInputType();

    protected abstract Class<StageParameters> stageParameterType();

    private void resetDirectory(Path path) throws IOException {
        File dir = path.toFile();

        if (dir.exists()) {
            // 关键点：cleanDirectory 只会删里面的东西，不会删 dir 这个壳
            // 这能有效避免 "delete之后立刻mkdir" 导致的 FileAlreadyExistsException
            FileUtils.cleanDirectory(dir);
        } else {
            // 不存在，则连同父级目录一起创建
            Files.createDirectories(path);
        }
    }

    protected void logErr(String msg, Exception e) {
        this.logger.error("Msg = {}. Exception = ", msg, e);
    }

    protected Map<String, String> loadInputUrlMap(BioPipelineStage bioPipelineStage) {
        try {
            Map<String, String> inputUrlMap = JsonUtil.toMap(bioPipelineStage.getInputUrl(), String.class);
            return inputUrlMap;
        } catch (JsonProcessingException e) {
            logger.error("{} parsing input json exception", bioPipelineStage);
        }
        return null;
    }

    protected void postExecute(StageRunResult stageRunResult) {
        Path inputDir = this.stageInputPath(stageRunResult.getStageContext());
        FileUtils.deleteQuietly(inputDir.toFile());
        if (!stageRunResult.isSuccess()) {
            FileUtils.deleteQuietly(this.workDirPath(stageRunResult.getStageContext()).toFile());
        }
    }

    protected boolean _execute(List<String> runCmd, Path redirectOutputStream, StageExecutionInput stageExecutionInput,
            Path... toValidateFiles) {

        ExecuteResult executeResult = redirectOutputStream == null ? _execute(runCmd, stageExecutionInput.workDir)
                : _execute(runCmd, stageExecutionInput.workDir, redirectOutputStream, null);

        if (!executeResult.success()) {
            logExecutionFailed(executeResult, stageExecutionInput.stageContext.getRunStageId());
            return false;
        }

        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(toValidateFiles);
        if (!stageOutputValidationResults.isEmpty()) {
            logNoOutput(stageOutputValidationResults, stageExecutionInput.stageContext.getRunStageId());
            return false;
        }
        return true;
    }

    @Override
    public StageRunResult<T> execute(BioPipelineStage bioPipelineStage) {

        StageContext stageContext = new StageContext();
        stageContext.setRunStageId(bioPipelineStage.getStageId());
        stageContext.setVersion(bioPipelineStage.getVersion());
        stageContext.setStageType(bioPipelineStage.getStageType());

        try {
            preExecute(stageContext);
        } catch (Exception e) {
            logger.error("{} exception happens at preExecute", bioPipelineStage, e);
            StageRunResult<T> stageRunResult = StageRunResult.fail("异常发生", stageContext, e);
            postExecute(stageRunResult);
            return stageRunResult;
        }

        Path workDir = this.workDirPath(stageContext);
        Path inputDir = this.stageInputPath(stageContext);

        StageExecutionInput stageExecutionInput = new StageExecutionInput();
        stageExecutionInput.stageContext = stageContext;
        stageExecutionInput.inputDir = inputDir;
        stageExecutionInput.workDir = workDir;

        StageRunResult<T> stageRunResult = null;
        try {

            stageExecutionInput.input = JsonUtil.toObject(bioPipelineStage.getInputUrl(), stageInputType());
            stageExecutionInput.stageParameters = JsonUtil.toObject(bioPipelineStage.getParameters(),
                    stageParameterType());
            stageRunResult = _execute(stageExecutionInput);
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            logger.error("JSON mapping exception while executing stage. input={}", stageExecutionInput, e);
            stageRunResult = this.runException(stageContext, e);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            logger.error("JSON processing exception while executing stage. input={}", stageExecutionInput, e);
            stageRunResult = this.runException(stageContext, e);
        } catch (LoadFailException e) {
            logger.error("stage = {} load failed. ", bioPipelineStage.getStageId(), e.causeException);
            stageRunResult = this.runException(stageContext, e);
        }catch(NotGetRefSeqException e){
            logger.error("stage = {}. Not get Refseq. reason = {}", bioPipelineStage.getStageId(), e.getReason());
            stageRunResult = this.runException(stageContext, e);
        }
        postExecute(stageRunResult);
        return stageRunResult;
    }

    /**
     * Subclasses must NOT throw exceptions.
     * Any failure should be captured and returned as
     * {@link StageRunResult#fail(...)}.
     */
    protected abstract StageRunResult<T> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException;

    protected StageRunResult<T> runException(StageContext bioPipelineStage, Exception e) {
        return runFail(bioPipelineStage, "异常\n" + e.getMessage());
    }

    protected StageRunResult<T> runFail(StageContext stageContext, String errorMsg, Exception e) {
        return StageRunResult.fail(errorMsg, stageContext, e);
    }

    protected StageRunResult<T> runFail(StageContext bioPipelineStage, String msg) {
        return runFail(bioPipelineStage, msg, null);
    }

    public Path stageInputPath(StageContext bioPipelineStage) {
        return bioStageUtil.stageExecutorInputDir(bioPipelineStage);
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

    protected void logNoOutput(List<StageOutputValidationResult> results, long stage) {
        if (results == null || results.isEmpty()) {
            logger.error(
                    "Stage output validation failed (no details). stageId={}",
                    stage);
            return;
        }

        // 1) 先把“空文件/不存在”的列出来
        String emptyPaths = results.stream()
                .filter(r -> r != null && r.empty && !r.hasIoError())
                .map(r -> String.valueOf(r.path))
                .distinct()
                .reduce((a, b) -> a + ", " + b)
                .orElse("");

        // 2) 再把 IO 异常的列出来（每个异常都打印堆栈）
        List<StageOutputValidationResult> ioErrors = results.stream()
                .filter(r -> r != null && r.hasIoError())
                .toList();

        if (!emptyPaths.isEmpty()) {
            logger.error(
                    "Stage output missing/empty. stageId={}, path={}",
                    stage,
                    emptyPaths);
        }

        for (StageOutputValidationResult r : ioErrors) {
            logger.error(
                    "Stage output validation IO error. stageId={}, path={}",
                    stage,
                    r.path,
                    r.ioException);
        }

        // 如果没有 emptyPaths 也没有 ioErrors，说明 results 里可能都是 null 或 empty=false
        if (emptyPaths.isEmpty() && ioErrors.isEmpty()) {
            logger.error(
                    "Stage output validation failed (unexpected state). stageId={}, resultCount={}",
                    stage,
                    results.size());
        }
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
                        .append(" msg=").append(ioe.getMessage() == null ? "<null>"
                                : ioe.getMessage().replace("\n", " ").replace("\r", " ").replace("\t", " "));
            }

            sb.append('\n');
        }

        sb.append("hint=check subprocess exitCode and stderr/stdout logs for root cause\n");
        sb.append("====================================\n");
        return sb.toString();

    }

    protected StageRunResult<T> runFail(StageContext stageContext, String errorMessge, Exception e,
            Path inputDir,
            Path workDir) {
        this.deleteTmpFiles(List.of(inputDir.toFile(), workDir.toFile()));
        return this.runFail(stageContext, errorMessge, e);
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

    protected StageRunResult parseError(StageContext bioPipelineStage) {
        return runFail(bioPipelineStage, PARSE_JSON_ERROR);
    }

    protected static boolean isMap(Object obj) {
        return (obj != null) && obj instanceof Map;
    }

    protected void logExecutionFailed(ExecuteResult failResult, long stage) {
        logger.error(
                "Stage execution failed. stageId={}, runCode={}, error={}",
                stage,
                failResult.runCode,
                failResult.ex == null ? "unknown" : failResult.ex.getMessage(),
                failResult.ex);
    }

    // protected StageRunResult<T> runFail(ExecuteResult failResult,
    // BioPipelineStage stage) {
    // logExecutionFailed(failResult, stage);
    // return this.runFail(stage, "execution failed");
    // }

    protected void loadInput(Map<String, Path> objectAndWriteToPath) throws LoadFailException {

        HashMap<String, GetObjectResult> inputMap = new HashMap<>();
        for (Map.Entry<String, Path> item : objectAndWriteToPath.entrySet()) {
            GetObjectResult getObjectResult = this.storageService.getObject(item.getKey(), item.getValue().toString());
            inputMap.put(item.getKey(), getObjectResult);
            if (!getObjectResult.success()) {
                logger.error("load input {} failed", item.getKey(), getObjectResult.e());
                throw new LoadFailException(getObjectResult.e());
            }
        }
    }

    protected String findFailedLoadingObject(Map<String, GetObjectResult> getResultMap) {

        for (Map.Entry<String, GetObjectResult> item : getResultMap.entrySet()) {
            if (!item.getValue().success()) {
                return item.getKey();
            }
        }

        return null;
    }

    // protected RefSeqConfig getRefSeqConfigFromParams(Map<String, Object> params)
    // {

    // Object refseqConfigObj =
    // params.get(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG);
    // if (refseqConfigObj == null || !(refseqConfigObj instanceof Map)) {
    // return null;
    // }

    // try {
    // RefSeqConfig refSeqConfig = JsonUtil.mapToPojo((Map) refseqConfigObj,
    // RefSeqConfig.class);
    // return refSeqConfig;
    // } catch (Exception e) {
    // logger.error("converting {} to RefseqConfigException", refseqConfigObj);
    // return null;
    // }

    // }

    // protected Object substractRefseqFromMap(Map<String, Object> params) {
    // Object refseq =
    // params.get(PipelineService.PIPLEINE_STAGE_PARAMETERS_REFSEQ_KEY);
    // Object isInnerRefseq =
    // params.get(PipelineService.PIPELINE_STAGE_PARAMETERS_REFSEQ_IS_INNER);

    // if (refseq == null) {
    // return null;
    // }
    // if (isInnerRefseq == null && !(refseq instanceof Integer)) {
    // return null;
    // }

    // return refseq;

    // }

    protected File[] moveSampleReadFilesToTmpPath(String r1Url, Path r1TmpPath, String r2Url, Path r2TmpPath) {

        GetObjectResult r1Result = storageService.getObject(r1Url, r1TmpPath.toString());
        GetObjectResult r2Result = null;
        if (r2Url != null) {
            r2Result = storageService.getObject(r2Url, r2TmpPath.toString());
        }
        return new File[] { r1Result.objectFile(), r2Url != null ? r2Result.objectFile() : null };

    }

    protected int runSubProcess(List<String> cmd, Path workDir, Path stdout, Path stdErr, boolean append)
            throws InterruptedException, IOException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(workDir.toFile());

        // stdout
        if (stdout != null) {
            Files.createDirectories(stdout.toAbsolutePath().getParent());
            pb.redirectOutput(append
                    ? ProcessBuilder.Redirect.appendTo(stdout.toFile())
                    : ProcessBuilder.Redirect.to(stdout.toFile()));
        } else {
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT); // 或 DISCARD，看你默认想要啥
        }

        // stderr（仅在不合并时有效）
        if (stdErr != null) {

            pb.redirectError(append
                    ? ProcessBuilder.Redirect.appendTo(stdErr.toFile())
                    : ProcessBuilder.Redirect.to(stdErr.toFile()));

        } else {
            pb.redirectError(ProcessBuilder.Redirect.INHERIT); // 或 DISCARD
        }

        Process p = pb.start();
        return p.waitFor();
    }

    protected int runSubProcess(List<String> cmd, Path workDir) throws IOException, InterruptedException {

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(workDir.toFile());

        // 不需要日志：直接丢弃 stdout/stderr
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        int code = p.waitFor(); // 无超时：一直等到结束
        return code;
    }

    public Path workDirPath(StageContext bioPipelineStage) {
        return bioStageUtil.stageExecutorWorkDir(bioPipelineStage);
    }

    protected ExecuteResult _execute(List<String> cmd, Path workDir, Path stdout, Path stdErr) {
        int runCode = -1;
        Exception runEx = null;
        try {
            runCode = runSubProcess(cmd, workDir, stdout, stdErr, false);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            runEx = e;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            Thread.currentThread().interrupt();
            runEx = e;
        }

        if (runEx != null) {
            return new ExecuteResult(runCode, runEx);
        }

        return new ExecuteResult(runCode, runEx);
    }

    protected ExecuteResult _execute(List<String> cmd, Path workDir) {

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

        if (runEx != null) {
            return new ExecuteResult(runCode, runEx);
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
