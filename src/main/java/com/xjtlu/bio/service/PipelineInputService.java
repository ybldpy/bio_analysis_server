package com.xjtlu.bio.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineInputFileExample;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.mapper.BioPipelineInputFileMapper;
import com.xjtlu.bio.service.StorageService.PutResult;
import com.xjtlu.bio.utils.SampleReadLengthDetector;

import jakarta.annotation.Resource;

@Service
public class PipelineInputService {

    // @Resource
    // private BioSampleMapper sampleMapper;
    // @Resource
    // private BioSampleExtensionMapper bioSampleExtensionMapper;

    private static Logger logger = LoggerFactory.getLogger(PipelineInputService.class);

    public static final int RESPONSE_CODE_FILE_EXIST = 100;
    public static final int RESPONSE_CODE_INVALID_KEY = 110;
    public static final int RESPONSE_CODE_BAD_KEY = 120;

    @Resource
    private BioPipelineInputFileMapper bioPipelineInputFileMapper;
    @Resource
    private PipelineService pipelineService;
    @Resource
    private SqlSessionTemplate batchSqlSessionTemplate;
    @Resource
    private SampleReadLengthDetector sampleReadLengthDetector;
    private Set<String> uploadingFiles = ConcurrentHashMap.newKeySet();

    @Resource
    private StorageService storageService;

    public static final int SAMPLE_UPLOAD_STATUS_NOT_UPLOAD = 0;
    public static final int SAMPLE_UPLOAD_STATUS_UPLOADING = 1;
    public static final int SAMPLE_UPLOAD_STATUS_READY = 2;
    public static final int SAMPLE_UPLOAD_STATUS_ERROR = 3;

    public static final int PIPELINE_INPUT_FILE_ROLE_R1 = 10;
    public static final int PIPELINE_INPUT_FILE_ROLE_R2 = 20;

    public static final int PIPELINE_INPUT_FILE_STATUS_NOT_UPLOAD = 0;
    public static final int PIPELINE_INPUT_FILE_STATUS_UPLOADED = 1;
    public static final int PIPELINE_INPUT_FILE_STATUS_CANCELLED = 2;

    public static final int PIPELINE_INPUT_TYPE_READ = 0;
    public static final int PIPELINE_INPUT_TYPE_REFSEQ = 1;

    private static final int INPUT_ALLOWED = 0;
    // key format is illegal
    private static final int INPUT_NOT_ALLOWED_BAD_KEY = 1;
    // key is valid but not allowed for this pipeline
    private static final int INPUT_NOT_ALLOWED_INVALID_KEY = 2;

    // key already exists
    private static final int INPUT_NOT_ALLOWED_DUPLICATE_KEY = 3;

    private static final Pattern SAMPLE_READ_KEY_PATTERN = Pattern.compile("^sample_\\d+/[01]$");

    private Set<String> uploadingLockSet = ConcurrentHashMap.newKeySet();

    private ConcurrentHashMap<Long, Integer> pipelineUploadingLocks = new ConcurrentHashMap<>();

    public List<BioPipelineInputFile> queryInputs(BioPipelineInputFileExample example) {

        return this.bioPipelineInputFileMapper.selectByExample(example);

    }

    private static String substractPostfixFromFileName(String filename) {
        int index = filename.indexOf(".");
        if (index == -1) {
            return "";
        }
        return filename.substring(index + 1);
    }

    private boolean checkInputFileExist(String filePath) {

        return storageService.exists(filePath);
    }

    private Result<Void> checkStatus(BioPipelineInputFile bioPipelineInputFile) {

        if (bioPipelineInputFile == null) {
            return new Result<Void>(Result.BUSINESS_FAIL, null, "未找到目标文件，不存在的分析任务");
        }
        if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_NOT_UPLOAD) {
            String path = bioPipelineInputFile.getFilePath();
            if (this.storageService.exists(path)) {

                return new Result<Void>(Result.SUCCESS, null, null);
            }
            return null;
        }
        if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_UPLOADED) {
            return new Result<Void>(Result.SUCCESS, null, null);
        }
        if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_CANCELLED) {
            return new Result<Void>(Result.BUSINESS_FAIL, null, "分析任务已取消");
        }

        return null;
    }

    private int updateStatus(BioPipelineInputFile bioPipelineInputFile, int status) {
        BioPipelineInputFile update = new BioPipelineInputFile();
        update.setStatus(status);
        BioPipelineInputFileExample bioPipelineInputFileExample = new BioPipelineInputFileExample();
        bioPipelineInputFileExample.createCriteria().andInputFileIdEqualTo(bioPipelineInputFile.getInputFileId());
        int res = this.bioPipelineInputFileMapper.updateByExampleSelective(update, bioPipelineInputFileExample);
        return res;
    }

    private Result<Void> handleUploaded(BioPipelineInputFile bioPipelineInputFile) {

        int updateRes = updateStatus(bioPipelineInputFile, PIPELINE_INPUT_FILE_STATUS_UPLOADED);
        if (updateRes <= 0) {
            return uploadFail();
        }

        // this.pipelineService.handleInputRecevied(bioPipelineInputFile.getPipelineId());

        return new Result<Void>(Result.SUCCESS, null, null);
    }

    private Result<Void> uploadFail() {
        return new Result<Void>(Result.INTERNAL_FAIL, null, "上传失败");
    }

    // public Result<Void> recevieInput(long inputFileId, InputStream in) {

    //     try {



            
    //         boolean getLockSuccess = this.uploadingLockSet.add(inputFileId);
    //         if (!getLockSuccess) {
    //             return new Result(Result.DUPLICATE_OPERATION, null, "文件正在上传");
    //         }

    //         BioPipelineInputFile bioPipelineInputFile = this.bioPipelineInputFileMapper.selectByPrimaryKey(inputFileId);

    //         if (bioPipelineInputFile == null) {
    //             return new Result<Void>(Result.BUSINESS_FAIL, null, "未找到目标文件，不存在的分析任务");
    //         } else if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_NOT_UPLOAD) {
    //             String path = bioPipelineInputFile.getFilePath();
    //             if (this.storageService.exists(path)) {
    //                 return this.handleUploaded(bioPipelineInputFile);
    //             }
    //         } else if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_UPLOADED) {

    //             this.pipelineService.handleInputRecevied(bioPipelineInputFile.getPipelineId());
    //             return new Result<Void>(Result.SUCCESS, null, null);
    //         } else if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_CANCELLED) {
    //             return new Result<Void>(Result.BUSINESS_FAIL, null, "分析任务已取消");
    //         }

    //         PutResult putResult = this.storageService.putObject(bioPipelineInputFile.getFilePath(), in);
    //         if (putResult.success()) {
    //             return this.handleUploaded(bioPipelineInputFile);
    //         }

    //         if (putResult.e() != null) {
    //             throw putResult.e();
    //         }
    //         return uploadFail();
    //     } catch (Exception e) {
    //         logger.error("input file id = {}. Expcetion happen when receving file", inputFileId, e);
    //         return uploadFail();
    //     } finally {
    //         this.uploadingLockSet.remove(inputFileId);
    //     }

    // }

    private int isInputAllowed(BioAnalysisPipeline pipeline, int inputType, String inputKey,
            List<BioPipelineInputFile> inputFiles) {

        BioPipelineInputFile inputFile = inputFiles.stream().filter(in -> in.getInputKey().equals(inputKey)).findAny()
                .orElse(null);
        if (inputFile != null) {
            return INPUT_NOT_ALLOWED_DUPLICATE_KEY;
        }
        if (inputType == PIPELINE_INPUT_TYPE_READ) {

            if (!SAMPLE_READ_KEY_PATTERN.matcher(inputKey).matches()) {
                return INPUT_NOT_ALLOWED_BAD_KEY;
            }

            String[] keys = inputKey.split("/");
            if (pipeline.getPipelineType() == PipelineService.PIPELINE_SNP_ANALYSIS) {
                return INPUT_ALLOWED;

            } else {
                String sampleId = keys[0];
                if (!"sample_0".equals(sampleId)) {
                    return INPUT_NOT_ALLOWED_INVALID_KEY;
                }
                return INPUT_ALLOWED;
            }
        }

        return INPUT_ALLOWED;

    }

    private String inputLockKey(long pipelineId, int inputType, String inputKey) {
        return String.format("pipeline-input-lock:%d::%d::%s", pipelineId, inputType, inputKey);
    }

    private String pipelineInputFilePath(String fileName, long pipelineId) {
        return "pipelineInput/" + pipelineId + "/" + fileName;
    }

    private String pipelineInputFilePath(long pipelineId, int inputType, String inputKey, String fileName) {
        String flatInputKey = inputKey.replace("/", "_");

        String storageFileName = String.format(
                "pipelineInput/p%d/%d/%s/%s",
                pipelineId,
                inputType,
                flatInputKey,
                fileName);

        return storageFileName;
    }

    public boolean lockDownPipelineUploading(long pipelineId) {
        Integer oldValue = this.pipelineUploadingLocks.putIfAbsent(pipelineId, -1);
        return oldValue == null;
    }

    // first must lock down
    public void unlockPipelineUploading(long pipelineId) {
        this.pipelineUploadingLocks.remove(pipelineId);

    }

    public Result<Long> createInputs(long pipelineId, String fileName, int inputType, String inputKey, InputStream in) {

        String storageKey = pipelineInputFilePath(pipelineId, inputType, inputKey, fileName);
        String lockKey = inputLockKey(pipelineId, inputType, inputKey);
        boolean getLockSuccess = this.uploadingLockSet.add(lockKey);

        if (!getLockSuccess) {
            return new Result(Result.DUPLICATE_OPERATION, -1l, "正在上传");
        }

        try {

            int pipelineUploadingCnt = this.pipelineUploadingLocks.compute(pipelineId, (id, cnt) -> {

                if (cnt == null) {
                    return 1;
                }
                if (cnt == -1) {
                    return cnt;
                }
                return cnt + 1;

            });

            if (pipelineUploadingCnt == -1) {
                return new Result<Long>(Result.BUSINESS_FAIL, -1l, "分析任务正在启动");
            }

            BioAnalysisPipelineExample bioAnalysisPipelineExample = new BioAnalysisPipelineExample();
            bioAnalysisPipelineExample.createCriteria().andPipelineIdEqualTo(pipelineId);
            Result<List<BioAnalysisPipeline>> pipelinesResult = this.pipelineService
                    .queryPipelines(bioAnalysisPipelineExample);
            if (pipelinesResult.getStatus() != Result.SUCCESS) {
                return new Result<Long>(pipelinesResult.getStatus(), -1l, pipelinesResult.getFailMsg());
            }

            List<BioAnalysisPipeline> pipelines = pipelinesResult.getData();
            if (pipelines == null || pipelines.isEmpty()) {
                return new Result(Result.BUSINESS_FAIL, -1l, "分析任务不存在");
            }

            BioAnalysisPipeline pipeline = pipelines.get(0);
            if (pipeline.getStatus() != PipelineService.PIPELINE_STATUS_PENDING) {
                return new Result(Result.BUSINESS_FAIL, -1l, "分析任务已经启动");
            }

            

            BioPipelineInputFileExample query = new BioPipelineInputFileExample();
            query.createCriteria().andPipelineIdEqualTo(pipelineId).andFileRoleEqualTo(inputType);
            List<BioPipelineInputFile> inputs = this.bioPipelineInputFileMapper.selectByExample(query);

            int inputAllowedCode = isInputAllowed(pipelines.get(0), inputType, inputKey, inputs);
            if (inputAllowedCode == INPUT_NOT_ALLOWED_DUPLICATE_KEY) {

                BioPipelineInputFile inputFile = inputs.stream().filter(pin -> pin.getInputKey().equals(inputKey))
                        .findAny().orElse(null);

                return new Result(RESPONSE_CODE_FILE_EXIST, inputFile.getInputFileId(), "上传文件已存在");
            }
            if (inputAllowedCode == INPUT_NOT_ALLOWED_BAD_KEY) {
                return new Result(RESPONSE_CODE_BAD_KEY, -1l, "上传key错误");
            }
            if (inputAllowedCode == INPUT_NOT_ALLOWED_INVALID_KEY) {
                return new Result(RESPONSE_CODE_INVALID_KEY, -1l, "当前分析流水线不允许使用该上传key");
            }

            PutResult putResult = this.storageService.putObject(storageKey, in);
            if (!putResult.success()) {
                logger.error(
                        "pipeline input upload failed, pipelineId={}, inputType={}, inputKey={}, fileName={}",
                        pipelineId,
                        inputType,
                        inputKey,
                        fileName,
                        putResult.e());
                return new Result(Result.INTERNAL_FAIL, -1l, "上传失败");
            }

            BioPipelineInputFile bioPipelineInputFile = new BioPipelineInputFile();
            bioPipelineInputFile.setFileName(fileName);
            bioPipelineInputFile.setFileRole(inputType);
            bioPipelineInputFile.setInputKey(inputKey);
            bioPipelineInputFile.setFilePath(storageKey);
            bioPipelineInputFile.setPipelineId(pipelineId);
            bioPipelineInputFile.setOriginalFilename(fileName);
            bioPipelineInputFile.setStatus(PIPELINE_INPUT_FILE_STATUS_UPLOADED);

            int insertRes = this.bioPipelineInputFileMapper.insertSelective(bioPipelineInputFile);
            if (insertRes <= 0) {
                this.storageService.delete(storageKey);
                return new Result(Result.INTERNAL_FAIL, -1l, "上传失败");
            }
            return new Result(Result.SUCCESS, bioPipelineInputFile.getInputFileId(), null);

        } catch (Exception e) {
            logger.error(
                    "pipeline input upload failed, pipelineId={}, inputType={}, inputKey={}, fileName={}",
                    pipelineId,
                    inputType,
                    inputKey,
                    fileName,
                    e);

            this.storageService.delete(storageKey);

            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "上传失败: 内部错误");

        } finally {
            this.uploadingLockSet.remove(lockKey);
            this.pipelineUploadingLocks.compute(pipelineId, (id, cnt) -> {

                if (cnt <= 1) {
                    return null;
                }
                return cnt - 1;
            });
        }

    }

    private static void copyTo(BioSample origin, BioSample to) {

        to.setSid(origin.getSid());
        to.setSampleType(origin.getSampleType());
        to.setSampleName(origin.getSampleName());
        to.setRead2Url(origin.getRead2Url());
        to.setRead2UploadStatus(origin.getRead2UploadStatus());
        to.setRead1UploadStatus(origin.getRead1UploadStatus());
        to.setRead1Url(origin.getRead1Url());
        to.setProjectId(origin.getProjectId());
        to.setIsPair(origin.getIsPair());
        to.setCreatedBy(origin.getCreatedBy());

    }

    private boolean isLongRead(String readUrl) throws IOException, Exception {
        boolean isLongRead = sampleReadLengthDetector.isLongRead(readUrl);
        return isLongRead;
    }

    // public Result<Boolean> receiveSampleData(long sid, int index, InputStream
    // datastream) {

    // String lockKey = sid + ":" + index;

    // final BioSample bioSample = new BioSample();
    // try {
    // int statusCode = transactionTemplate.execute(status -> {
    // BioSample selectedBioSample =
    // this.bioSampleExtensionMapper.selectByIdForUpdate(sid);
    // if (selectedBioSample == null) {
    // return -1;
    // }

    // // 这里是防止重复操作的
    // if ((index == 0 && (selectedBioSample.getRead1UploadStatus() ==
    // SAMPLE_UPLOAD_STATUS_UPLOADING
    // || selectedBioSample.getRead1UploadStatus() == SAMPLE_UPLOAD_STATUS_READY))
    // || (index == 1 && (selectedBioSample.getRead2UploadStatus() ==
    // SAMPLE_UPLOAD_STATUS_UPLOADING
    // || selectedBioSample.getRead2UploadStatus() == SAMPLE_UPLOAD_STATUS_READY)))
    // {
    // return 1;
    // }

    // BioSample updateSample = new BioSample();
    // updateSample.setSid(sid);

    // if (index == 0) {
    // updateSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    // selectedBioSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    // } else {
    // updateSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    // selectedBioSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    // }
    // int updateResult = 0;

    // try {
    // updateResult = this.sampleMapper.updateByPrimaryKeySelective(updateSample);
    // } catch (Exception e) {
    // status.setRollbackOnly();
    // return 2;
    // }
    // if (updateResult < 1) {
    // status.setRollbackOnly();
    // return 2;
    // }

    // copyTo(selectedBioSample, bioSample);
    // return 0;
    // });

    // if (statusCode == -1) {
    // return new Result<>(Result.BUSINESS_FAIL, false, "未找到对应样本");
    // }
    // if (statusCode == 1) {
    // return new Result<>(Result.DUPLICATE_OPERATION, false, "样本不能被重复上传");
    // }
    // if (statusCode == 2) {
    // return new Result<>(Result.INTERNAL_FAIL, false, "上传样本失败");
    // }
    // } catch (TransactionException transactionException) {
    // return new Result<Boolean>(Result.INTERNAL_FAIL, false, "上传失败");
    // }

    // String uploadToUrl = index == 0 ? bioSample.getRead1Url() :
    // bioSample.getRead2Url();
    // PutResult putResult = this.storageService.putObject(uploadToUrl, datastream);

    // if (!putResult.success()) {

    // if (index == 0) {
    // bioSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_ERROR);
    // } else {
    // bioSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_ERROR);
    // }

    // int updateResult = this.sampleMapper.updateByPrimaryKey(bioSample);
    // return new Result<>(Result.INTERNAL_FAIL, false, "上传失败");
    // }

    // BioSample updatedSample = transactionTemplate.execute(status -> {
    // BioSample curSample = this.bioSampleExtensionMapper.selectByIdForUpdate(sid);
    // BioSample updateSample = new BioSample();
    // updateSample.setSid(sid);
    // if (index == 0) {
    // updateSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
    // curSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
    // } else {
    // updateSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
    // curSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
    // }
    // int updateRes = this.sampleMapper.updateByPrimaryKeySelective(updateSample);

    // if (updateRes < 1) {
    // // 代表更新失败, 依赖定时任务来更新状态吧
    // if (index == 0) {
    // curSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    // } else {
    // curSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    // }
    // }
    // return curSample;
    // });

    // boolean canStartPipeline = updatedSample.getRead1UploadStatus() ==
    // SAMPLE_UPLOAD_STATUS_READY
    // && (!updatedSample.getIsPair() || updatedSample.getRead2UploadStatus() ==
    // SAMPLE_UPLOAD_STATUS_READY);

    // if (canStartPipeline) {
    // this.pipelineService.pipelineStart(sid);
    // }
    // return new Result<Boolean>(Result.SUCCESS, true, null);
    // }

    // private int tryInsertion(BioSample bioSample) {
    // return sampleMapper.insertSelective(bioSample);

    // }
}
