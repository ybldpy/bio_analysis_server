package com.xjtlu.bio.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.transaction.TransactionException;
import org.hibernate.validator.internal.util.stereotypes.Lazy;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;

import com.xjtlu.bio.common.Result;
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

    @Resource
    private BioPipelineInputFileMapper bioPipelineInputFileMapper;
    @Resource
    private PipelineService pipelineService;
    @Resource
    private SqlSessionTemplate batchSqlSessionTemplate;
    @Resource
    private SampleReadLengthDetector sampleReadLengthDetector;
    private Set<Integer> uploadingFiles = ConcurrentHashMap.newKeySet();

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

    private Set<Long> uploadingLockSet = ConcurrentHashMap.newKeySet();


    public List<BioPipelineInputFile> queryInputs(BioPipelineInputFileExample example){

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

        this.pipelineService.handleInputRecevied(bioPipelineInputFile.getPipelineId());

        return new Result<Void>(Result.SUCCESS, null, null);

    }

    private Result<Void> uploadFail() {
        return new Result<Void>(Result.INTERNAL_FAIL, null, "上传失败");
    }

    public Result<Void> recevieInput(long inputFileId, InputStream in) {

        try {
            boolean getLockSuccess = this.uploadingLockSet.add(inputFileId);
            if (!getLockSuccess) {
                return new Result(Result.DUPLICATE_OPERATION, null, "文件正在上传");
            }

            BioPipelineInputFile bioPipelineInputFile = this.bioPipelineInputFileMapper.selectByPrimaryKey(inputFileId);

            if (bioPipelineInputFile == null) {
                return new Result<Void>(Result.BUSINESS_FAIL, null, "未找到目标文件，不存在的分析任务");
            }
            else if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_NOT_UPLOAD) {
                String path = bioPipelineInputFile.getFilePath();
                if (this.storageService.exists(path)) {
                    return this.handleUploaded(bioPipelineInputFile);
                }
            }
            else if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_UPLOADED) {

                this.pipelineService.handleInputRecevied(bioPipelineInputFile.getPipelineId());
                return new Result<Void>(Result.SUCCESS, null, null);
            }
            else if (bioPipelineInputFile.getStatus() == PIPELINE_INPUT_FILE_STATUS_CANCELLED) {
                return new Result<Void>(Result.BUSINESS_FAIL, null, "分析任务已取消");
            }

            PutResult putResult = this.storageService.putObject(bioPipelineInputFile.getFilePath(), in);
            if (putResult.success()) {
                return this.handleUploaded(bioPipelineInputFile);
            }

            if (putResult.e() != null) {
                throw putResult.e();
            }
            return uploadFail();
        } catch (Exception e) {
            logger.error("input file id = {}. Expcetion happen when receving file", inputFileId, e);
            return uploadFail();
        } finally {
            this.uploadingLockSet.remove(inputFileId);
        }

    }


    private String pipelineInputFilePath(String fileName, long pipelineId){
        return "pipelineInput/"+pipelineId+"/"+fileName;
    }

    @Transactional
    public Result createInputs(List<Integer> inputRoles, List<String> fileOriginalName, long associatedPipelineId) {

        List<BioPipelineInputFile> bioPipelineInputFiles = new ArrayList<>();



        for (int i = 0; i < inputRoles.size(); i++) {
            BioPipelineInputFile bioPipelineInputFile = new BioPipelineInputFile();
            bioPipelineInputFile.setPipelineId(associatedPipelineId);
            bioPipelineInputFile.setOriginalFilename(fileOriginalName.get(i));
            bioPipelineInputFile.setFileName(fileOriginalName.get(i));
            bioPipelineInputFile.setStatus(PIPELINE_INPUT_FILE_STATUS_NOT_UPLOAD);
            bioPipelineInputFile.setFileRole(inputRoles.get(i));
            bioPipelineInputFiles.add(bioPipelineInputFile);
            String filePath = pipelineInputFilePath(fileOriginalName.get(i), associatedPipelineId);
            bioPipelineInputFile.setFilePath(filePath);
        }

        try {
            

            for (BioPipelineInputFile bioPipelineInputFile : bioPipelineInputFiles) {
                int res = this.bioPipelineInputFileMapper.insertSelective(bioPipelineInputFile);
                if(res <= 0 ){
                    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                    return new Result(Result.INTERNAL_FAIL, null, "创建输入失败");
                }
            }

            return new Result(Result.SUCCESS, null, null);
            

        } catch (Exception e) {
            logger.error("Pipeline id = {} Inserting input file exception", associatedPipelineId, e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result(Result.INTERNAL_FAIL, null, "创建输入失败");
        }

        // BioSample bioSample = new BioSample();
        // bioSample.setIsPair(isPair);
        // bioSample.setSampleName(sampleName);
        // bioSample.setProjectId(projectId);
        // bioSample.setSampleType(sampleType);
        // bioSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_NOT_UPLOAD);
        // bioSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_NOT_UPLOAD);

        // int res = 0;
        // try {
        // res = tryInsertion(bioSample);
        // } catch (DuplicateKeyException duplicateKeyException) {
        // return new Result<BioSample>(Result.BUSINESS_FAIL, null, "样本名称重复");
        // }

        // res = 0;

        // long sid = bioSample.getSid();
        // String r1Name = "r1." + substractPostfixFromFileName(read1FileOriginalName);
        // String r2Name = isPair ? "r2." +
        // substractPostfixFromFileName(read2FileOriginalName) : null;

        // String r1Url = String.format("samples/%d/%s", sid, r1Name);
        // String r2Url = isPair ? String.format("samples/%d/%s", sid, r2Name) : null;

        // BioSample updateSample = new BioSample();
        // updateSample.setSid(sid);
        // updateSample.setRead1Url(r1Url);
        // updateSample.setRead2Url(r2Url);

        // res = this.sampleMapper.updateByPrimaryKeySelective(updateSample);
        // if (res < 1) {
        // TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        // return new Result<BioSample>(Result.INTERNAL_FAIL, null, "创建样本失败");
        // }

        // bioSample.setRead1Url(r1Url);
        // bioSample.setRead2Url(r2Url);

        // Result<Long> createPipelineResult = pipelineService.createPipeline(bioSample,
        // pipelineStageParams);
        // if(createPipelineResult.getStatus()!=Result.SUCCESS){
        // TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        // return new Result<BioSample>(createPipelineResult.getStatus(), null,
        // createPipelineResult.getFailMsg());
        // }

        // return new Result<BioSample>(Result.SUCCESS, bioSample, null);

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

    // public Result<Boolean> receiveSampleData(long sid, int index, InputStream datastream) {

    //     String lockKey = sid + ":" + index;

    //     final BioSample bioSample = new BioSample();
    //     try {
    //         int statusCode = transactionTemplate.execute(status -> {
    //             BioSample selectedBioSample = this.bioSampleExtensionMapper.selectByIdForUpdate(sid);
    //             if (selectedBioSample == null) {
    //                 return -1;
    //             }

    //             // 这里是防止重复操作的
    //             if ((index == 0 && (selectedBioSample.getRead1UploadStatus() == SAMPLE_UPLOAD_STATUS_UPLOADING
    //                     || selectedBioSample.getRead1UploadStatus() == SAMPLE_UPLOAD_STATUS_READY))
    //                     || (index == 1 && (selectedBioSample.getRead2UploadStatus() == SAMPLE_UPLOAD_STATUS_UPLOADING
    //                             || selectedBioSample.getRead2UploadStatus() == SAMPLE_UPLOAD_STATUS_READY))) {
    //                 return 1;
    //             }

    //             BioSample updateSample = new BioSample();
    //             updateSample.setSid(sid);

    //             if (index == 0) {
    //                 updateSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    //                 selectedBioSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    //             } else {
    //                 updateSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    //                 selectedBioSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    //             }
    //             int updateResult = 0;

    //             try {
    //                 updateResult = this.sampleMapper.updateByPrimaryKeySelective(updateSample);
    //             } catch (Exception e) {
    //                 status.setRollbackOnly();
    //                 return 2;
    //             }
    //             if (updateResult < 1) {
    //                 status.setRollbackOnly();
    //                 return 2;
    //             }

    //             copyTo(selectedBioSample, bioSample);
    //             return 0;
    //         });

    //         if (statusCode == -1) {
    //             return new Result<>(Result.BUSINESS_FAIL, false, "未找到对应样本");
    //         }
    //         if (statusCode == 1) {
    //             return new Result<>(Result.DUPLICATE_OPERATION, false, "样本不能被重复上传");
    //         }
    //         if (statusCode == 2) {
    //             return new Result<>(Result.INTERNAL_FAIL, false, "上传样本失败");
    //         }
    //     } catch (TransactionException transactionException) {
    //         return new Result<Boolean>(Result.INTERNAL_FAIL, false, "上传失败");
    //     }

    //     String uploadToUrl = index == 0 ? bioSample.getRead1Url() : bioSample.getRead2Url();
    //     PutResult putResult = this.storageService.putObject(uploadToUrl, datastream);

    //     if (!putResult.success()) {

    //         if (index == 0) {
    //             bioSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_ERROR);
    //         } else {
    //             bioSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_ERROR);
    //         }

    //         int updateResult = this.sampleMapper.updateByPrimaryKey(bioSample);
    //         return new Result<>(Result.INTERNAL_FAIL, false, "上传失败");
    //     }

    //     BioSample updatedSample = transactionTemplate.execute(status -> {
    //         BioSample curSample = this.bioSampleExtensionMapper.selectByIdForUpdate(sid);
    //         BioSample updateSample = new BioSample();
    //         updateSample.setSid(sid);
    //         if (index == 0) {
    //             updateSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
    //             curSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
    //         } else {
    //             updateSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
    //             curSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
    //         }
    //         int updateRes = this.sampleMapper.updateByPrimaryKeySelective(updateSample);

    //         if (updateRes < 1) {
    //             // 代表更新失败, 依赖定时任务来更新状态吧
    //             if (index == 0) {
    //                 curSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    //             } else {
    //                 curSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
    //             }
    //         }
    //         return curSample;
    //     });

    //     boolean canStartPipeline = updatedSample.getRead1UploadStatus() == SAMPLE_UPLOAD_STATUS_READY
    //             && (!updatedSample.getIsPair() || updatedSample.getRead2UploadStatus() == SAMPLE_UPLOAD_STATUS_READY);

    //     if (canStartPipeline) {
    //         this.pipelineService.pipelineStart(sid);
    //     }
    //     return new Result<Boolean>(Result.SUCCESS, true, null);
    // }

    // private int tryInsertion(BioSample bioSample) {
    //     return sampleMapper.insertSelective(bioSample);

    // }
}
