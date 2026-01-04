package com.xjtlu.bio.service;

import java.io.IOException;
import java.io.InputStream;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.ibatis.transaction.TransactionException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioSampleExtensionMapper;
import com.xjtlu.bio.mapper.BioSampleMapper;
import com.xjtlu.bio.service.StorageService.PutResult;
import com.xjtlu.bio.utils.SampleReadLengthDetector;

import jakarta.annotation.Resource;

@Service
public class SampleService {

    @Resource
    private BioSampleMapper sampleMapper;
    @Resource
    private BioSampleExtensionMapper bioSampleExtensionMapper;
    @Resource
    private PipelineService pipelineService;
    @Resource
    private TransactionTemplate transactionTemplate;
    @Resource
    private SampleReadLengthDetector sampleReadLengthDetector;
    

    @Resource
    private StorageService storageService;

    public static final int SAMPLE_TYPE_VIRUS = 0;
    public static final int SAMPLE_TYPE_BACTERIA = 1;
    public static final int SAMPLE_TYPE_VIRUS_COVID = 2;

    public static final int SAMPLE_UPLOAD_STATUS_NOT_UPLOAD = 0;
    public static final int SAMPLE_UPLOAD_STATUS_UPLOADING = 1;
    public static final int SAMPLE_UPLOAD_STATUS_READY = 2;
    public static final int SAMPLE_UPLOAD_STATUS_ERROR = 3;

    private Set<String> bioSampleOpeartion = ConcurrentHashMap.newKeySet();
    private Set<String> bioSampleCreationSet = ConcurrentHashMap.newKeySet();

    private static String substractPostfixFromFileName(String filename) {
        return null;
    }

    @Transactional
    public Result<BioSample> createSample(boolean isPair, String sampleName, long projectId, int sampleType,
            String read1FileOriginalName, String read2FileOriginalName, Map<String, Object> pipelineStageParams) {

        BioSample bioSample = new BioSample();
        bioSample.setIsPair(isPair);
        bioSample.setSampleName(sampleName);
        bioSample.setProjectId(projectId);
        bioSample.setSampleType(sampleType);
        bioSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_NOT_UPLOAD);
        bioSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_NOT_UPLOAD);

        int res = 0;
        try {
            res = tryInsertion(bioSample);
        } catch (DuplicateKeyException duplicateKeyException) {
            return new Result<BioSample>(Result.BUSINESS_FAIL, null, "样本名称重复");
        }

        res = 0;

        long sid = bioSample.getSid();
        String r1Name = "r1." + substractPostfixFromFileName(read1FileOriginalName);
        String r2Name = isPair ? "r2." + substractPostfixFromFileName(read2FileOriginalName) : null;

        String r1Url = String.format("samples/%d/%s", sid, r1Name);
        String r2Url = isPair ? String.format("samples/%d/%s", sid, r2Name) : null;

        bioSample.setRead1Url(r1Url);
        bioSample.setRead2Url(r2Url);

        res = this.sampleMapper.updateByPrimaryKey(bioSample);
        if (res < 1) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<BioSample>(Result.INTERNAL_FAIL, null, "创建样本失败");
        }

        res = 0;
        if (res == 1) {
            Result<Long> createPipelineResult = pipelineService.createPipeline(bioSample, pipelineStageParams);
            if (createPipelineResult.getStatus() != Result.SUCCESS) {
                // do rollback
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                return new Result<BioSample>(Result.INTERNAL_FAIL, null, "创建样本失败");
            }
            return new Result<BioSample>(Result.SUCCESS, bioSample, null);
        }
        return new Result<BioSample>(Result.BUSINESS_FAIL, bioSample, "创建样本失败");
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


    private boolean isLongRead(String readUrl) throws IOException, Exception{
        boolean isLongRead = sampleReadLengthDetector.isLongRead(readUrl);
        return isLongRead;
    }

    public Result<Boolean> receiveSampleData(long sid, int index, InputStream datastream) {

        String lockKey = sid + ":" + index;

        final BioSample bioSample = new BioSample();
        try {
            int statusCode = transactionTemplate.execute(status -> {
                BioSample selectedBioSample = this.bioSampleExtensionMapper.selectByIdForUpdate(sid);
                if (selectedBioSample == null) {
                    return -1;
                }

                // 这里是防止重复操作的
                if ((index == 0 && (selectedBioSample.getRead1UploadStatus() == SAMPLE_UPLOAD_STATUS_UPLOADING
                        || selectedBioSample.getRead1UploadStatus() == SAMPLE_UPLOAD_STATUS_READY))
                        || (index == 1 && (selectedBioSample.getRead2UploadStatus() == SAMPLE_UPLOAD_STATUS_UPLOADING
                                || selectedBioSample.getRead2UploadStatus() == SAMPLE_UPLOAD_STATUS_READY))) {
                    return 1;
                }


                BioSample updateSample = new BioSample();
                updateSample.setSid(sid);



                if (index == 0) {
                    updateSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
                    selectedBioSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
                } else {

                    updateSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
                    selectedBioSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
                }
                int updateResult = 0;

                try {
                    updateResult = this.sampleMapper.updateByPrimaryKeySelective(updateSample);
                } catch (Exception e) {
                    status.setRollbackOnly();
                    return 2;
                }
                if (updateResult < 1) {
                    status.setRollbackOnly();
                    return 2;
                }

                copyTo(selectedBioSample, bioSample);
                return 0;
            });

            if (statusCode == -1) {
                return new Result<>(Result.BUSINESS_FAIL, false, "未找到对应样本");
            }
            if (statusCode == 1) {
                return new Result<>(Result.DUPLICATE_OPERATION, false, "样本不能被重复上传");
            }
            if (statusCode == 2) {
                return new Result<>(Result.INTERNAL_FAIL, false, "上传样本失败");
            }
        } catch (TransactionException transactionException) {
            return new Result<Boolean>(Result.INTERNAL_FAIL, false, "上传失败");
        }

        String uploadToUrl = index == 0 ? bioSample.getRead1Url() : bioSample.getRead2Url();
        PutResult putResult = this.storageService.putObject(uploadToUrl, datastream);

        if (!putResult.success()) {

            if (index == 0) {
                bioSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_ERROR);
            } else {
                bioSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_ERROR);
            }

            int updateResult = this.sampleMapper.updateByPrimaryKey(bioSample);
            return new Result<>(Result.INTERNAL_FAIL, false, "上传失败");
        }


        
        
        BioSample updatedSample = transactionTemplate.execute(status -> {
            BioSample curSample = this.bioSampleExtensionMapper.selectByIdForUpdate(sid);
            BioSample updateSample = new BioSample();
            updateSample.setSid(sid);
            if (index == 0) {
                updateSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
                curSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
            } else {
                updateSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
                curSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_READY);
            }
            int updateRes = this.sampleMapper.updateByPrimaryKeySelective(updateSample);

            if (updateRes < 1) {
                // 代表更新失败, 依赖定时任务来更新状态吧
                if(index == 0){
                    curSample.setRead1UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
                }else {
                    curSample.setRead2UploadStatus(SAMPLE_UPLOAD_STATUS_UPLOADING);
                }
            }
            return curSample;
        });



        boolean canStartPipeline = updatedSample.getRead1UploadStatus() == SAMPLE_UPLOAD_STATUS_READY && (!updatedSample.getIsPair() || updatedSample.getRead2UploadStatus() == SAMPLE_UPLOAD_STATUS_READY);


        if(canStartPipeline){
            this.pipelineService.pipelineStart(sid);
        }
        return new Result<Boolean>(Result.SUCCESS, true, null);
    }

    private int tryInsertion(BioSample bioSample) {
        return sampleMapper.insertSelective(bioSample);
        
    }
}
