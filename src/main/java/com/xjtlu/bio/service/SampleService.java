package com.xjtlu.bio.service;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.mapper.BioSampleMapper;

import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import jakarta.annotation.Resource;

@Service
public class SampleService {

    @Resource
    private BioSampleMapper sampleMapper;

    @Resource
    private MinioService minioService;

    public static final int SAMPLE_TYPE_VIRUS = 0;
    public static final int SAMPLE_TYPE_BACTERIA = 1;
    public static final int SAMPLE_TYPE_VIRUS_COVID = 2;

    private Set<String> bioSampleUploadStatusSet = ConcurrentHashMap.newKeySet();
    
    @Transactional
    public Result<BioSample> createSample(boolean isPair, String sampleName, long projectId, long pipelineId,int sampleType) {


        BioSample bioSample = new BioSample();
        bioSample.setIsPair(isPair);
        bioSample.setSampleName(sampleName);
        bioSample.setProjectId(projectId);
        bioSample.setSampleType(sampleType);
        bioSample.setPipelineId(pipelineId);
        // BioSample previousSample = sampleMapper.lockRowByPipeline(bioSample);
        // if (previousSample != null) {
        //     return new Result<Integer>(Result.DUPLICATE_OPERATION, -1, "不能重复绑定分析流水线");
        // }
        
        int res = tryInsertion(bioSample);
        if (res == 1) {
            return new Result<BioSample>(Result.SUCCESS, bioSample, null);
        }
        return new Result<BioSample>(Result.BUSINESS_FAIL, bioSample, "样本名称已存在于该项目中");
    }

    // @Transactional(rollbackFor = Exception.class, isolation =
    // org.springframework.transaction.annotation.Isolation.READ_COMMITTED)
    public Result receiveSampleData(long sid, int index, InputStream datastream) {


        if (!minioService.isMinioOk()) {
            return new Result<>(2, null, "内部错误");
        }
        BioSample bioSample = sampleMapper.selectByPrimaryKey(sid);

        if (bioSample == null) {
            return new Result<>(Result.BUSINESS_FAIL, null, "样本不存在");
        }

        String uploadSample = String.format("%d-%d", bioSample.getSid(), index);
        if (!bioSampleUploadStatusSet.add(uploadSample)) {
            return new Result<>(Result.DUPLICATE_OPERATION, null, "重复操作");
        }

        String setUrl = String.format("samples/%d/%d", bioSample.getSid(), index);

        try {
            minioService.uploadObject(setUrl, datastream);
        } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                | IOException e) {
            // TODO Auto-generated catch block
            return new Result<>(2, null, "内部错误_" + e.getMessage());
        }finally{
            bioSampleUploadStatusSet.remove(setUrl);
        }

        BioSample updateSample = new BioSample();
        updateSample.setSid(bioSample.getSid());
        if (index == 0) {
            updateSample.setRead1Url(setUrl);
        }else {
            updateSample.setRead2Url(setUrl);
        }
        int res = this.sampleMapper.updateByPrimaryKeySelective(updateSample);
        if (res == 0) {
            try {
                minioService.removeObject(setUrl);
            } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                    | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                    | IOException e) {
                //todo post a message and try later
                return new Result<>(2, null, "内部错误_"+e.getMessage());
            }
        }
        return new Result<>(0, null, null);
    }

    private int tryInsertion(BioSample bioSample) {

        try {
            sampleMapper.insertSelective(bioSample);
            return 1;
        } catch (DuplicateKeyException e) {
            return 0;
        } catch (Exception e) {
            //todo: log
            throw new RuntimeException("内部错误");
        }
    }

    public void testInsertDuplicate(String sampleName, int projectId) {
        BioSample bioSample = new BioSample();
        bioSample.setSampleName(sampleName);
        bioSample.setProjectId(projectId);
        bioSample.setIsPair(false);

        try {
            // System.out.println("a");
            sampleMapper.insertSelective(bioSample);
            // System.out.println("b");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
