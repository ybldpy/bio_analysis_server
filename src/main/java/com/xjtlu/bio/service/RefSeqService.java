package com.xjtlu.bio.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.xjtlu.bio.entity.BioRefseqMeta;
import com.xjtlu.bio.mapper.BioRefseqMetaMapper;
import com.xjtlu.bio.service.StorageService.GetObjectResult;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;

@Service
public class RefSeqService {

    @Value("${refSeqService.refseqsDir}")
    private String refseqsDir;

    public final static int VIRUS_TYPE = 1;
    public final static int BACTERIA_TYPE = 2;

    @Resource
    private StorageService storageService;
    @Resource
    private BioRefseqMetaMapper refseqMetaMapper;


    @Value("${refSeqService.nonInnerRefSeqDir}")
    private String nonInnerRefseqDir;
    @Value("${refSeqService.temporaryPath}")
    private String refSeqServiceTmpPath;
    @Value("${refSeqService.refSeqIndexDir}")
    private String refSeqIndexDir;

    private Map<String, Object> virusIndex;

    private static final Logger logger = LoggerFactory.getLogger(RefSeqService.class);


    @PostConstruct
    public void init(){

        Path tmpParentDir = Path.of(this.refSeqServiceTmpPath);
        if(!Files.exists(tmpParentDir)){
            try {
                Files.createDirectories(tmpParentDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                logger.error("{} create tmp dir exception", tmpParentDir);
            }
        }
        Path nonInnerRefseqDir = Path.of(this.nonInnerRefseqDir);
        if(!Files.exists(nonInnerRefseqDir)){
            try {
                Files.createDirectories(nonInnerRefseqDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                logger.error("{} create tmp dir exception", nonInnerRefseqDir);
            }
        }

        Path refSeqIndexDir = Path.of(this.refSeqIndexDir);
        if(!Files.exists(refSeqIndexDir)){
            try {
                Files.createDirectories(refSeqIndexDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                logger.error("{} create tmp dir exception", refSeqIndexDir);
            }
        }
    }

    @Resource
    private AnalysisPipelineToolsConfig analysisPipelineToolsConfig;

    public boolean deleteRefSeq(String outterRefseqObjName) {

        String refseqObjNameInFlatFormat = outterRefseqObjName.replaceAll("/", "_");
        try {
            Files.delete(Paths.get(this.nonInnerRefseqDir, refseqObjNameInFlatFormat));
            return true;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
    }

    private File buildRefSeqIndex(String outputPath, File refseq) {

        // 组装命令：samtools faidx <ref.fa>
        Path tmpDir = null;
        try {
            
            tmpDir = Files.createTempDirectory(Path.of(this.refSeqServiceTmpPath), "indexTemp");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("create tmp dir excetpion", e);
            return null;
        }

        Path refseqPath = refseq.toPath();
        Path refseqLink = null;
        try {
            refseqLink = Files.createSymbolicLink(tmpDir.resolve(refseq.getName()), refseqPath);
        } catch (IOException e) {
            logger.error("{} create refseq link exception", refseqPath.toString(), e);
            try {
                Files.delete(tmpDir);
            } catch (IOException e1) {
                logger.error("{} delete dir excetpion", tmpDir.toString(), e1);
            }
            return null;
        }

        List<String> cmd = new ArrayList<>();
        cmd.addAll(analysisPipelineToolsConfig.getSamtools());
        cmd.add("faidx");
        cmd.add(refseqLink.toString());

        ProcessBuilder pb = new ProcessBuilder(cmd);
        // 让进程在参考文件所在目录跑，确保产物落同目录（更直观）
        pb.directory(tmpDir.toFile());
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);

        try {
            Process p = pb.start();
            int code = p.waitFor();
            if (code == 0) {
                // 有些版本会无视工作目录，兜底再检查一次默认位置
                Path indexFilePath = tmpDir.resolve(refseq.getName() + ".fai");
                if (!Files.exists(indexFilePath) || Files.size(indexFilePath) == 0) {
                    return null;
                }

                Path indexOutputPath = Path.of(outputPath);
                try {
                    Files.move(indexFilePath, indexOutputPath, StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException atomicMoveNotSupportedException) {
                    Files.move(indexFilePath, indexOutputPath, StandardCopyOption.REPLACE_EXISTING);
                }
                return indexOutputPath.toFile();
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException)
                Thread.currentThread().interrupt();
            return null;
        } finally {
            try {
                FileUtils.deleteDirectory(tmpDir.toFile());

            } catch (IOException e) {
                logger.error("{} delete dir excetpion", tmpDir.toString(), e);
            }
        }

        return null;
    }

    public File getRefSeqIndex(long refseqId) {

        if (Files.exists(Path.of(this.refSeqIndexDir + "/inner/" + refseqId + ".fai"))) {
            return Paths.get(this.refSeqIndexDir, "inner", String.valueOf(refseqId) + ".fai").toFile();
        }

        BioRefseqMeta refseqMeta = refseqMetaMapper.selectByPrimaryKey(refseqId);
        if (refseqMeta == null) {
            return null;
        }
        String refseqPath = refseqMeta.getPath();
        File refseqFile = new File(refseqPath);

        if (!refseqFile.exists() || refseqFile.length() <= 0) {
            return null;
        }
        return this.buildRefSeqIndex(
                Paths.get(this.refSeqIndexDir, "inner", String.valueOf(refseqId) + ".fai").toString(),
                refseqFile);
    }

    public File getRefSeqIndex(String outterRefSeqObjectName) {

        String objectNameInFlatFormat = outterRefSeqObjectName.replaceAll("/", "_");

        Path refseqIndexPath = Paths.get(
                this.refSeqIndexDir,
                "non_inner",
                objectNameInFlatFormat + ".fai");

        Path dirPath = refseqIndexPath.getParent();
        if (!Files.exists(dirPath)) {
            try {
                Files.createDirectories(dirPath);
            } catch (IOException e) {
                logger.error("create dir {} exception ", dirPath.toString(), e);
                return null;
            }
        }

        if (Files.exists(refseqIndexPath)) {
            if (refseqIndexPath.toFile().length() > 0)
                return refseqIndexPath.toFile();
        }

        Path outterRefSeqObjectLocalPath = Paths.get(
                this.nonInnerRefseqDir,
                objectNameInFlatFormat);

        if (!Files.exists(outterRefSeqObjectLocalPath)) {
            return null;
        }

        File refseqIndex = this.buildRefSeqIndex(
                refseqIndexPath.toString(),
                outterRefSeqObjectLocalPath.toFile());
        return refseqIndex;
    }

    public boolean exist(long refId) {
        BioRefseqMeta refseqMeta = refseqMetaMapper.selectByPrimaryKey(refId);
        return refseqMeta != null;
    }

    public File getRefSeqByRefSeqId(long refId) throws Exception {
        // todo
        BioRefseqMeta bioRefseqMeta = null;

        bioRefseqMeta = this.refseqMetaMapper.selectByPrimaryKey(refId);

        if (bioRefseqMeta == null) {
            return null;
        }

        String refPath = bioRefseqMeta.getPath();

        File f = new File(refseqsDir + "/" + refPath);
        return f.exists() ? f : null;
    }

    public File getRefseq(String refseqObjName) {
        String refseqObjNameInFlat = refseqObjName.replaceAll("/", "_");
        Path parentDir = Path.of(this.nonInnerRefseqDir);

        Path storePath = parentDir.resolve(refseqObjNameInFlat);

        if (Files.exists(storePath)) {
            return storePath.toFile();
        }

        GetObjectResult getObjectResult = this.storageService.getObject(refseqObjName, storePath.toString());
        if (!getObjectResult.success()) {
            return null;
        }

        return storePath.toFile();

    }

    public File getRefseq(long refseqId) {

        BioRefseqMeta bioRefseqMeta = this.refseqMetaMapper.selectByPrimaryKey(refseqId);

        if (bioRefseqMeta == null) {
            return null;
        }

        String path = bioRefseqMeta.getPath();
        int refseqType = bioRefseqMeta.getOrgType();

        if (refseqType == VIRUS_TYPE) {
            File f = new File(this.refseqsDir + "/" + path);
            if (f.exists() && f.length() > 0) {
                return f;
            }
            return null;
        } else if (refseqType == BACTERIA_TYPE) {
            // todo
            return null;
        }

        return null;
    }

}
