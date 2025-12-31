package com.xjtlu.bio.service;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.xjtlu.bio.entity.BioRefseqMeta;
import com.xjtlu.bio.mapper.BioRefseqMetaMapper;
import com.xjtlu.bio.service.StorageService.GetObjectResult;

import jakarta.annotation.Resource;

@Service
public class RefSeqService {

    @Value("${refSeq.virus}")
    private String virusPath;

    @Value("${refSeq.virus-index}")
    private String virusIndexPath;


    public final static int VIRUS_TYPE = 1;
    public final static int BACTERIA_TYPE = 2;


    


    @Resource
    private StorageService storageService;

    private String nonInnerRefseqDir;

    @Resource
    private BioRefseqMetaMapper refseqMetaMapper;

    private String refSeqServiceTmpPath;

    private String refSeqIndexDir;

    private Map<String, Object> virusIndex;

    private String samtools;

    private static final String ACCESSION_KEY = "accession";





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

    public File buildRefSeqIndex(String outputPath, File refseq) {
        
        Path refSeqIndexTmpDir = Paths.get(this.refSeqServiceTmpPath, String.valueOf(System.currentTimeMillis()));
        Path refseqLink = null;

        try {
            Files.createDirectories(refSeqIndexTmpDir);
            refseqLink = Files.createSymbolicLink(refSeqIndexTmpDir.resolve(refSeqIndexTmpDir.resolve(refseq.getName())), refseq.toPath());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        // 组装命令：samtools faidx <ref.fa>
        List<String> cmd = new ArrayList<>();
        cmd.add(samtools);
        cmd.add("faidx");
        cmd.add(refseqLink.toAbsolutePath().toString());
        
        ProcessBuilder pb = new ProcessBuilder(cmd);
        // 让进程在参考文件所在目录跑，确保产物落同目录（更直观）
        pb.directory(refSeqIndexTmpDir.toFile());
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);

        try {
            Process p = pb.start();
            int code = p.waitFor();
            
            if (code == 0) {
                // 有些版本会无视工作目录，兜底再检查一次默认位置
                Path indexFilePath = Path.of(refseqLink.toString()+".fai");
                
                if (!Files.exists(indexFilePath) || Files.size(indexFilePath) == 0){
                    return null;
                }
                
                Files.move(indexFilePath, Path.of(outputPath), java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                refSeqIndexTmpDir.toFile().delete();
                return new File(outputPath);
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException)
                Thread.currentThread().interrupt();
            return null;
        }

        return null;
    }



    public File getRefSeqIndex(long refseqId) {

        if(Files.exists(Path.of(this.refSeqIndexDir + "/inner/" + refseqId+".fai"))){
            return Paths.get(this.refSeqIndexDir, "inner", String.valueOf(refseqId)+".fai").toFile();
        }

        BioRefseqMeta refseqMeta = refseqMetaMapper.selectByPrimaryKey(refseqId);
        if(refseqMeta == null){
            return null;
        }
        String refseqPath = refseqMeta.getPath();
        File refseqFile = new File(refseqPath);

        if(!refseqFile.exists() || refseqFile.length() <=0){
            return null;
        }
        return this.buildRefSeqIndex(
            Paths.get(this.refSeqIndexDir, "inner", String.valueOf(refseqId)+".fai").toString(),
            refseqFile
        );
    }


    public File getRefSeqIndex(String outterRefSeqObjectName){

        String objectNameInFlatFormat = outterRefSeqObjectName.replaceAll("/", "_");

        Path refseqIndexPath = Paths.get(
            this.refSeqIndexDir, 
            "non_inner", 
            objectNameInFlatFormat,
            "refseq.fai"
        );


        
        if(Files.exists(refseqIndexPath) && refseqIndexPath.toFile().length() > 0){
            return refseqIndexPath.toFile();
        }

        Path outterRefSeqObjectLocalPath = Paths.get(
            this.nonInnerRefseqDir, 
            objectNameInFlatFormat
        );

        if(!Files.exists(outterRefSeqObjectLocalPath)){
            return null;
        }

        Path outterRefSeqObjectPath = null;
        try {
            outterRefSeqObjectPath = Files.createSymbolicLink(refseqIndexPath, outterRefSeqObjectLocalPath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }

        File refseqIndex = this.buildRefSeqIndex(
            refseqIndexPath.toString(),
            outterRefSeqObjectPath.toFile()
        );
        return refseqIndex;
    }

    public boolean exist(long refId){
        BioRefseqMeta refseqMeta = refseqMetaMapper.selectByPrimaryKey(refId);
        return refseqMeta!=null;
    }

    public File getRefSeqByRefSeqId(long refId) throws Exception{
        // todo
        BioRefseqMeta bioRefseqMeta = null;
        
        bioRefseqMeta = this.refseqMetaMapper.selectByPrimaryKey(refId);
        
        if(bioRefseqMeta == null){
            return null;
        }

        String refPath = bioRefseqMeta.getPath();

        File f = new File(virusPath + "/"+refPath);
        return f.exists()?f:null;
    }


    public File getRefseq(String refseqObjName){
        String refseqObjNameInFlat = refseqObjName.replaceAll("/", "_");
        Path storePath = Paths.get(this.nonInnerRefseqDir, refseqObjNameInFlat);
        if(Files.exists(storePath)){
            return storePath.toFile();
        }
        GetObjectResult getObjectResult = this.storageService.getObject(refseqObjName, storePath.toString());
        if(!getObjectResult.success()){
            return null;
        }

        return storePath.toFile();
        
    }


    public File getRefseq(long refseqId){
        
        BioRefseqMeta bioRefseqMeta = this.refseqMetaMapper.selectByPrimaryKey(refseqId);

        if(bioRefseqMeta == null){return null;}

        String path = bioRefseqMeta.getPath();
        int refseqType = bioRefseqMeta.getOrgType();

        if(refseqType == VIRUS_TYPE){
            File f = new File(this.virusPath + "/" + path);
            if(f.exists() && f.length() > 0){
                return f;
            }
            return null;
        }else if(refseqType == BACTERIA_TYPE){
            //todo
            return null;
        }

        return null;
    }

    



    private String queryRefSeqPath(String queryRefSeqAccession) {
        Object o = virusIndex.get(queryRefSeqAccession);
        if (o == null) {
            return null;
        }
        return String.format("%s/%s", virusPath, queryRefSeqAccession);
    }

}
