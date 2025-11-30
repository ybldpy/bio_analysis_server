package com.xjtlu.bio.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.entity.BioRefseqMeta;
import com.xjtlu.bio.entity.BioRefseqMetaExample;
import com.xjtlu.bio.mapper.BioRefseqMetaMapper;

import jakarta.annotation.Resource;

@Service
public class RefSeqService {

    @Value("${refSeq.virus}")
    private String virusPath;

    @Value("${refSeq.virus-index}")
    private String virusIndexPath;


    @Resource
    private BioRefseqMetaMapper refseqMetaMapper;

    private String refSeqServiceTmpPath;

    private String refSeqIndexDir;

    private Map<String, Object> virusIndex;

    private String samtools;

    private static final String ACCESSION_KEY = "accession";

    public RefSeqService() {

    }

    public File buildRefSeqIndex(String refSeqAccession) {
        File f = this.getRefSeqIndex(refSeqAccession);
        if (f == null || !f.exists()) {
            return null;
        }

        File fai = new File(this.refSeqIndexDir + "/" + refSeqAccession + "fa.fai");
        if (fai.exists() && fai.length() > 0) {
            return fai;
        }

        Path refSeqTmpDir = Paths.get(this.refSeqServiceTmpPath);
        try {
            Files.createDirectories(refSeqTmpDir);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        // 组装命令：samtools faidx <ref.fa>
        List<String> cmd = new ArrayList<>();
        cmd.add(samtools);
        cmd.add("faidx");
        cmd.add(f.getAbsolutePath());

        ProcessBuilder pb = new ProcessBuilder(cmd);
        // 让进程在参考文件所在目录跑，确保产物落同目录（更直观）
        pb.directory(f.getParentFile());
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);

        try {
            Process p = pb.start();
            int code = p.waitFor();
            if (code != 0) {
                // 有些版本会无视工作目录，兜底再检查一次默认位置
                if (fai.exists() && fai.length() > 0)
                    return fai;
                return null;
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException)
                Thread.currentThread().interrupt();
            return null;
        }

        // 校验 .fai 是否生成
        if (!fai.exists() || fai.length() == 0)
            return null;

        // 若参考是 .gz，可以顺带看下 .gzi（可选，不强制）
        // File gzi = new File(f.getAbsolutePath() + ".gzi");

        return fai;

    }

    public File getRefSeqIndex(String refSeqAccession) {

        
    }

    public File getRefSeqByRefSeqIf(long refId) throws Exception{
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

    



    private String queryRefSeqPath(String queryRefSeqAccession) {
        Object o = virusIndex.get(queryRefSeqAccession);
        if (o == null) {
            return null;
        }
        return String.format("%s/%s", virusPath, queryRefSeqAccession);
    }

}
