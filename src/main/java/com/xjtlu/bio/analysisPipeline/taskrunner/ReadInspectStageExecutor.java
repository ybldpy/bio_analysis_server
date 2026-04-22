package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.ReadMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;

class FastQIO{




    public static boolean isGzip(Path p){
        String fileName = p.getFileName().toString();
        return fileName.endsWith(".gz");
    }

    public static BufferedReader getReader(Path in) throws IOException{

        InputStream is = Files.newInputStream(in);

        if (isGzip(in)) {
            is = new GZIPInputStream(is);
        }

        return new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8)
        );
    }

    public static BufferedWriter getWriter(Path out) throws IOException{


        OutputStream os = Files.newOutputStream(out);

        if(isGzip(out)){
            os = new GZIPOutputStream(os);
        }

        return new BufferedWriter(
            new OutputStreamWriter(os)
        );
    }
}




@Component
public class ReadInspectStageExecutor
        extends AbstractPipelineStageExector<ReadInspectStageOutput, ReadInspectStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<ReadInspectStageOutput> {

    @Override
    protected Class<ReadInspectStageInputUrls> stageInputType() {
        // TODO Auto-generated method stub
        return ReadInspectStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        // TODO Auto-generated method stub
        return BaseStageParams.class;
    }

    private int[] checkQualityEncodingAndReadLen(Path p) throws IOException {

        int encoding = ReadMeta.QUALITY_ENCODING_33;
        int readLenType = ReadMeta.READ_LEN_TYPE_SHORT;

        int minQual = Integer.MAX_VALUE;
        int maxQual = Integer.MIN_VALUE;

        int totalLen = 0;
        int count = 0;

        try (BufferedReader br = Files.newBufferedReader(p)) {

            String line;
            int lineIndex = 0;

            while ((line = br.readLine()) != null && count < 1000) {

                int mod = lineIndex % 4;

                // sequence line
                if (mod == 1) {
                    totalLen += line.length();
                    count++;
                }

                // quality line
                if (mod == 3) {
                    for (char c : line.toCharArray()) {
                        int v = (int) c;
                        minQual = Math.min(minQual, v);
                        maxQual = Math.max(maxQual, v);
                    }
                }

                lineIndex++;
            }
        }

        // 判断 encoding
        if (minQual >= 33 && maxQual <= 74) {
            encoding = ReadMeta.QUALITY_ENCODING_33;
        } else if (minQual >= 64) {
            encoding = ReadMeta.QUALITY_ENCODING_64;
        }

        // 判断 read length
        if (count > 0) {
            int avgLen = totalLen / count;

            if (avgLen >= 500) {
                readLenType = ReadMeta.READ_LEN_TYPE_LONG;
            } else {
                readLenType = ReadMeta.READ_LEN_TYPE_SHORT;
            }
        }

        return new int[] { encoding, readLenType };
    }

    private static class SplitInterleavedResult {
        Path r1;
        Path r2;
        boolean success;
        boolean brokenInput;

        public SplitInterleavedResult(Path r1, Path r2, boolean success, boolean brokenInput) {
            this.r1 = r1;
            this.r2 = r2;
            this.success = success;
            this.brokenInput = brokenInput;
        }

    }

    private SplitInterleavedResult splitIfLeaved(Path read, Path workDir) {

        String fileName = read.getFileName().toString();
        String baseName = fileName;
        boolean decompressed = false;
        String format = null;
        if (baseName.endsWith(".fastq.gz")) {
            decompressed = true;
            baseName = baseName.substring(0, baseName.length() - ".fastq.gz".length());
            format = ".fastq.gz";
        } else if (baseName.endsWith(".fq.gz")) {
            decompressed = true;
            baseName = baseName.substring(0, baseName.length() - ".fq.gz".length());
            format = ".fq.gz";
        } else if (baseName.endsWith(".fastq")) {
            baseName = baseName.substring(0, baseName.length() - ".fastq".length());
            format = ".fastq";
        } else if (baseName.endsWith(".fq")) {
            baseName = baseName.substring(0, baseName.length() - ".fq".length());
            format = ".fq";
        }

        Path r1 = workDir.resolve(baseName + "_r1" + format);
        Path r2 = workDir.resolve(baseName + "_r2" + format);

        BufferedReader br = null;
        BufferedWriter w1 = null;
        BufferedWriter w2 = null;

        try {

            br = FastQIO.getReader(read);
            w1 = FastQIO.getWriter(r1);
            w2 = FastQIO.getWriter(r2);

            


            while (true) {

                String[] r1Lines = new String[4];
                String[] r2Lines = new String[4];

                for (int i = 0; i < 4; i++) {
                    r1Lines[i] = br.readLine();
                    if (r1Lines[i] == null) {
                        return new SplitInterleavedResult(r1, r2, false, true);
                    }
                }

                for (int i = 0; i < 4; i++) {
                    r2Lines[i] = br.readLine();
                    if (r2Lines[i] == null) {
                        return new SplitInterleavedResult(null, null, false, true);
                    }
                }

                for (String l : r1Lines) {
                    w1.write(l);
                    w1.newLine();
                }

                for (String l : r2Lines) {
                    w2.write(l);
                    w2.newLine();
                }
            }
        } catch (Exception e) {
            logger.error("Spliting expcetion", e);
            return new SplitInterleavedResult(null, null, false, false);
        } finally {

            if(br!=null){
                try {
                    br.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                }
            }

            if(w1!=null){
                try {
                    w1.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                }
            }

            if(w2!=null){
                try {
                    w2.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    
                }
            }
            
        }

    }

    @Override
    protected StageRunResult<ReadInspectStageOutput> _execute(
            StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {
        // TODO Auto-generated method stub

        ReadInspectStageInputUrls readInspectStageInputUrls = stageExecutionInput.input;

        String read1Url = readInspectStageInputUrls.getRead1Url();
        String read2Url = readInspectStageInputUrls.getRead2Url();

        Path readLocalPath = stageExecutionInput.inputDir.resolve(read1Url.substring(read1Url.lastIndexOf("/") + 1));

        Map<String, Path> loadMap = Map.of(read1Url, readLocalPath);
        loadInput(loadMap);

        int encoding = -1;
        int readLen = -1;

        SplitInterleavedResult splitInterleavedResult = null;
        try {
            int[] result = checkQualityEncodingAndReadLen(readLocalPath);

            encoding = result[0];
            readLen = result[1];

            if (StringUtils.isBlank(read2Url)) {
                splitInterleavedResult = splitIfLeaved(readLocalPath, stageExecutionInput.workDir);
            }







        } catch (IOException e) {

            logger.error("stage = {} exception", stageExecutionInput.stageContext.getRunStageId(), e);
            return this.runFail(stageExecutionInput.stageContext, "run exception");
        }

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
    }

}
