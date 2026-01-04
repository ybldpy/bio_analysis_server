package com.xjtlu.bio.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.springframework.stereotype.Component;

import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;

import jakarta.annotation.Resource;


@Component
public class SampleReadLengthDetector {

    // 你可以按需调整
    private static final int DEFAULT_SAMPLE_READS = 2000;

    private String tempDir;

    @Resource
    private StorageService storageService;

    public boolean isLongRead(Path localReadFile) throws IOException {
        return isLongRead(localReadFile, DEFAULT_SAMPLE_READS);
    }

    public boolean isLongRead(Path localReadFile, int sampleReads) throws IOException {
        if (sampleReads <= 0)
            sampleReads = DEFAULT_SAMPLE_READS;

        try (BufferedReader br = openPossiblyGz(localReadFile)) {
            String first = nextNonEmptyLine(br);
            if (first == null)
                return false;

            if (first.startsWith("@")) {
                // FASTQ
                ReadStats stats = sampleFastqLengths(br, first, sampleReads);
                return classifyLong(stats);
            } else if (first.startsWith(">")) {
                // FASTA
                ReadStats stats = sampleFastaLengths(br, first, sampleReads);
                return classifyLong(stats);
            } else {
                // Unknown, conservative: treat as not long
                return false;
            }
        }
    }

    /** 如果你一定要用 readUrl，这里只是示意：先下载到 tmp，再调用 Path 版本 */
    public boolean isLongRead(String readUrl) throws IOException, Exception {
        // TODO: 用你的 storageService.getObject(readUrl, tmpPath) 下载到本地
        // Path tmp = ...
        // return isLongRead(tmp);
        Path tmpPath = Paths.get(this.tempDir, readUrl.replaceAll("/", "_"));
        GetObjectResult getObjectResult = storageService.getObject(readUrl, tempDir.toString());

        if(!getObjectResult.success()){
            if(getObjectResult.e()!=null){
                throw getObjectResult.e();
            }
            throw new IOException("无法加载文件");
        }

        return isLongRead(tmpPath);
        

    }

    // ---------------- helpers ----------------

    private static boolean classifyLong(ReadStats s) {
        if (s.sampled == 0)
            return false;
        // 实用阈值：p95>=1000 或 max>=5000 基本可以认定长读
        return (s.p95 >= 1000) || (s.max >= 5000);
    }

    private static ReadStats sampleFastqLengths(BufferedReader br, String firstHeader, int sampleReads)
            throws IOException {
        int[] lens = new int[sampleReads];
        int n = 0;
        int min = Integer.MAX_VALUE, max = 0;

        String h = firstHeader;
        while (n < sampleReads) {
            String seq = br.readLine();
            String plus = br.readLine();
            String qual = br.readLine();
            if (seq == null || plus == null || qual == null)
                break;

            // 基本校验：header/plus，质量长度最好与序列一致（不一致就跳过该条）
            if (!h.startsWith("@") || !plus.startsWith("+") || qual.length() != seq.length()) {
                h = br.readLine();
                if (h == null)
                    break;
                continue;
            }

            int L = seq.length();
            lens[n++] = L;
            if (L < min)
                min = L;
            if (L > max)
                max = L;

            h = br.readLine();
            if (h == null)
                break; // next header
        }

        return buildStats(lens, n, min, max);
    }

    private static ReadStats sampleFastaLengths(BufferedReader br, String firstHeader, int sampleSeqs)
            throws IOException {
        int[] lens = new int[sampleSeqs];
        int n = 0;
        int min = Integer.MAX_VALUE, max = 0;

        String line = firstHeader;
        StringBuilder seq = new StringBuilder(4096);

        while (true) {
            line = br.readLine();
            if (line == null || line.startsWith(">")) {
                if (seq.length() > 0) {
                    int L = seq.length();
                    if (n < sampleSeqs) {
                        lens[n++] = L;
                        if (L < min)
                            min = L;
                        if (L > max)
                            max = L;
                    }
                    seq.setLength(0);
                    if (n >= sampleSeqs)
                        break;
                }
                if (line == null)
                    break;
                continue;
            }

            String s = line.trim();
            if (!s.isEmpty())
                seq.append(s);
        }

        return buildStats(lens, n, min, max);
    }

    private static ReadStats buildStats(int[] lens, int n, int min, int max) {
        if (n == 0)
            return new ReadStats(0, 0, 0, 0, 0);

        Arrays.sort(lens, 0, n);
        int p50 = lens[(int) Math.floor(0.50 * (n - 1))];
        int p95 = lens[(int) Math.floor(0.95 * (n - 1))];

        return new ReadStats(n, min, p50, p95, max);
    }

    private static String nextNonEmptyLine(BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty())
                return line;
        }
        return null;
    }

    private static BufferedReader openPossiblyGz(Path file) throws IOException {
        InputStream in = Files.newInputStream(file);

        // gzip magic: 1F 8B
        PushbackInputStream pb = new PushbackInputStream(in, 2);
        byte[] sig = pb.readNBytes(2);
        pb.unread(sig);

        boolean gz = sig.length == 2 && (sig[0] == (byte) 0x1f && sig[1] == (byte) 0x8b);
        InputStream real = gz ? new GZIPInputStream(pb) : pb;

        return new BufferedReader(new InputStreamReader(real, StandardCharsets.UTF_8), 64 * 1024);
    }

    private static final class ReadStats {
        final int sampled;
        final int min;
        final int p50;
        final int p95;
        final int max;

        ReadStats(int sampled, int min, int p50, int p95, int max) {
            this.sampled = sampled;
            this.min = min;
            this.p50 = p50;
            this.p95 = p95;
            this.max = max;
        }
    }
}
