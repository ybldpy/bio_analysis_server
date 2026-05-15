package com.xjtlu.bio.analysisPipeline.taskrunner.util;


import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
public class FaiBuilder {

    private static final Logger log = LoggerFactory.getLogger(FaiBuilder.class);

    private final AnalysisPipelineToolsConfig toolProperties;

    public FaiBuilder(AnalysisPipelineToolsConfig toolProperties) {
        this.toolProperties = toolProperties;
    }



    public static class FaiBuildException extends Exception {
        public FaiBuildException() {
        }

        public FaiBuildException(String message) {
            super(message);
        }

        public FaiBuildException(String message, Throwable cause) {
            super(message, cause);
        }

        public FaiBuildException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * 为 src 构建默认位置的 fai: src.fai
     *
     * @param src FASTA 文件路径
     * @return 生成后的 fai 路径
     * @throws FaiBuildException 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public Path build(Path src) throws IOException, InterruptedException, FaiBuildException {
        Path defaultFai = getDefaultFaiPath(src);
        build(src, defaultFai);
        return defaultFai;
    }

    /**
     * 为 src 构建 fai。
     * samtools faidx 默认输出到 src.fai。
     * 如果 dst 不是默认路径，则先生成默认 fai，再移动到 dst。
     *
     * @param src FASTA 文件路径
     * @param dst 希望输出的 fai 路径
     * @throws InterruptedException 
     * @throws IOException 
     * @throws FaiBuildException 
     */
    public void build(Path src, Path dst) throws IOException, InterruptedException, FaiBuildException {
        validateInput(src, dst);

        Path defaultFai = getDefaultFaiPath(src);

        runFaidx(src);

        if (!defaultFai.equals(dst)) {
            Files.createDirectories(dst.getParent());
            Files.move(defaultFai, dst, StandardCopyOption.REPLACE_EXISTING);
            log.info("Moved fai from {} to {}", defaultFai, dst);
        }

        log.info("Fai built successfully. src={}, dst={}", src, dst);

    }

    /**
     * 如果目标已存在则直接返回；不存在才构建。
     * @throws FaiBuildException 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public Path ensure(Path src) throws IOException, InterruptedException, FaiBuildException {
        Path defaultFai = getDefaultFaiPath(src);
        if (Files.exists(defaultFai)) {
            return defaultFai;
        }
        return build(src);
    }

    private void runFaidx(Path src) throws IOException, InterruptedException, FaiBuildException {
        List<String> samtools = toolProperties.getSamtools();

        List<String> command = new ArrayList<>();
        command.addAll(samtools);
        command.add("faidx");
        command.add(src.toString());

        log.info("Running command: {}", String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        Process process = pb.start();

        String output;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append(System.lineSeparator());
            }
            output = sb.toString();
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new FaiBuildException(
                    "samtools faidx failed, exitCode=" + exitCode + ", output=" + output);
        }

        log.info("samtools faidx finished successfully for {}", src);
    }

    private void validateInput(Path src, Path dst) {
        Objects.requireNonNull(src, "src must not be null");
        Objects.requireNonNull(dst, "dst must not be null");

        if (!Files.exists(src)) {
            throw new IllegalArgumentException("Source fasta does not exist: " + src);
        }

        if (!Files.isRegularFile(src)) {
            throw new IllegalArgumentException("Source fasta is not a regular file: " + src);
        }

        if (dst.getParent() != null) {
            try {
                Files.createDirectories(dst.getParent());
            } catch (IOException e) {
                throw new RuntimeException("Failed to create dst parent directory: " + dst.getParent(), e);
            }
        }
    }

    private Path getDefaultFaiPath(Path src) {
        return src.resolveSibling(src.getFileName().toString() + ".fai");
    }
}
