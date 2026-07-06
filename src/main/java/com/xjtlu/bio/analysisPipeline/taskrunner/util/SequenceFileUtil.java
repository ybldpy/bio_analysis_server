package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.xjtlu.bio.analysisPipeline.Constants;

public class SequenceFileUtil {

    public static boolean isCompressedFormat(String fname) {
        return Constants.CompressedFormat.isCompressed(fname);
    }

    public static boolean isGzip(Path p) {
        String fileName = p.getFileName().toString();
        return Constants.CompressedFormat.isGzip(fileName);
    }

    public static String getUncompressedFileName(String fname) {
        if (fname == null || fname.isBlank()) {
            throw new IllegalArgumentException("File name must not be blank");
        }

        String lower = fname.toLowerCase(Locale.ROOT);

        if (lower.endsWith(".gzip")) {
            return fname.substring(0, fname.length() - ".gzip".length());
        }

        if (lower.endsWith(".gz")) {
            return fname.substring(0, fname.length() - ".gz".length());
        }

        if (lower.endsWith(".zip")) {
            return fname.substring(0, fname.length() - ".zip".length());
        }

        return fname;
    }

    public static void uncompress(Path compressedSequence, Path to) throws IOException {
        Objects.requireNonNull(compressedSequence, "compressedSequence must not be null");
        Objects.requireNonNull(to, "target path must not be null");

        if (!Files.exists(compressedSequence)) {
            throw new IOException("Compressed sequence file does not exist: " + compressedSequence.toAbsolutePath());
        }

        if (!Files.isRegularFile(compressedSequence)) {
            throw new IOException(
                    "Compressed sequence path is not a regular file: " + compressedSequence.toAbsolutePath());
        }

        Path parent = to.toAbsolutePath().getParent();
        if (parent == null) {
            Files.createDirectories(parent);
        }

        String fileName = compressedSequence.getFileName().toString().toLowerCase(Locale.ROOT);

        if (fileName.endsWith(".gz") || fileName.endsWith(".gzip")) {
            uncompressGzip(compressedSequence, to);
            return;
        }

        if (fileName.endsWith(".zip")) {
            uncompressZipFirstFile(compressedSequence, to);
            return;
        }

        throw new IOException("Unsupported compressed sequence format: " + compressedSequence.toAbsolutePath());
    }

    private static void uncompressGzip(Path source, Path target) throws IOException {
        try (InputStream in = new GZIPInputStream(Files.newInputStream(source));
                OutputStream out = Files.newOutputStream(target)) {
            in.transferTo(out);
        }
    }

    private static void uncompressZipFirstFile(Path source, Path target) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(source));
                OutputStream out = Files.newOutputStream(target)) {

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    zis.closeEntry();
                    continue;
                }

                zis.transferTo(out);
                zis.closeEntry();
                return;
            }

            throw new IOException("No file entry found in zip archive: " + source.toAbsolutePath());
        }
    }

    public static void copyOrUncompress(Path source, Path to) throws IOException {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(to, "target path must not be null");

        Path parent = to.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        String fileName = source.getFileName().toString();

        if (isCompressedFormat(fileName)) {
            uncompress(source, to);
        } else {
            Files.copy(source, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static BufferedReader getReader(Path in) throws IOException {

        InputStream is = Files.newInputStream(in);

        if (isGzip(in)) {
            is = new GZIPInputStream(is);
        }

        return new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    public static BufferedWriter getWriter(Path out) throws IOException {

        OutputStream os = Files.newOutputStream(out);

        if (isGzip(out)) {
            os = new GZIPOutputStream(os);
        }

        return new BufferedWriter(
                new OutputStreamWriter(os));
    }

}
