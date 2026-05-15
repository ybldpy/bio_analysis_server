package com.xjtlu.bio.analysisPipeline.taskrunner.util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PafParser {

    public static class PafParseResult {
        private final boolean success;
        private final Exception exception;

        public PafParseResult(boolean success, Exception exception) {
            this.success = success;
            this.exception = exception;
        }

        public static PafParseResult success() {
            return new PafParseResult(true, null);
        }

        public static PafParseResult fail(Exception exception) {
            return new PafParseResult(false, exception);
        }

        public boolean isSuccess() {
            return success && exception == null;
        }

        public Exception getException() {
            return exception;
        }
    }

    public static PafParseResult parseToDifferenceTsv(Path pafPath, Path differenceTsvPath) {
        try {
            doParseToDifferenceTsv(pafPath, differenceTsvPath);
            return PafParseResult.success();
        } catch (Exception e) {
            return PafParseResult.fail(e);
        }
    }

    private static void doParseToDifferenceTsv(Path pafPath, Path differenceTsvPath) throws IOException {
        Path parent = differenceTsvPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        try (
                BufferedReader reader = Files.newBufferedReader(pafPath);
                BufferedWriter writer = Files.newBufferedWriter(
                        differenceTsvPath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
                )
        ) {
            writer.write("reference_name\tquery_name\tstrand\treference_position\ttype\tref\tquery\tlength\n");

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }

                parsePafLine(line, writer);
            }
        }
    }

    private static void parsePafLine(String line, BufferedWriter writer) throws IOException {
        String[] fields = line.split("\t");

        if (fields.length < 12) {
            return;
        }

        String queryName = fields[0];
        String strand = fields[4];
        String referenceName = fields[5];

        int refPos = Integer.parseInt(fields[7]) + 1;

        String csTag = findCsTag(fields);
        if (csTag == null || csTag.isBlank()) {
            return;
        }

        parseCsTagToTsvRows(csTag, referenceName, queryName, strand, refPos, writer);
    }

    private static String findCsTag(String[] fields) {
        for (int i = 12; i < fields.length; i++) {
            if (fields[i].startsWith("cs:Z:")) {
                return fields[i].substring("cs:Z:".length());
            }
        }
        return null;
    }

    private static void parseCsTagToTsvRows(
            String cs,
            String referenceName,
            String queryName,
            String strand,
            int refPos,
            BufferedWriter writer
    ) throws IOException {

        int i = 0;

        while (i < cs.length()) {
            char op = cs.charAt(i);

            if (op == ':') {
                i++;
                int start = i;

                while (i < cs.length() && Character.isDigit(cs.charAt(i))) {
                    i++;
                }

                if (start < i) {
                    refPos += Integer.parseInt(cs.substring(start, i));
                }

            } else if (op == '=') {
                i++;
                int start = i;

                while (i < cs.length() && Character.isLetter(cs.charAt(i))) {
                    i++;
                }

                refPos += i - start;

            } else if (op == '*') {
                if (i + 2 >= cs.length()) {
                    break;
                }

                char refBase = Character.toUpperCase(cs.charAt(i + 1));
                char queryBase = Character.toUpperCase(cs.charAt(i + 2));

                writeRow(writer, referenceName, queryName, strand, refPos,
                        "SNP",
                        String.valueOf(refBase),
                        String.valueOf(queryBase),
                        1);

                refPos += 1;
                i += 3;

            } else if (op == '+') {
                i++;
                int start = i;

                while (i < cs.length() && Character.isLetter(cs.charAt(i))) {
                    i++;
                }

                String inserted = cs.substring(start, i).toUpperCase();

                writeRow(writer, referenceName, queryName, strand, refPos,
                        "INSERTION",
                        "-",
                        inserted,
                        inserted.length());

            } else if (op == '-') {
                i++;
                int start = i;

                while (i < cs.length() && Character.isLetter(cs.charAt(i))) {
                    i++;
                }

                String deleted = cs.substring(start, i).toUpperCase();

                writeRow(writer, referenceName, queryName, strand, refPos,
                        "DELETION",
                        deleted,
                        "-",
                        deleted.length());

                refPos += deleted.length();

            } else if (op == '~') {
                i++;
                while (i < cs.length() && !isCsOperator(cs.charAt(i))) {
                    i++;
                }

            } else {
                i++;
            }
        }
    }

    private static void writeRow(
            BufferedWriter writer,
            String referenceName,
            String queryName,
            String strand,
            int refPos,
            String type,
            String ref,
            String query,
            int length
    ) throws IOException {
        writer.write(referenceName);
        writer.write("\t");
        writer.write(queryName);
        writer.write("\t");
        writer.write(strand);
        writer.write("\t");
        writer.write(String.valueOf(refPos));
        writer.write("\t");
        writer.write(type);
        writer.write("\t");
        writer.write(ref);
        writer.write("\t");
        writer.write(query);
        writer.write("\t");
        writer.write(String.valueOf(length));
        writer.write("\n");
    }

    private static boolean isCsOperator(char c) {
        return c == ':'
                || c == '='
                || c == '*'
                || c == '+'
                || c == '-'
                || c == '~';
    }
}