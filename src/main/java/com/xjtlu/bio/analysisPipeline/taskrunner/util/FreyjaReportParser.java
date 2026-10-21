package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.xjtlu.bio.analysisPipeline.stageMetrics.CovidWastewaterResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.CovidWastewaterResultMetrics.LineageAbundance;

/** Parses the key/value TSV report, never evaluating Python or numpy expressions. */
public final class FreyjaReportParser {

    private static final Pattern DECIMAL = Pattern.compile(
            "[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?");

    private FreyjaReportParser() {
    }

    public static CovidWastewaterResultMetrics parse(Path reportPath) throws IOException {
        if (reportPath == null || !Files.isRegularFile(reportPath) || Files.size(reportPath) == 0L) {
            throw invalid("report must be a nonempty regular file");
        }
        String text = Files.readString(reportPath, StandardCharsets.UTF_8);
        if (text.startsWith("\uFEFF")) {
            text = text.substring(1);
        }
        Map<String, String> fields = reportFields(readTsv(text));
        List<String> names = array(required(fields, "lineages"), "lineages");
        List<String> abundances = array(required(fields, "abundances"), "abundances");
        if (names.isEmpty()) {
            throw invalid("lineage estimates are unavailable: lineages is empty");
        }
        if (names.size() != abundances.size()) {
            throw invalid("lineages and abundances must have the same length");
        }

        Set<String> seen = new HashSet<>();
        List<LineageAbundance> parsed = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i).strip();
            if (name.isEmpty()) {
                throw invalid("lineage names must not be blank");
            }
            if (!seen.add(name)) {
                throw invalid("duplicate lineage name: " + name);
            }
            double abundance = number(abundances.get(i), "abundances[" + i + "]");
            if (abundance < 0.0 || abundance > 1.0) {
                throw invalid("abundances must be fractions in [0, 1]");
            }
            parsed.add(new LineageAbundance(name, abundance));
        }

        double residual = number(required(fields, "resid"), "resid");
        if (residual == -1.0) {
            throw invalid("lineage estimates are unavailable: resid is -1");
        }
        if (residual < 0.0) {
            throw invalid("resid must be nonnegative");
        }
        double coverage = number(required(fields, "coverage"), "coverage");
        if (coverage < 0.0 || coverage > 100.0) {
            throw invalid("coverage must be a percentage in [0, 100]");
        }

        CovidWastewaterResultMetrics metrics = new CovidWastewaterResultMetrics();
        metrics.setLineages(parsed);
        metrics.setCoveragePercent(coverage);
        metrics.setResidual(residual);
        metrics.setSummarizedRaw(fields.get("summarized"));
        return metrics;
    }

    private static String required(Map<String, String> fields, String name) throws IOException {
        String value = fields.get(name);
        if (value == null || value.isBlank()) {
            throw invalid("missing or blank field: " + name);
        }
        return value;
    }

    private static double number(String value, String field) throws IOException {
        String trimmed = value.strip();
        if (!DECIMAL.matcher(trimmed).matches()) {
            throw invalid(field + " must be a finite decimal number");
        }
        try {
            double parsed = Double.parseDouble(trimmed);
            if (!Double.isFinite(parsed)) {
                throw invalid(field + " must be finite");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw invalid(field + " is not a valid number");
        }
    }

    private static Map<String, String> reportFields(List<List<String>> records) throws IOException {
        Map<String, String> fields = new LinkedHashMap<>();
        boolean firstRecord = true;
        for (List<String> record : records) {
            if (record.stream().allMatch(String::isBlank)) {
                continue;
            }
            if (firstRecord && record.size() == 2 && record.get(0).isBlank()) {
                firstRecord = false; // pandas' optional index/sample header
                continue;
            }
            firstRecord = false;
            if (record.size() != 2 || record.get(0).isBlank()) {
                throw invalid("expected one key and one sample value per TSV row");
            }
            String key = record.get(0).strip();
            if (fields.putIfAbsent(key, record.get(1)) != null) {
                throw invalid("duplicate field: " + key);
            }
        }
        return fields;
    }

    /** Standard double-quoted TSV cells, including escaped quotes and embedded newlines. */
    private static List<List<String>> readTsv(String text) throws IOException {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;
        boolean closedQuote = false;
        int arrayDepth = 0;
        char arrayQuote = 0;
        boolean arrayEscape = false;
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (quoted) {
                if (ch == '"') {
                    if (i + 1 < text.length() && text.charAt(i + 1) == '"') {
                        cell.append('"');
                        i++;
                    } else {
                        quoted = false;
                        closedQuote = true;
                    }
                } else {
                    cell.append(ch);
                }
                continue;
            }
            if (arrayDepth > 0) {
                // Numpy also emits unquoted bracketed cells spanning physical
                // lines. Quotes within these arrays are data, not CSV delimiters.
                cell.append(ch);
                if (arrayQuote != 0) {
                    if (arrayEscape) {
                        arrayEscape = false;
                    } else if (ch == '\\') {
                        arrayEscape = true;
                    } else if (ch == arrayQuote) {
                        arrayQuote = 0;
                    }
                } else if (ch == '\'' || ch == '"') {
                    arrayQuote = ch;
                } else if (ch == '[') {
                    arrayDepth++;
                } else if (ch == ']') {
                    arrayDepth--;
                }
                continue;
            }
            if (ch == '\t' || ch == '\r' || ch == '\n') {
                row.add(cell.toString());
                cell.setLength(0);
                closedQuote = false;
                if (ch != '\t') {
                    rows.add(row);
                    row = new ArrayList<>();
                    if (ch == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n') {
                        i++;
                    }
                }
            } else if (closedQuote) {
                if (!Character.isWhitespace(ch)) {
                    throw invalid("unexpected text after a quoted TSV cell");
                }
            } else if (ch == '"' && cell.length() == 0) {
                quoted = true;
            } else if (ch == '[' && cell.toString().isBlank()) {
                arrayDepth = 1;
                cell.append(ch);
            } else {
                cell.append(ch);
            }
        }
        if (quoted) {
            throw invalid("unterminated quoted TSV cell");
        }
        if (arrayDepth > 0) {
            throw invalid("unterminated bracketed array");
        }
        if (!row.isEmpty() || cell.length() > 0 || closedQuote) {
            row.add(cell.toString());
            rows.add(row);
        }
        return rows;
    }

    /** Accepts legacy bracketed arrays and modern whitespace-joined report values. */
    private static List<String> array(String value, String field) throws IOException {
        String text = value.strip();
        boolean bracketed = text.startsWith("[") || text.endsWith("]");
        if (bracketed && (!text.startsWith("[") || !text.endsWith("]"))) {
            throw invalid(field + " must be a bracketed array");
        }
        String body = bracketed ? text.substring(1, text.length() - 1) : text;
        List<String> values = new ArrayList<>();
        int index = 0;
        while (index < body.length()) {
            while (index < body.length() && Character.isWhitespace(body.charAt(index))) {
                index++;
            }
            if (index == body.length()) {
                break;
            }
            StringBuilder token = new StringBuilder();
            char first = body.charAt(index);
            if (first == '\'' || first == '"') {
                char quote = first;
                index++;
                boolean closed = false;
                while (index < body.length()) {
                    char ch = body.charAt(index++);
                    if (ch == quote) {
                        closed = true;
                        break;
                    }
                    if (ch == '\\') {
                        if (index == body.length()) {
                            throw invalid("unterminated escape in " + field);
                        }
                        ch = body.charAt(index++);
                        if (ch != quote && ch != '\\') {
                            throw invalid("unsupported escape in " + field);
                        }
                    }
                    token.append(ch);
                }
                if (!closed) {
                    throw invalid("unterminated quoted value in " + field);
                }
            } else {
                while (index < body.length() && !Character.isWhitespace(body.charAt(index))
                        && body.charAt(index) != ',') {
                    char ch = body.charAt(index++);
                    if (ch == '[' || ch == ']' || ch == '\'' || ch == '"') {
                        throw invalid("unexpected character in " + field);
                    }
                    token.append(ch);
                }
                if (token.length() == 0) {
                    throw invalid("missing array value in " + field);
                }
            }
            values.add(token.toString());
            int tokenEnd = index;
            while (index < body.length() && Character.isWhitespace(body.charAt(index))) {
                index++;
            }
            if (index < body.length() && body.charAt(index) == ',') {
                index++;
            } else if (index < body.length() && index == tokenEnd) {
                throw invalid("missing array separator in " + field);
            }
        }
        return values;
    }

    private static IOException invalid(String message) {
        return new IOException("Invalid Freyja demix report: " + message);
    }
}
