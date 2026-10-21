package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.xjtlu.bio.analysisPipeline.stageMetrics.CovidWastewaterResultMetrics;
import com.xjtlu.bio.utils.JsonUtil;

class FreyjaReportParserTest {

    @TempDir
    Path tempDir;

    @Test
    void parsesReportValuesWithoutRenormalizingAbundances() throws Exception {
        String summary = "[('Group A', 0.6), ('Other',0.2)]";
        CovidWastewaterResultMetrics metrics = parse("\tsample\n"
                + "summarized\t" + summary + "\n"
                + report("['Lineage-A' 'Lineage-B']", "[0.6 0.2]", "0.1", "95.0"));

        assertEquals(2, metrics.getLineages().size());
        assertEquals("Lineage-A", metrics.getLineages().get(0).getLineage());
        assertEquals(0.6, metrics.getLineages().get(0).getAbundance());
        assertEquals("Lineage-B", metrics.getLineages().get(1).getLineage());
        assertEquals(0.2, metrics.getLineages().get(1).getAbundance());
        assertEquals(95.0, metrics.getCoveragePercent());
        assertEquals(0.1, metrics.getResidual());
        assertEquals(summary, metrics.getSummarizedRaw());
        String json = JsonUtil.toJson(metrics);
        assertTrue(json.contains("\"lineages\""));
        assertTrue(json.contains("\"abundance\":0.6"));
        assertTrue(json.contains("\"coveragePercent\":95.0"));
    }

    @Test
    void parsesUnquotedMultilineArraysMixedQuotesCommasAndScientificNotation() throws Exception {
        CovidWastewaterResultMetrics metrics = parse(report(
                "['Lineage-A',\n\"Lineage-B\"]", "[6e-1\n\t+.4E0]", "1.2e-3", "9.5E1"));

        assertEquals(2, metrics.getLineages().size());
        assertEquals(0.4, metrics.getLineages().get(1).getAbundance());
        assertEquals(0.0012, metrics.getResidual());
        assertEquals(95.0, metrics.getCoveragePercent());
        assertNull(metrics.getSummarizedRaw());
    }

    @Test
    void parsesModernWhitespaceJoinedLineagesAndAbundancesWithoutBrackets() throws Exception {
        CovidWastewaterResultMetrics metrics = parse("\tsample\n"
                + report("Lineage-A Lineage-B", "6e-1 2e-1", "0.1", "95"));

        assertEquals(2, metrics.getLineages().size());
        assertEquals("Lineage-A", metrics.getLineages().get(0).getLineage());
        assertEquals("Lineage-B", metrics.getLineages().get(1).getLineage());
        assertEquals(0.6, metrics.getLineages().get(0).getAbundance());
        assertEquals(0.2, metrics.getLineages().get(1).getAbundance());
    }

    @Test
    void parsesCsvQuotedMultilineCellsAndEscapedDoubleQuotes() throws Exception {
        String summary = "[(\"Group A\", 0.6),\n (\"Other\", 0.4)]";
        String text = "\uFEFF\t\"sample\"\r\n"
                + "summarized\t" + quotedCell(summary) + "\r\n"
                + "lineages\t" + quotedCell("[\"Lineage-A\",\n \"Lineage-B\"]") + "\r\n"
                + "abundances\t" + quotedCell("[0.6,\n 0.4]") + "\r\n"
                + "resid\t\"0.1\"\r\ncoverage\t\"95.0\"";

        CovidWastewaterResultMetrics metrics = parse(text);

        assertEquals("Lineage-A", metrics.getLineages().get(0).getLineage());
        assertEquals("Lineage-B", metrics.getLineages().get(1).getLineage());
        assertEquals(0.4, metrics.getLineages().get(1).getAbundance());
        assertEquals(summary, metrics.getSummarizedRaw());
    }

    @Test
    void permitsBoundaryValuesAndDoesNotRequireAnAbundanceSumOfOne() throws Exception {
        CovidWastewaterResultMetrics metrics = parse(report(
                "['Lineage-A', 'Lineage-B',]", "[1.0, 1.0,]", "0", "100"));

        assertEquals(1.0, metrics.getLineages().get(0).getAbundance());
        assertEquals(1.0, metrics.getLineages().get(1).getAbundance());
        assertEquals(100.0, metrics.getCoveragePercent());
        assertEquals(0.0, metrics.getResidual());
        CovidWastewaterResultMetrics zero = parse(report("['Lineage-A']", "[0]", "0", "0"));
        assertEquals(0.0, zero.getLineages().get(0).getAbundance());
        assertEquals(0.0, zero.getCoveragePercent());
    }

    @ParameterizedTest
    @MethodSource("invalidReports")
    void rejectsUnavailableMalformedAndOutOfRangeReports(String text, String expectedMessage) throws Exception {
        Path reportPath = write(text);

        IOException error = assertThrows(IOException.class, () -> FreyjaReportParser.parse(reportPath));

        assertTrue(error.getMessage().startsWith("Invalid Freyja demix report:"));
        assertTrue(error.getMessage().contains(expectedMessage), error.getMessage());
    }

    private static Stream<Arguments> invalidReports() {
        String valid = report("['Lineage-A']", "[0.6]", "0.1", "95");
        return Stream.of(
                Arguments.of(report("[]", "[]", "0.1", "95"), "unavailable"),
                Arguments.of(report("['Lineage-A']", "[0.6]", "-1", "95"), "unavailable"),
                Arguments.of(report("[' ' ]", "[0.6]", "0.1", "95"), "blank"),
                Arguments.of(report("['Lineage-A', 'Lineage-A']", "[0.6 0.4]", "0.1", "95"), "duplicate lineage"),
                Arguments.of(report("['Lineage-A', 'Lineage-B']", "[0.6]", "0.1", "95"), "same length"),
                Arguments.of(report("['Lineage-A']", "[-0.1]", "0.1", "95"), "[0, 1]"),
                Arguments.of(report("['Lineage-A']", "[1.1]", "0.1", "95"), "[0, 1]"),
                Arguments.of(report("['Lineage-A']", "[NaN]", "0.1", "95"), "finite"),
                Arguments.of(report("['Lineage-A']", "[Infinity]", "0.1", "95"), "finite"),
                Arguments.of(report("['Lineage-A']", "[1e309]", "0.1", "95"), "finite"),
                Arguments.of(report("['Lineage-A']", "[0.6]", "-0.1", "95"), "nonnegative"),
                Arguments.of(report("['Lineage-A']", "[0.6]", "NaN", "95"), "finite"),
                Arguments.of(report("['Lineage-A']", "[0.6]", "0.1", "-1"), "[0, 100]"),
                Arguments.of(report("['Lineage-A']", "[0.6]", "0.1", "100.1"), "[0, 100]"),
                Arguments.of(report("['Lineage-A']", "[0.6]", "0.1", "Infinity"), "finite"),
                Arguments.of(report("['Lineage-A']", "[0.6", "0.1", "95"), "bracketed"),
                Arguments.of(report("['Lineage-A' 'Lineage-B'", "[0.6 0.4]", "0.1", "95"), "bracketed"),
                Arguments.of(report("['Lineage-A''Lineage-B']", "[0.6 0.4]", "0.1", "95"), "separator"),
                Arguments.of(report("['Lineage-A]", "[0.6]", "0.1", "95"), "unterminated"),
                Arguments.of(report("['Lineage-A']", "[0.6,,]", "0.1", "95"), "missing array value"),
                Arguments.of(valid.replace("coverage\t95\n", ""), "missing or blank field: coverage"),
                Arguments.of(valid + "resid\t0.2\n", "duplicate field"),
                Arguments.of("\tsample-one\tsample-two\n" + valid, "one sample value"),
                Arguments.of("lineages\t\"['Lineage-A']\n", "unterminated quoted TSV"),
                Arguments.of("lineages\t\"['Lineage-A']\"extra\n", "after a quoted TSV"),
                Arguments.of("\n  \n", "missing or blank field"));
    }

    @Test
    void rejectsMissingEmptyAndDirectoryInputsClearly() throws Exception {
        Path empty = Files.createFile(tempDir.resolve("empty.tsv"));
        for (Path path : new Path[] { null, empty, tempDir, tempDir.resolve("missing.tsv") }) {
            IOException error = assertThrows(IOException.class, () -> FreyjaReportParser.parse(path));
            assertTrue(error.getMessage().contains("nonempty regular file"));
        }
    }

    private CovidWastewaterResultMetrics parse(String text) throws IOException {
        return FreyjaReportParser.parse(write(text));
    }

    private Path write(String text) throws IOException {
        return Files.writeString(tempDir.resolve("report.tsv"), text, StandardCharsets.UTF_8);
    }

    private static String quotedCell(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String report(String lineages, String abundances, String residual, String coverage) {
        return "lineages\t" + lineages + "\nabundances\t" + abundances
                + "\nresid\t" + residual + "\ncoverage\t" + coverage + "\n";
    }
}
