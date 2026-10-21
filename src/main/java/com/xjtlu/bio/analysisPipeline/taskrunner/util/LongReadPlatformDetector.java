package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform;

/** Conservative recognition of native long-read FASTQ header conventions. */
public final class LongReadPlatformDetector {

    private static final Pattern UUID = Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final Pattern LEGACY_RUN_ID = Pattern.compile("[0-9a-fA-F]{40}");
    private static final Pattern POSITIVE_INTEGER = Pattern.compile("[1-9][0-9]*");
    private static final Pattern NON_NEGATIVE_INTEGER = Pattern.compile("[0-9]+");
    private static final Pattern FLOW_CELL_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]*");
    private static final Pattern BASECALL_MODEL = Pattern.compile(
            "(?:[0-9]{4}-[0-9]{2}-[0-9]{2}_)?(?:dna|rna[0-9]*)_[A-Za-z0-9][A-Za-z0-9._@+-]*");
    private static final Pattern MODERN_MOVIE = Pattern.compile(
            "m[0-9]{4,}[A-Za-z]?_([0-9]{6})_([0-9]{6})(?:_[A-Za-z0-9]+)*");
    private static final Pattern LEGACY_MOVIE = Pattern.compile(
            "m([0-9]{6})_([0-9]{6})_[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*");

    private LongReadPlatformDetector() {
    }

    /**
     * Returns a platform only when every supplied header positively identifies the
     * same platform. Missing, renamed, weak or mixed evidence returns UNKNOWN.
     * This is a deliberately strict recognition rule, not a calibrated confidence
     * estimate or proof of a file's provenance. Callers decide whether detection
     * is appropriate; user declarations and read-length decisions are out of scope.
     */
    public static int detect(List<String> headers) {
        if (headers == null || headers.isEmpty()) {
            return SequencingPlatform.UNKNOWN;
        }
        int platform = SequencingPlatform.UNKNOWN;
        for (String header : headers) {
            int identified = identify(header);
            if (identified == SequencingPlatform.UNKNOWN
                    || (platform != SequencingPlatform.UNKNOWN && platform != identified)) {
                return SequencingPlatform.UNKNOWN;
            }
            platform = identified;
        }
        return platform;
    }

    private static int identify(String header) {
        if (header == null || !header.startsWith("@")) {
            return SequencingPlatform.UNKNOWN;
        }
        String[] fields = header.substring(1).stripTrailing().split("\\s+");
        String name = fields[0];
        if (isPacBioName(name)) {
            return SequencingPlatform.PACBIO;
        }
        if (!UUID.matcher(name).matches()) {
            return SequencingPlatform.UNKNOWN;
        }
        Map<String, String> attributes = new HashMap<>();
        for (int i = 1; i < fields.length; i++) {
            int separator = fields[i].indexOf('=');
            if (separator > 0) {
                String key = fields[i].substring(0, separator);
                String value = fields[i].substring(separator + 1);
                if (attributes.putIfAbsent(key, value) != null) {
                    return SequencingPlatform.UNKNOWN;
                }
            }
        }
        String runId = attributes.get("runid");
        if (runId == null || (!UUID.matcher(runId).matches() && !LEGACY_RUN_ID.matcher(runId).matches())) {
            return SequencingPlatform.UNKNOWN;
        }
        boolean channel = matches(POSITIVE_INTEGER, attributes.get("ch"));
        boolean startTime = isTimestamp(attributes.get("start_time"));
        boolean flowAndModel = matches(FLOW_CELL_ID, attributes.get("flow_cell_id"))
                && matches(BASECALL_MODEL, attributes.get("basecall_model_version_id"));
        return channel || startTime || flowAndModel ? SequencingPlatform.ONT : SequencingPlatform.UNKNOWN;
    }

    private static boolean matches(Pattern pattern, String value) {
        return value != null && pattern.matcher(value).matches();
    }

    private static boolean isTimestamp(String value) {
        if (value == null) {
            return false;
        }
        try {
            OffsetDateTime.parse(value);
            return true;
        } catch (DateTimeException exception) {
            return false;
        }
    }

    private static boolean isPacBioName(String name) {
        String[] components = name.split("/", -1);
        if (components.length < 3 || components.length > 5 || !isNativeMovie(components[0])
                || !NON_NEGATIVE_INTEGER.matcher(components[1]).matches()) {
            return false;
        }
        if (!"ccs".equals(components[2])) {
            return components.length == 3 && isInterval(components[2]);
        }
        if (components.length == 3) {
            return true;
        }
        boolean strand = "fwd".equals(components[3]) || "rev".equals(components[3]);
        return components.length == 4 ? strand || isInterval(components[3])
                : strand && isInterval(components[4]);
    }

    private static boolean isNativeMovie(String movie) {
        return matchesNativeMovie(MODERN_MOVIE, movie) || matchesNativeMovie(LEGACY_MOVIE, movie);
    }

    private static boolean matchesNativeMovie(Pattern pattern, String movie) {
        Matcher matcher = pattern.matcher(movie);
        if (!matcher.matches()) {
            return false;
        }
        String date = matcher.group(1);
        String time = matcher.group(2);
        try {
            LocalDateTime.of(2000 + Integer.parseInt(date.substring(0, 2)),
                    Integer.parseInt(date.substring(2, 4)), Integer.parseInt(date.substring(4, 6)),
                    Integer.parseInt(time.substring(0, 2)), Integer.parseInt(time.substring(2, 4)),
                    Integer.parseInt(time.substring(4, 6)));
            return true;
        } catch (DateTimeException exception) {
            return false;
        }
    }

    private static boolean isInterval(String value) {
        String[] endpoints = value.split("_", -1);
        if (endpoints.length != 2 || !NON_NEGATIVE_INTEGER.matcher(endpoints[0]).matches()
                || !NON_NEGATIVE_INTEGER.matcher(endpoints[1]).matches()) {
            return false;
        }
        try {
            return Long.parseLong(endpoints[0]) < Long.parseLong(endpoints[1]);
        } catch (NumberFormatException exception) {
            return false;
        }
    }
}
