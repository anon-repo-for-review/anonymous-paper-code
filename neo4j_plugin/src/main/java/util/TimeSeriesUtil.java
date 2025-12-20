package util;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import result_classes.TimeSeriesResult;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Stream;

// Importiere die Standard-Implementierungen (falls du sie genau dort ablegst)
import util.sources.LocalTimeSeriesSource;
import util.sources.PrometheusTimeSeriesSource;

public final class TimeSeriesUtil {

    private TimeSeriesUtil() {}


    /**
     * Zentrale Methode: durchsucht alle HAS_TIME_SERIES relationships des startNode
     * und versucht, von registrierten TimeSeriesSource-Implementierungen ein Ergebnis zu holen.
     *
     * @param startNode Startknoten
     * @param tsName Name der gesuchten Zeitreihe
     * @param params Parameter-Map (time/range / startTime / endTime ...)
     * @param db GraphDatabaseService (optional)
     * @param log Neo4j Log
     * @return Stream von TimeSeriesResult
     */
    public static Stream<TimeSeriesResult> getFilteredTimeSeries(Node startNode, String tsName, Map<String, Object> params, GraphDatabaseService db, Log log) {
        if (startNode == null) {
            log.warn("getFilteredTimeSeries: startNode was null.");
            return Stream.empty();
        }
        if (params == null) params = Collections.emptyMap();

        TimeWindow window;
        try {
            window = extractTimeWindow(params, log);
        } catch (Exception e) {
            log.error("getFilteredTimeSeries: invalid time window params: %s", e.getMessage());
            return Stream.empty();
        }

        List<TimeSeriesResult> results = new ArrayList<>();

        // Default-Registry: du kannst hier eigene Implementierungen hinzufügen oder die Liste injizieren
        List<TimeSeriesSource> sources = Arrays.asList(
                new LocalTimeSeriesSource(),
                new PrometheusTimeSeriesSource()
        );

        try {
            for (Relationship rel : startNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_TIME_SERIES"))) {
                Node sourceNode = rel.getEndNode();

                for (TimeSeriesSource source : sources) {
                    try {
                        Optional<TimeSeriesResult> maybe = source.fetch(sourceNode, startNode, tsName, window, log, params);
                        if (maybe.isPresent()) {
                            results.add(maybe.get());
                            break; // node geliefert -> next relationship // warum mehrfache Ergebnisse trotz break?????
                        }
                    } catch (Exception e) {
                        log.error("Error while using source %s for node %d: %s", source.getClass().getSimpleName(), sourceNode.getElementId(), e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception while iterating relationships for node %d: %s", startNode.getElementId(), e.getMessage());
        }

        if (results.isEmpty()) {
            log.warn("No time series named '%s' found connected to node %d.", tsName, startNode.getElementId());
        }

        //manuell einstellbar machen
        if ("rps".equals(tsName)) {
            // sum across series, return single aggregated TimeSeriesResult
            TimeSeriesResult agg = uniteTimeSeries(results, tsName, "sum", log);
            if (agg == null) return Stream.empty();
            return Stream.of(agg);
        } else if ("error_rate_pct".equals(tsName) || "latency_ms".equals(tsName)) {
            // mean across series, return single aggregated TimeSeriesResult
            TimeSeriesResult agg = uniteTimeSeries(results, tsName, "mean", log);
            if (agg == null) return Stream.empty();
            return Stream.of(agg);
        }

        String aggregation = (String) params.getOrDefault("aggregation", "");
        long period = ((Number) params.getOrDefault("period", 0L)).longValue();
        return results.stream().flatMap(result -> AggregationUtil.apply(aggregation, result, (int) period, log));

        //return results.stream();
    }




    // --- Parsing / Helper ---
    public static TimeWindow extractTimeWindow(Map<String, Object> params, Log log) {
        if (params == null) params = Collections.emptyMap();

        if (params.containsKey("time") && params.containsKey("range")) {
            long baseTime = parseTimeParameterToMillis(params.get("time"));
            long rangeMillis = parseDuration(params.get("range"));

            if (rangeMillis >= 0) {
                log.info("Creating a forward-looking time window: from %s for %d ms", Instant.ofEpochMilli(baseTime), rangeMillis);
                return new TimeWindow(baseTime, baseTime + rangeMillis);
            } else {
                log.info("Creating a backward-looking time window: from %s for %d ms", Instant.ofEpochMilli(baseTime), rangeMillis);
                return new TimeWindow(baseTime + rangeMillis, baseTime);
            }
        }

        Object startRaw = params.get("startTime");
        Object endRaw = params.get("endTime");
        long startTime = (startRaw != null) ? parseTimeParameterToMillis(startRaw) : 0L;
        long endTime = (endRaw != null) ? parseTimeParameterToMillis(endRaw) : Long.MAX_VALUE;
        return new TimeWindow(startTime, endTime);
    }

    public static long parseTimeParameterToMillis(Object value) {
        if (value == null) throw new IllegalArgumentException("Time parameter cannot be null.");
        if (value instanceof Number) return ((Number) value).longValue();

        String sValue = value.toString().trim();
        if ("now".equalsIgnoreCase(sValue)) return System.currentTimeMillis();

        try {
            return ZonedDateTime.parse(sValue).toInstant().toEpochMilli();
        } catch (Exception e) {
            throw new IllegalArgumentException("Unsupported time format for '" + sValue + "'. Use ISO-8601, 'now', or epoch milliseconds.", e);
        }
    }

    public static long parseDuration(Object value) {
        String s = value.toString().trim().toLowerCase();
        String numberPart = s;
        long multiplier = 1L;

        if (s.endsWith("ms")) {
            multiplier = 1L;
            numberPart = s.substring(0, s.length() - 2);
        } else if (s.endsWith("s")) {
            multiplier = 1000L;
            numberPart = s.substring(0, s.length() - 1);
        } else if (s.endsWith("m")) {
            multiplier = 60_000L;
            numberPart = s.substring(0, s.length() - 1);
        } else if (s.endsWith("h")) {
            multiplier = 3_600_000L;
            numberPart = s.substring(0, s.length() - 1);
        } else if (s.endsWith("d")) {
            multiplier = 86_400_000L;
            numberPart = s.substring(0, s.length() - 1);
        }

        try {
            long parsedNumber = Long.parseLong(numberPart.trim());
            return parsedNumber * multiplier;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid duration format: " + s, e);
        }
    }



    public static Object[] convertToObjectArray(Object raw) {
        if (raw instanceof Object[]) return (Object[]) raw;
        if (raw instanceof int[]) {
            int[] arr = (int[]) raw;
            Object[] out = new Object[arr.length];
            for (int i = 0; i < arr.length; i++) out[i] = arr[i];
            return out;
        }
        if (raw instanceof long[]) {
            long[] arr = (long[]) raw;
            Object[] out = new Object[arr.length];
            for (int i = 0; i < arr.length; i++) out[i] = arr[i];
            return out;
        }
        if (raw instanceof double[]) {
            double[] arr = (double[]) raw;
            Object[] out = new Object[arr.length];
            for (int i = 0; i < arr.length; i++) out[i] = arr[i];
            return out;
        }
        throw new IllegalArgumentException("Unsupported array type");
    }

    public static Instant parseToInstant(String s) {
        if (s == null) throw new IllegalArgumentException("Timestamp darf nicht null sein.");
        try { return Instant.parse(s); } catch (DateTimeParseException ignored) {}
        try { return ZonedDateTime.parse(s, DateTimeFormatter.ISO_DATE_TIME).toInstant(); } catch (DateTimeParseException ignored) {}
        try { return LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant(); } catch (DateTimeParseException ignored) {}
        try { return Instant.ofEpochMilli(Long.parseLong(s)); } catch (NumberFormatException ignored) {}
        throw new IllegalArgumentException("Timestamp-Format nicht erkannt: " + s);
    }


    public static ZonedDateTime parseToZonedDateTime(Object value) {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        } else if (value instanceof String) {
            return ZonedDateTime.parse((String) value, DateTimeFormatter.ISO_DATE_TIME);
        } else if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault());
        } else {
            throw new IllegalArgumentException("Unsupported date format: " + value);
        }
    }



    private static TimeSeriesResult uniteTimeSeries(List<TimeSeriesResult> seriesList, String tsName, String mode, Log log) {
        if (seriesList == null || seriesList.isEmpty()) return null;

        // 1) Für jede Serie: xs (long[] epochMillis) und ys (double[]) bauen
        List<long[]> xsList = new ArrayList<>();
        List<double[]> ysList = new ArrayList<>();
        for (TimeSeriesResult ts : seriesList) {
            if (ts == null || ts.timestamps == null) continue;
            List<String> tStrings = ts.timestamps;
            List<Double> values = ts.values != null ? ts.values.get(tsName) : null;
            if ((values == null || values.isEmpty()) && ts.values != null && !ts.values.isEmpty()) {
                // fallback: erste Serie benutzen
                values = ts.values.values().iterator().next();
            }
            if (values == null || values.isEmpty()) continue;

            List<Long> millis = new ArrayList<>();
            int n = Math.min(tStrings.size(), values.size());
            for (int i = 0; i < n; i++) {
                try {
                    ZonedDateTime zdt = parseToZonedDateTime(tStrings.get(i)); // vorhandene util-Methode
                    long m = zdt.toInstant().toEpochMilli();
                    millis.add(m);
                } catch (Exception e) {
                    // skip unparsable timestamp
                }
            }
            if (millis.isEmpty()) continue;

            long[] xs = millis.stream().mapToLong(Long::longValue).toArray();
            double[] ys = new double[xs.length];
            for (int i = 0; i < xs.length; i++) {
                Double v = values.get(i);
                ys[i] = (v == null || v.isNaN() || v.isInfinite()) ? 0.0 : v;
            }
            // ensure xs are sorted with corresponding ys
            // if not sorted, we must sort them together
            if (!isSorted(xs)) {
                int len = xs.length;
                LongDoublePair[] pairs = new LongDoublePair[len];
                for (int i = 0; i < len; i++) pairs[i] = new LongDoublePair(xs[i], ys[i]);
                Arrays.sort(pairs, Comparator.comparingLong(p -> p.x));
                for (int i = 0; i < len; i++) { xs[i] = pairs[i].x; ys[i] = pairs[i].y; }
            }
            xsList.add(xs);
            ysList.add(ys);
        }

        if (xsList.isEmpty()) return null;

        // 2) Build sorted union of all timestamps
        Set<Long> allTimestampsSet = new HashSet<>();
        for (long[] xs : xsList) for (long v : xs) allTimestampsSet.add(v);
        List<Long> allTimestamps = new ArrayList<>(allTimestampsSet);
        Collections.sort(allTimestamps);

        if (allTimestamps.isEmpty()) return null;

        // 3) For each timestamp compute aggregated value
        List<String> outTimestamps = new ArrayList<>(allTimestamps.size());
        List<Double> outValues = new ArrayList<>(allTimestamps.size());

        for (Long t : allTimestamps) {
            double sum = 0.0;
            int countPresent = 0;
            for (int s = 0; s < xsList.size(); s++) {
                long[] xs = xsList.get(s);
                double[] ys = ysList.get(s);
                Double v = interpolateAt(xs, ys, t); // may be null if outside series range
                if (v != null) {
                    sum += v;
                    countPresent++;
                } else {
                    if ("sum".equals(mode)) {
                        // treat missing as 0 for sum
                        sum += 0.0;
                    } // for mean, we simply don't increase countPresent or sum
                }
            }

            double outVal;
            if ("mean".equals(mode)) {
                if (countPresent == 0) {
                    // no value available at this t for any series -> skip timestamp
                    continue;
                }
                outVal = sum / (double) countPresent;
            } else { // sum
                outVal = sum;
            }

            outTimestamps.add(Instant.ofEpochMilli(t).toString());
            outValues.add(outVal);
        }

        if (outTimestamps.isEmpty()) return null;

        Map<String, List<Double>> valuesMap = new HashMap<>();
        valuesMap.put(tsName, outValues);
        return new TimeSeriesResult(outTimestamps, valuesMap);
    }

    /** linear interpolation helper: xs must be sorted ascending; returns null when t outside [xs[0], xs[last]] */
    private static Double interpolateAt(long[] xs, double[] ys, long t) {
        if (xs == null || xs.length == 0) return null;
        if (t < xs[0] || t > xs[xs.length - 1]) return null;
        int idx = Arrays.binarySearch(xs, t);
        if (idx >= 0) return ys[idx];
        int ins = -idx - 1;
        if (ins == 0 || ins >= xs.length) return null;
        int left = ins - 1;
        int right = ins;
        long x0 = xs[left];
        long x1 = xs[right];
        double y0 = ys[left];
        double y1 = ys[right];
        if (x1 == x0) return y0;
        double ratio = (double) (t - x0) / (double) (x1 - x0);
        return y0 + ratio * (y1 - y0);
    }

    /** tiny helper to check sorted */
    private static boolean isSorted(long[] a) {
        for (int i = 1; i < a.length; i++) if (a[i] < a[i - 1]) return false;
        return true;
    }

    /** small pair used for sorting */
    private static class LongDoublePair {
        long x; double y;
        LongDoublePair(long x, double y) { this.x = x; this.y = y; }
    }
}



















































/*package util;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import result_classes.TimeSeriesResult;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public final class TimeSeriesUtil {

    public static void validateTimeSeries(Node tsNode) {
        if (!tsNode.hasLabel(Label.label("time_series")) || !tsNode.hasProperty("timestamps")) {
            throw new IllegalArgumentException("Knoten muss `time_series` mit `timestamps` enthalten.");
        }
    }

    public static Map<String, Object[]> getValueProperties(Node node, int expectedLength) {
        Map<String, Object[]> result = new HashMap<>();
        for (String key : node.getPropertyKeys()) {
            if (key.equals("timestamps") || key.equals("name")) continue;
            if (!node.hasProperty(key)) continue;

            Object raw = node.getProperty(key);
            if (!(raw instanceof Object[] || raw instanceof int[] || raw instanceof long[] || raw instanceof double[]))
                continue;

            Object[] array = convertToObjectArray(raw);
            if (array.length == expectedLength) {
                result.put(key, array);
            }
        }
        return result;
    }


    public static Map<String, List<Double>> getDoubleTs(Node node, int expectedLength) {
        Map<String, List<Double>> result = new HashMap<>();

        for (String key : node.getPropertyKeys()) {
            if (key.equals("timestamps") || key.equals("name")) continue;
            if (!node.hasProperty(key)) continue;

            Object raw = node.getProperty(key);

            if (!(raw instanceof Object[] || raw instanceof int[] || raw instanceof long[] || raw instanceof double[]))
                continue;

            Object[] array = convertToObjectArray(raw);


            if (array == null || array.length != expectedLength) continue;

            List<Double> doubleList = new ArrayList<>();
            for (Object obj : array) {
                if (obj instanceof Number) {
                    doubleList.add(((Number) obj).doubleValue());
                } else {
                    // Optional: überspringen oder Fehlerbehandlung
                    // Hier überspringen wir nicht-numerische Werte
                }
            }

            result.put(key, doubleList);
        }

        return result;
    }


    public static TimeSeriesResult getFilteredTimeSeries(Node tsNode, long startTime, long endTime) {
        validateTimeSeries(tsNode);

        // Lade die vollständigen Daten aus dem Knoten
        List<String> allTimestamps = Arrays.asList((String[]) tsNode.getProperty("timestamps", new String[0]));
        Map<String, List<Double>> allValues = getDoubleTs(tsNode, allTimestamps.size());

        List<Integer> indicesToKeep = new ArrayList<>();
        List<String> filteredTimestamps = new ArrayList<>();

        // 1. Finde die Indizes der Zeitstempel, die im gewünschten Bereich liegen
        for (int i = 0; i < allTimestamps.size(); i++) {
            try {
                // Verwende ZonedDateTime.parse für maximale Flexibilität
                long currentTimestamp = ZonedDateTime.parse(allTimestamps.get(i)).toInstant().toEpochMilli();
                if (currentTimestamp >= startTime && currentTimestamp < endTime) {
                    indicesToKeep.add(i);
                    filteredTimestamps.add(allTimestamps.get(i));
                }
            } catch (Exception e) {
                // Zeitstempel wird bei Parse-Fehler ignoriert
            }
        }

        // 2. Erstelle neue Wertelisten, die nur die gefilterten Indizes enthalten
        Map<String, List<Double>> filteredValues = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : allValues.entrySet()) {
            List<Double> originalValues = entry.getValue();
            List<Double> newValues = indicesToKeep.stream()
                    .map(originalValues::get)
                    .collect(Collectors.toList());
            filteredValues.put(entry.getKey(), newValues);
        }

        return new TimeSeriesResult(filteredTimestamps, filteredValues);
    }

    public static Object[] convertToObjectArray(Object raw) {
        if (raw instanceof Object[]) return (Object[]) raw;
        if (raw instanceof int[]) return Arrays.stream((int[]) raw).boxed().toArray();
        if (raw instanceof long[]) return Arrays.stream((long[]) raw).boxed().toArray();
        if (raw instanceof double[]) return Arrays.stream((double[]) raw).boxed().toArray();
        throw new IllegalArgumentException("Unsupported array type");
    }

    public static ZonedDateTime parseToZonedDateTime(Object value) {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        } else if (value instanceof String) {
            return ZonedDateTime.parse((String) value, DateTimeFormatter.ISO_DATE_TIME);
        } else if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault());
        } else {
            throw new IllegalArgumentException("Unsupported date format: " + value);
        }
    }
}*/
