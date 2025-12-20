package mathematical_relations;


import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventCorrelationById {

    @Context
    public Transaction tx;

    @Context
    public Log log;

    public static class CorrelationResult {
        public double correlation;
        public long contributingIntervals;

        public CorrelationResult(double correlation, long contributingIntervals) {
            this.correlation = correlation;
            this.contributingIntervals = contributingIntervals;
        }
    }

    @Procedure(name = "graphobs.analysis.correlate_events_with_value_by_id", mode = Mode.READ)
    @Description("Berechnet die Korrelation zwischen der Häufigkeit von Ereignissen (via Element-ID-Liste) und dem mittleren Wert einer Zeitreihe.")
    public Stream<CorrelationResult> correlateEventsWithValueByElementId(
            @Name("eventNodeElementIds") List<String> eventNodeElementIds, // NEU: List<String>
            @Name("eventTimestampProperty") String eventTimestampProperty,
            @Name("valueNodeElementId") String valueNodeElementId,         // NEU: String
            @Name("valueProperty") String valueProperty,
            @Name("intervalSeconds") long intervalSeconds) {

        // --- 1. Lade alle Datenpunkte durch Nachschlagen der Element-IDs ---
        List<ZonedDateTime> eventTimestamps = parseTimestampsFromNodeElementIds(eventNodeElementIds, eventTimestampProperty);
        Node valueNode = tx.getNodeByElementId(valueNodeElementId); // KORREKTE, MODERNE METHODE
        Map<ZonedDateTime, Double> valueSeries = parseKeyValue(valueNode, valueProperty);

        if (eventTimestamps.isEmpty() || valueSeries.isEmpty()) {
            log.warn("Eine der Datenquellen (Ereignisse oder Werte) ist leer. Es kann keine Korrelation berechnet werden.");
            return Stream.empty();
        }

        // --- 2. Erstelle Bins (Intervalle) und aggregiere die Daten ---
        Map<ZonedDateTime, Integer> eventCounts = new TreeMap<>();
        for (ZonedDateTime ts : eventTimestamps) {
            ZonedDateTime bucket = getBucket(ts, intervalSeconds);
            eventCounts.put(bucket, eventCounts.getOrDefault(bucket, 0) + 1);
        }

        Map<ZonedDateTime, List<Double>> valueBuckets = new TreeMap<>();
        for (Map.Entry<ZonedDateTime, Double> entry : valueSeries.entrySet()) {
            ZonedDateTime bucket = getBucket(entry.getKey(), intervalSeconds);
            valueBuckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(entry.getValue());
        }

        Map<ZonedDateTime, Double> valueMeans = valueBuckets.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0)
                ));

        // --- 3. Erstelle die zwei zu korrelierenden Vektoren ---
        Set<ZonedDateTime> allBuckets = new TreeSet<>(eventCounts.keySet());
        allBuckets.addAll(valueMeans.keySet());

        if (allBuckets.size() < 2) {
            log.warn("Nach der Aggregation in " + intervalSeconds + "s-Intervalle sind weniger als 2 Datenpunkte übrig.");
            return Stream.empty();
        }

        List<Double> eventCountVector = new ArrayList<>();
        List<Double> valueMeanVector = new ArrayList<>();

        for (ZonedDateTime bucket : allBuckets) {
            eventCountVector.add((double) eventCounts.getOrDefault(bucket, 0));
            valueMeanVector.add(valueMeans.getOrDefault(bucket, 0.0));
        }

        // --- 4. Berechne die Pearson-Korrelation ---
        double correlation = calculatePearson(
                eventCountVector.stream().mapToDouble(d -> d).toArray(),
                valueMeanVector.stream().mapToDouble(d -> d).toArray()
        );

        return Stream.of(new CorrelationResult(correlation, allBuckets.size()));
    }

    /**
     * KORRIGIERTE Hilfsfunktion: Verwendet Element-IDs (Strings).
     */
    private List<ZonedDateTime> parseTimestampsFromNodeElementIds(List<String> elementIds, String timestampProperty) {
        List<ZonedDateTime> timestamps = new ArrayList<>(elementIds.size());
        for (String elementId : elementIds) {
            Node node = tx.getNodeByElementId(elementId); // KORREKTE, MODERNE METHODE
            if (node.hasProperty(timestampProperty)) {
                Object rawTimestamp = node.getProperty(timestampProperty);
                timestamps.add(parseToZonedDateTime(rawTimestamp));
            }
        }
        return timestamps;
    }

    // --- Restliche Hilfsfunktionen (unverändert) ---

    private ZonedDateTime getBucket(ZonedDateTime ts, long intervalSeconds) {
        long epochSecond = ts.toEpochSecond();
        long bucketStartSecond = (epochSecond / intervalSeconds) * intervalSeconds;
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(bucketStartSecond), ts.getZone());
    }

    private Map<ZonedDateTime, Double> parseKeyValue(Node node, String valueProperty) {
        if (!node.hasProperty("timestamps")) throw new IllegalArgumentException("Knoten " + node.getElementId() + " hat keine 'timestamps'-Property.");
        if (!node.hasProperty(valueProperty)) throw new IllegalArgumentException("Knoten " + node.getElementId() + " hat keine Property namens '" + valueProperty + "'.");

        Object[] timestampsRaw = (Object[]) node.getProperty("timestamps");
        double[] values = convertToDoubleArray(node.getProperty(valueProperty));

        if (timestampsRaw.length != values.length) {
            throw new IllegalStateException("Anzahl der Zeitstempel und Werte stimmt auf Knoten " + node.getElementId() + " nicht überein.");
        }

        Map<ZonedDateTime, Double> dataMap = new HashMap<>();
        for (int i = 0; i < timestampsRaw.length; i++) {
            dataMap.put(parseToZonedDateTime(timestampsRaw[i]), values[i]);
        }
        return dataMap;
    }

    private double calculatePearson(double[] x, double[] y) {
        int n = x.length;
        if (n < 2) return Double.NaN;

        double sum_x = 0.0, sum_y = 0.0, sum_xy = 0.0;
        double sum_x_sq = 0.0, sum_y_sq = 0.0;

        for (int i = 0; i < n; i++) {
            sum_x += x[i];
            sum_y += y[i];
            sum_xy += x[i] * y[i];
            sum_x_sq += x[i] * x[i];
            sum_y_sq += y[i] * y[i];
        }

        double numerator = n * sum_xy - sum_x * sum_y;
        double denominator = Math.sqrt((n * sum_x_sq - sum_x * sum_x) * (n * sum_y_sq - sum_y * sum_y));

        if (denominator < 1e-9) {
            return 0.0;
        }
        return numerator / denominator;
    }

    private double[] convertToDoubleArray(Object raw) {
        if (raw instanceof double[]) return (double[]) raw;
        if (raw instanceof long[]) return Arrays.stream((long[]) raw).asDoubleStream().toArray();
        if (raw instanceof int[]) return Arrays.stream((int[]) raw).asDoubleStream().toArray();
        if (raw instanceof Object[])
            return Arrays.stream((Object[]) raw).mapToDouble(o -> ((Number) o).doubleValue()).toArray();
        throw new IllegalArgumentException("Nicht unterstützter Array-Typ für Werte: " + raw.getClass().getName());
    }

    private ZonedDateTime parseToZonedDateTime(Object value) {
        if (value instanceof ZonedDateTime) return (ZonedDateTime) value;
        if (value instanceof String) return ZonedDateTime.parse((String) value, DateTimeFormatter.ISO_DATE_TIME);
        if (value instanceof Long)
            return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault());
        throw new IllegalArgumentException("Nicht unterstützter Zeitstempel-Typ: " + value.getClass().getName());
    }
}
