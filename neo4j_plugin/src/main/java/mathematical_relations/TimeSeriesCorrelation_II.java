package mathematical_relations;

import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import temporal_joins.AlignedData;
import temporal_joins.JoinStrategyFactory;
import temporal_joins.LinearInterpolationJoinStrategy;
import temporal_joins.TemporalJoinStrategy;


import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Stream;

public class TimeSeriesCorrelation_II {

    @Context
    public Log log;

    /** Ergebnis-Wrapper-Klasse */
    public static class CorrelationResult {
        public double correlation;
        public CorrelationResult(double correlation) { this.correlation = correlation; }
    }

    @Procedure(name = "graphobs.analysis.correlation", mode = Mode.READ)
    @Description("Berechnet die Pearson- oder Kendall-Tau-Korrelation zwischen zwei univariaten Zeitreihen, " +
            "die jeweils als (timestamps, values) übergeben werden. Zeitpunkte müssen nicht exakt übereinstimmen.")
    public Stream<CorrelationResult> calculateCorrelation(
            @Name("timestamps1") List<String> timestamps1,
            @Name("values1")     Map<String, List<Double>> values1,
            @Name("timestamps2") List<String> timestamps2,
            @Name("values2")     Map<String, List<Double>> values2,
            //@Name(value = "method", defaultValue = "pearson") String method,
             @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {

        String metricKey1 = getAndValidateMetricKey(values1, "values1");
        String metricKey2 = getAndValidateMetricKey(values2, "values2");

        String method = params.getOrDefault("method", "pearson").toString();

        if (timestamps1.size() != values1.get(metricKey1).size()) {
            throw new IllegalArgumentException("timestamps1 und values1 müssen gleich lang sein.");
        }
        if (timestamps2.size() != values2.get(metricKey2).size()) {
            throw new IllegalArgumentException("timestamps2 und values2 müssen gleich lang sein.");
        }

        // --- 2) Eingabedaten für die Join-Strategie vorbereiten ---
        // Wir erstellen temporäre TimeSeriesResult-Objekte, um die standardisierte Schnittstelle zu nutzen.
        List<TimeSeriesResult> seriesList1 = Collections.singletonList(new TimeSeriesResult(timestamps1, values1));
        List<TimeSeriesResult> seriesList2 = Collections.singletonList(new TimeSeriesResult(timestamps2, values2));

        // --- 3) Temporal Join mit der ausgelagerten Strategie durchführen ---
        // Da diese Funktion immer interpoliert, instanziieren wir die Strategie direkt.
        TemporalJoinStrategy joinStrategy = JoinStrategyFactory.getStrategy((String) params.getOrDefault("join", "linear"));
        Map<String, Object> join_params = new HashMap<>();
        join_params.put("intervalSeconds", params.getOrDefault("intervalSeconds", 60));
        AlignedData alignedData = joinStrategy.align(seriesList1, metricKey1, seriesList2, metricKey2, join_params);

        if (alignedData.size() < 2) {
            return Stream.empty(); // Nicht genug Datenpunkte nach dem Join für eine Korrelation
        }
        // --- 1) Validierung & Werte extrahieren ---
        /*List<Double> v1 = extractSingleSeries(values1, "values1");
        List<Double> v2 = extractSingleSeries(values2, "values2");

        if (timestamps1.size() != v1.size()) {
            throw new IllegalArgumentException("timestamps1 und values1 müssen gleich lang sein.");
        }
        if (timestamps2.size() != v2.size()) {
            throw new IllegalArgumentException("timestamps2 und values2 müssen gleich lang sein.");
        }

        TreeMap<Instant, Double> series1 = new TreeMap<>(buildSeriesMap(timestamps1, v1));
        TreeMap<Instant, Double> series2 = new TreeMap<>(buildSeriesMap(timestamps2, v2));

        if (series1.isEmpty() || series2.isEmpty()) {
            return Stream.empty();
        }

        // --- 2) Gemeinsamen Zeitraum bestimmen ---
        Instant start = Collections.max(Arrays.asList(series1.firstKey(), series2.firstKey()));
        Instant end   = Collections.min(Arrays.asList(series1.lastKey(), series2.lastKey()));
        if (!start.isBefore(end)) {
            return Stream.empty();
        }

        // --- 3) Gemeinsame Zeitachse = Union der Zeitstempel innerhalb des Overlaps ---
        Set<Instant> allTimes = new TreeSet<>();
        allTimes.addAll(series1.subMap(start, true, end, true).keySet());
        allTimes.addAll(series2.subMap(start, true, end, true).keySet());

        List<Double> aligned1 = new ArrayList<>();
        List<Double> aligned2 = new ArrayList<>();
        for (Instant t : allTimes) {
            Double val1 = interpolate(series1, t);
            Double val2 = interpolate(series2, t);
            if (val1 != null && val2 != null) {
                aligned1.add(val1);
                aligned2.add(val2);
            }
        }

        if (aligned1.size() < 2) {
            return Stream.empty();
        }*/

        // --- 4) Korrelation auf den ausgerichteten Daten berechnen ---
        double coefficient;
        switch (method.toLowerCase()) {
            case "pearson":
                coefficient = calculatePearson(alignedData.valuesA, alignedData.valuesB);
                break;
            case "kendall":
                coefficient = calculateKendallTau(alignedData.valuesA, alignedData.valuesB);
                break;
            default:
                throw new IllegalArgumentException("Ungültige Methode: '" + method + "'. Erlaubt: 'pearson' oder 'kendall'.");
        }

        if (Double.isNaN(coefficient)) {
            return Stream.empty();
        }

        return Stream.of(new CorrelationResult(coefficient));
    }


    /**
     * Extrahiert den Schlüssel der einzigen Zeitreihe aus der Map und validiert, dass es nur eine gibt.
     */
    private static String getAndValidateMetricKey(Map<String, List<Double>> valuesMap, String paramName) {
        if (valuesMap == null || valuesMap.isEmpty()) {
            throw new IllegalArgumentException(paramName + " darf nicht leer sein und muss genau eine Serie enthalten.");
        }
        if (valuesMap.size() != 1) {
            throw new IllegalArgumentException(paramName + " enthält " + valuesMap.size() +
                    " Serien. Nur univariate Zeitreihen (genau eine Serie) sind erlaubt.");
        }
        return valuesMap.keySet().iterator().next();
    }

    /** Lineare Interpolation: liefert Wert am Zeitpunkt t, oder null wenn t außerhalb der Serie liegt */
    private static Double interpolate(NavigableMap<Instant, Double> series, Instant t) {
        if (series.containsKey(t)) return series.get(t);
        Map.Entry<Instant, Double> floor = series.floorEntry(t);
        Map.Entry<Instant, Double> ceil  = series.ceilingEntry(t);
        if (floor == null || ceil == null) return null;
        if (floor.getKey().equals(ceil.getKey())) return floor.getValue();

        double x0 = floor.getKey().toEpochMilli();
        double y0 = floor.getValue();
        double x1 = ceil.getKey().toEpochMilli();
        double y1 = ceil.getValue();
        double x  = t.toEpochMilli();
        return y0 + (y1 - y0) * (x - x0) / (x1 - x0);
    }

    /** Extrahiert genau eine Serie aus dem Map-Wrapper */
    private static List<Double> extractSingleSeries(Map<String, List<Double>> valuesMap, String paramName) {
        if (valuesMap == null || valuesMap.isEmpty()) {
            throw new IllegalArgumentException(paramName + " darf nicht leer sein und muss genau eine Serie enthalten.");
        }
        if (valuesMap.size() != 1) {
            throw new IllegalArgumentException(paramName + " enthält " + valuesMap.size() +
                    " Serien. Nur univariate Zeitreihen (genau eine Serie) sind erlaubt.");
        }
        return valuesMap.values().iterator().next();
    }

    /** Baut eine Map<Instant, Double> aus parallel übergebenen Listen. */
    private static Map<Instant, Double> buildSeriesMap(List<String> timestamps, List<Double> values) {
        Map<Instant, Double> map = new HashMap<>(timestamps.size());
        for (int i = 0; i < timestamps.size(); i++) {
            Double v = values.get(i);
            if (v == null || v.isNaN() || v.isInfinite()) continue;
            Instant ts = parseToInstant(timestamps.get(i));
            map.put(ts, v);
        }
        return map;
    }

    /** Robustes Parsing: ISO-8601, LocalDate, Epoch-Millis */
    private static Instant parseToInstant(String s) {
        if (s == null) throw new IllegalArgumentException("Timestamp darf nicht null sein.");
        try { return Instant.parse(s); } catch (DateTimeParseException ignored) {}
        try { return ZonedDateTime.parse(s, DateTimeFormatter.ISO_DATE_TIME).toInstant(); } catch (DateTimeParseException ignored) {}
        try { return LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant(); } catch (DateTimeParseException ignored) {}
        try { return Instant.ofEpochMilli(Long.parseLong(s)); } catch (NumberFormatException ignored) {}
        throw new IllegalArgumentException("Timestamp-Format nicht erkannt: " + s);
    }

    /** Pearson-Korrelation */
    private static double calculatePearson(double[] x, double[] y) {
        int n = x.length;
        double sum_x = 0.0, sum_y = 0.0, sum_xy = 0.0, sum_x_sq = 0.0, sum_y_sq = 0.0;
        for (int i = 0; i < n; i++) {
            sum_x += x[i];
            sum_y += y[i];
            sum_xy += x[i] * y[i];
            sum_x_sq += x[i] * x[i];
            sum_y_sq += y[i] * y[i];
        }
        double num = n * sum_xy - sum_x * sum_y;
        double den = Math.sqrt((n * sum_x_sq - sum_x * sum_x) * (n * sum_y_sq - sum_y * sum_y));
        return den == 0.0 ? 0.0 : num / den;
    }

    /** Kendall-Tau (Tau-a) */
    private static double calculateKendallTau(double[] x, double[] y) {
        int n = x.length, concordant = 0, discordant = 0;
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                boolean xu = x[j] > x[i], yu = y[j] > y[i];
                boolean xd = x[j] < x[i], yd = y[j] < y[i];
                if ((xu && yu) || (xd && yd)) concordant++;
                else if ((xu && yd) || (xd && yu)) discordant++;
            }
        }
        double total = n * (n - 1.0) / 2.0;
        return total == 0.0 ? 0.0 : (concordant - discordant) / total;
    }
}
