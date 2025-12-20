package ts_querys;


import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static util.TimeSeriesUtil.getFilteredTimeSeries;


public class ServiceAggregation_old {

/*    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Procedure(name = "timegraph.data.get_all", mode = Mode.READ)
    @Description("Aggregiert eine Metrik über alle Pods, die mit einem Server/Service-Knoten verbunden sind. " +
            "Für rps/cpu_total -> Summe, für latency_ms/error_rate_pct -> Durchschnitt. " +
            "Sonst Rückgabe aller Einzel-TS.")
    public Stream<TimeSeriesResult> aggregatePodMetric(
            @Name("node") Node startNode,
            @Name("metric") String metric,
            @Name(value = "params", defaultValue = "{}") Map<String,Object> params
    ) {
        if (startNode == null) {
            log.warn("aggregatePodMetric: startNode war null.");
            return Stream.empty();
        }

        try {
            // --- 1) Pods suchen ---
            List<Node> pods = new ArrayList<>();
            for (Relationship rel : startNode.getRelationships(Direction.BOTH)) {
                Node other = rel.getOtherNode(startNode);
                if (other.hasLabel(Label.label("Pod"))) {
                    pods.add(other);
                }
            }
            if (pods.isEmpty()) {
                log.warn("aggregatePodMetric: keine Pods gefunden.");
                return Stream.empty();
            }

            // --- 2) Zeitreihen laden ---
            List<TimeSeriesResult> series = new ArrayList<>();
            for (Node pod : pods) {
                getFilteredTimeSeries(pod, metric, params, db, log).forEach(series::add);
            }
            if (series.isEmpty()) {
                log.warn("aggregatePodMetric: keine Zeitreihen für Metrik '%s' gefunden.", metric);
                return Stream.empty();
            }

            // --- 3) Aggregation abhängig von Metrik ---
            switch (metric) {
                case "rps":
                case "cpu_total":
                    return Stream.of(combineSeriesInterpolated(series, metric, true));
                case "latency_ms":
                case "error_rate_pct":
                    return Stream.of(combineSeriesInterpolated(series, metric, false));
                default:
                    // Alle unverändert zurück
                    return series.stream().map(ts -> new TimeSeriesResult(ts.timestamps, ts.values));
            }

        } catch (Exception e) {
            log.error("aggregatePodMetric: Exception: %s", e.getMessage());
            return Stream.empty();
        }
    }

    /**
     * Aggregiert mehrere Zeitreihen mit linearer Interpolation.
     * @param series Zeitreihen
     * @param metric Metrik
     * @param sumMode true = Summieren, false = Durchschnitt
     */
    /*private static TimeSeriesResult combineSeriesInterpolated(List<TimeSeriesResult> series, String metric, boolean sumMode) {
        List<NavigableMap<Instant, Double>> mapped = new ArrayList<>();
        for (TimeSeriesResult ts : series) {
            if (ts.timestamps == null || ts.timestamps.isEmpty()) continue;
            List<String> timestamps = ts.timestamps;
            List<Double> values = ts.values.getOrDefault(metric, Collections.emptyList());
            if (values.size() != timestamps.size()) continue;

            TreeMap<Instant, Double> map = new TreeMap<>();
            for (int i = 0; i < timestamps.size(); i++) {
                try {
                    Instant t = TimeSeriesUtil.parseToZonedDateTime(timestamps.get(i)).toInstant();
                    Double v = values.get(i);
                    if (v != null && !v.isNaN() && !v.isInfinite()) {
                        map.put(t, v);
                    }
                } catch (Exception ignored) {
                }
            }
            if (!map.isEmpty()) mapped.add(map);
        }

        if (mapped.isEmpty()) {
            return new TimeSeriesResult(Collections.emptyList(), Collections.emptyMap());
        }

        // globaler Zeitraum: min aller Starts, max aller Ends (Union-Strategie)
        Instant globalStart = mapped.stream()
                .map(m -> m.firstKey())
                .min(Instant::compareTo)
                .orElseThrow();
        Instant globalEnd = mapped.stream()
                .map(m -> m.lastKey())
                .max(Instant::compareTo)
                .orElseThrow();

        // Timeline = Vereinigung aller Zeitpunkte (aus allen Serien), begrenzt auf [globalStart, globalEnd]
        TreeSet<Instant> timeline = new TreeSet<>();
        for (NavigableMap<Instant, Double> m : mapped) {
            for (Instant k : m.keySet()) {
                if (!k.isBefore(globalStart) && !k.isAfter(globalEnd)) timeline.add(k);
            }
        }

        // Wenn du stattdessen ein regelmäßiges Raster möchtest, würde man hier ein Gitter erzeugen.
        if (timeline.isEmpty()) {
            // Fallback: falls keine exakten Zeitstempel im Intervall vorhanden, erstelle leere Rückgabe
            return new TimeSeriesResult(Collections.emptyList(), Collections.emptyMap());
        }

        List<String> outTimestamps = new ArrayList<>(timeline.size());
        List<Double> aggregated = new ArrayList<>(timeline.size());

        for (Instant t : timeline) {
            double sum = 0.0;
            int countPresent = 0;
            for (NavigableMap<Instant, Double> serie : mapped) {
                Double v = interpolateAt(serie, t); // null wenn außerhalb Serie
                if (v != null) {
                    sum += v;
                    countPresent++;
                } else {
                    // für sumMode: fehlende Werte als 0 behandeln (sum += 0.0)
                    // für meanMode: fehlende Werte nicht zählen (countPresent nicht erhöhen)
                }
            }

            double outVal;
            if (sumMode) {
                outVal = sum; // missing treated as 0
            } else {
                outVal = (countPresent == 0) ? 0.0 : (sum / (double) countPresent);
            }

            outTimestamps.add(t.toString());
            aggregated.add(outVal);
        }

        Map<String, List<Double>> valuesMap = new HashMap<>();
        valuesMap.put(metric, aggregated);
        return new TimeSeriesResult(outTimestamps, valuesMap);
    }

    /** Lineare Interpolation: returns interpolated value or null if t outside [first,last] of serie */
    /*private static Double interpolateAt(NavigableMap<Instant, Double> serie, Instant t) {
        if (serie.containsKey(t)) return serie.get(t);

        Map.Entry<Instant, Double> floor = serie.floorEntry(t);
        Map.Entry<Instant, Double> ceil = serie.ceilingEntry(t);

        if (floor == null || ceil == null) return null; // außerhalb Bereich -> kein Wert

        Instant t0 = floor.getKey();
        Instant t1 = ceil.getKey();
        double y0 = floor.getValue();
        double y1 = ceil.getValue();

        if (t1.equals(t0)) return y0;
        double fraction = (double) (t.toEpochMilli() - t0.toEpochMilli()) / (double) (t1.toEpochMilli() - t0.toEpochMilli());
        return y0 + fraction * (y1 - y0);
    }*/

}
