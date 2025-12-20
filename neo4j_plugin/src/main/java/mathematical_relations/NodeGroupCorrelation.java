package mathematical_relations;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import temporal_joins.AlignedData;
import temporal_joins.JoinStrategyFactory;
import temporal_joins.TemporalJoinStrategy;
import util.TimeSeriesUtil;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NodeGroupCorrelation {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    public static class CorrelationResult {
        public Node node;
        public double correlation;

        public CorrelationResult(Node node, double correlation) {
            this.node = node;
            this.correlation = correlation;
        }
    }

    @Procedure(name = "graphobs.analysis.node_group_correlation", mode = Mode.READ)
    @Description("Berechnet die Korrelation (pearson/kendall) zwischen einer Metrik eines Knotens und " +
            "einer Metrik einer Menge von Knoten. Interpoliert auf gemeinsame Zeitpunkte.")
    public Stream<CorrelationResult> nodeGroupCorrelation(
            @Name("nodeA") Node nodeA,
            @Name("metricA") String metricA,
            @Name("nodesB") List<Node> nodesB,
            @Name("metricB") String metricB,
            //@Name(value = "method", defaultValue = "pearson") String method,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        try {

            String method = params.getOrDefault("method", "pearson").toString();

            // 1) hole alle TimeSeriesResult für Node A (kann mehrere Serien liefern)
            List<TimeSeriesResult> seriesA = TimeSeriesUtil.getFilteredTimeSeries(nodeA, metricA, params, db, log)
                    .collect(Collectors.toList());
            if (seriesA.isEmpty()) {
                log.warn("Keine Zeitreihen für Node A gefunden: " + nodeA.getElementId());
                return Stream.empty();
            }

            // für A: wir vereinfachen und flachen alle Serien zu einer "kombinierten" Serie zusammen,
            // indem wir alle (timestamp,value)-Paare in eine Map<Long,Double> packen (letzter Wert gewinnt).
            /*Map<Long, Double> combinedA = combineSeriesToMillisMap(seriesA, metricA);
            if (combinedA.isEmpty()) {
                log.warn("Node A hat keine verwertbaren Werte für metric " + metricA);
                return Stream.empty();

            }*/

            String joinMethodName = (String) params.getOrDefault("join", "resample");
            TemporalJoinStrategy joinStrategy = JoinStrategyFactory.getStrategy(joinMethodName);
            Map<String, Object> join_params = new HashMap<>();
            join_params.put("intervalSeconds", params.getOrDefault("intervalSeconds", 60));

            List<CorrelationResult> results = new ArrayList<>();

            // Für jeden Node B separat rechnen
            for (Node nodeB : nodesB) {
                //AlignedData alignedData = new AlignedData(new double[0], new double[0]);
                try {
                    List<TimeSeriesResult> seriesB = TimeSeriesUtil.getFilteredTimeSeries(nodeB, metricB, params, db, log)
                            .collect(Collectors.toList());

                    log.info(seriesB.get(0).values.toString());
                    log.info(seriesB.get(0).timestamps.toString());
                    if (seriesB.isEmpty()) {
                        // keine serien für nodeB -> skip
                        continue;
                    }


                    AlignedData alignedData = joinStrategy.align(seriesA, metricA, seriesB, metricB, join_params);


                    // 5. Verarbeite das Ergebnis des Joins
                    if (alignedData.size() < 2) {
                        // Nicht genug gepaarte Punkte, um eine sinnvolle Korrelation zu berechnen
                        continue;
                    }

                    /*Map<Long, Double> combinedB = combineSeriesToMillisMap(seriesB, metricB);
                    if (combinedB.isEmpty()) continue;



                    // Ermittle min/max beider Serien
                    long minA = Collections.min(combinedA.keySet());
                    long maxA = Collections.max(combinedA.keySet());
                    long minB = Collections.min(combinedB.keySet());
                    long maxB = Collections.max(combinedB.keySet());

                    long overlapStart = Math.max(minA, minB);
                    long overlapEnd = Math.min(maxA, maxB);

                    if (overlapStart >= overlapEnd) {
                        // kein überlappender Zeitraum
                        continue;
                    }

                    // Erzeuge sortierte, einzigartige Liste aller Zeitpunkte aus A und B, die im Overlap liegen
                    List<Long> mergedTimestamps = Stream.concat(combinedA.keySet().stream(), combinedB.keySet().stream())
                            .filter(t -> t >= overlapStart && t <= overlapEnd)
                            .distinct()
                            .sorted()
                            .collect(Collectors.toList());

                    if (mergedTimestamps.size() < 2) {
                        // zu wenige Punkte zum Berechnen der Korrelation
                        continue;
                    }

                    // Baue sortierte arrays für Interpolation
                    long[] xsA = combinedA.keySet().stream().sorted().mapToLong(Long::longValue).toArray();
                    double[] ysA = Arrays.stream(xsA).mapToDouble(x -> combinedA.get(x)).toArray();

                    long[] xsB = combinedB.keySet().stream().sorted().mapToLong(Long::longValue).toArray();
                    double[] ysB = Arrays.stream(xsB).mapToDouble(x -> combinedB.get(x)).toArray();

                    List<Double> alignedA = new ArrayList<>(mergedTimestamps.size());
                    List<Double> alignedB = new ArrayList<>(mergedTimestamps.size());

                    for (Long t : mergedTimestamps) {
                        Double vA = interpolateAt(xsA, ysA, t);
                        Double vB = interpolateAt(xsB, ysB, t);
                        if (vA == null || vB == null) continue; // falls außerhalb Range oder interpolation nicht möglich
                        alignedA.add(vA);
                        alignedB.add(vB);
                    }

                    if (alignedA.size() < 2) continue; // nicht genug gepaarte Punkte

                    double[] arrA = alignedA.stream().mapToDouble(Double::doubleValue).toArray();
                    double[] arrB = alignedB.stream().mapToDouble(Double::doubleValue).toArray();*/

                    log.info(Arrays.toString(alignedData.valuesA));

                    log.info(Arrays.toString(alignedData.valuesB));

                    double coeff;
                    switch (method.toLowerCase()) {
                        case "pearson":
                            coeff = calculatePearson(alignedData.valuesA, alignedData.valuesB);
                            break;
                        case "kendall":
                            coeff = calculateKendallTau(alignedData.valuesA, alignedData.valuesB);
                            break;
                        default:
                            throw new IllegalArgumentException("Unbekannte Methode: " + method);
                    }

                    results.add(new CorrelationResult(nodeB, coeff));
                } catch (Exception e) {
                    log.warn("Fehler bei Node B " + nodeB.getElementId() + ": " + e.getMessage());
                    //log.info(Arrays.toString(alignedData.valuesA));
                }
            }

            return results.stream();

        } catch (Exception ex) {
            log.error("Unexpected error in node_group_correlation: " + ex.getMessage());
            return Stream.empty();
        }
    }

    /*private static Map<Long, Double> combineSeriesToMillisMap(List<TimeSeriesResult> seriesList, String preferredMetricKey) {
        Map<Long, Double> map = new HashMap<>();
        if (seriesList == null) return map;

        for (TimeSeriesResult ts : seriesList) {
            if (ts == null || ts.timestamps == null) continue;

            for (int i = 0; i < ts.timestamps.size(); i++) {
                try {
                    ZonedDateTime zdt = TimeSeriesUtil.parseToZonedDateTime(ts.timestamps.get(i));
                    long epochMillis = zdt.toInstant().toEpochMilli();

                    Double v = null;
                    if (ts.values != null) {
                        // prefer the metric key if present
                        if (ts.values.containsKey(preferredMetricKey)) {
                            List<Double> list = ts.values.get(preferredMetricKey);
                            if (list == null && !ts.values.isEmpty()) {
                                list = ts.values.values().iterator().next();
                            }


                            if (list != null && list.size() > i) v = list.get(i);
                        } else {
                            // fallback: pick the first series that has a value at index i
                            for (List<Double> list : ts.values.values()) {
                                if (list != null && list.size() > i) {
                                    v = list.get(i);
                                    break;
                                }
                            }
                        }
                    }

                    if (v != null && !v.isNaN() && !v.isInfinite()) {
                        map.put(epochMillis, v);
                    }
                } catch (Exception ignored) {
                    // skip unparsable timestamp / index issues
                }
            }
        }
        return map;
    }

    // Lineare Interpolation: xs muss sortiertes long[] sein, ys entsprechend.
    // Rückgabe: interpolierter Wert als Double, oder null wenn t außerhalb [xs[0], xs[last]].
    private static Double interpolateAt(long[] xs, double[] ys, long t) {
        if (xs == null || xs.length == 0) return null;
        if (t < xs[0] || t > xs[xs.length - 1]) return null;
        // exakter Treffer?
        int idx = Arrays.binarySearch(xs, t);
        if (idx >= 0) return ys[idx];
        // binarySearch returns (-(insertion point) - 1)
        int ins = -idx - 1;
        if (ins == 0 || ins >= xs.length) return null; // t outside
        int left = ins - 1;
        int right = ins;
        long x0 = xs[left];
        long x1 = xs[right];
        double y0 = ys[left];
        double y1 = ys[right];
        if (x1 == x0) return y0; // should not happen
        double ratio = (double) (t - x0) / (double) (x1 - x0);
        return y0 + ratio * (y1 - y0);
    }*/

    // Pearson & Kendall wie vorher (kopiert)
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
