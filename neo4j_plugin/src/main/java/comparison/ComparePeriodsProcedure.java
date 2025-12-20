package comparison;

import org.apache.commons.math3.stat.inference.TTest;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.ComparisonResult;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static util.ComparisonUtil.computeMeansForNodeAndPeriod;

/**
 * Compare per-node means across two time periods.
 *
 * - nodes: list of nodes to evaluate (e.g. Pods)
 * - metric: metric name to fetch from time series
 * - periodA / periodB: maps with time filter parameters (same semantics as TimeSeriesUtil.extractTimeWindow)
 * - options: optional, e.g. { "alpha": 0.05 }
 */
public class ComparePeriodsProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(name = "graphobs.comparison.compare_periods_means", mode = Mode.READ)
    @Description("Compare per-node means for two time periods. " +
            "Signature: compare_periods_means(nodes, metric, periodA, periodB, options)")
    public Stream<ComparisonResult> comparePeriodsMeans(
            @Name("nodes") List<Node> nodes,
            @Name("metric") String metric,
            @Name("periodA") Map<String, Object> periodA,
            @Name("periodB") Map<String, Object> periodB,
            @Name(value = "options", defaultValue = "{}") Map<String, Object> options
    ) {
        if (nodes == null || nodes.isEmpty()) {
            log.warn("No nodes provided to compare_periods_means.");
            return Stream.empty();
        }
        if (metric == null || metric.isBlank()) {
            log.warn("No metric provided to compare_periods_means.");
            return Stream.empty();
        }
        if (periodA == null) periodA = Collections.emptyMap();
        if (periodB == null) periodB = Collections.emptyMap();
        if (options == null) options = Collections.emptyMap();

        double alpha = options.containsKey("alpha") ? ((Number) options.get("alpha")).doubleValue() : 0.05;

        List<Double> meansA = new ArrayList<>();
        List<Double> meansB = new ArrayList<>();

        for (Node node : nodes) {
            try {
                // Für jeden Knoten: Mittelwert in Periode A und Periode B (falls vorhanden)
                List<Double> meanAOpt = computeMeansForNodeAndPeriod(node, metric, periodA, db, log);
                List<Double> meanBOpt = computeMeansForNodeAndPeriod(node, metric, periodB, db, log);

                meansA.addAll(meanAOpt);
                meansB.addAll(meanBOpt);

            } catch (Exception e) {
                log.warn("Failed for node %s while computing means: %s", node.getElementId(), e.getMessage());
            }
        }


        double meanA = meansA.isEmpty() ? Double.NaN : meanFromList(meansA);
        double meanB = meansB.isEmpty() ? Double.NaN : meanFromList(meansB);

        double pValue = Double.NaN;
        boolean significant = false;

        try {
            if (meansA.size() >= 2 && meansB.size() >= 2) {
                double[] arrA = meansA.stream().mapToDouble(Double::doubleValue).toArray();
                double[] arrB = meansB.stream().mapToDouble(Double::doubleValue).toArray();
                TTest tTest = new TTest();
                pValue = tTest.tTest(arrA, arrB);
                significant = !Double.isNaN(pValue) && pValue < alpha;
            } else {
                log.info("Not enough samples to run t-test: samplesA=" + meansA.size() + ", samplesB=" + meansB.size());
            }
        } catch (Exception ex) {
            log.error("Exception during t-test: " + ex.getMessage());
        }

        ComparisonResult result = new ComparisonResult(
                meanA,
                meanB,
                pValue,
                significant,
                meansA.size(),
                meansB.size()
        );

        return Stream.of(result);
    }


    /**
     * Vergleicht zwei Zeiträume für eine EINZELNE Zeitreihe (einen Knoten) mit einem gepaarten t-Test.
     * WICHTIG: Erfordert, dass beide Zeiträume die gleiche Anzahl an Datenpunkten haben.
     */
    @Procedure(name = "graphobs.comparison.compare_periods_paired", mode = Mode.READ)
    @Description("Compare two time periods for a single time series using a paired t-test. " +
            "Signature: compare_periods_paired(node, metric, periodA, periodB, options)")
    public Stream<ComparisonResult> comparePeriodsPaired(
            @Name("node") Node node,
            @Name("metric") String metric,
            @Name("timeParams") Map<String, Object> timeParams,
            @Name(value = "options", defaultValue = "{}") Map<String, Object> options
    ) {
        if (node == null) {
            log.warn("No node provided to compare_periods_paired.");
            return Stream.empty();
        }
        if (metric == null || metric.isBlank()) {
            log.warn("No metric provided to compare_periods_paired.");
            return Stream.empty();
        }
        if (options == null) options = Collections.emptyMap();

        // Schritt 1: Zeitparameter parsen
        Map<String, Map<String, Object>> periods;
        try {
            periods = parseTimeParameterization(timeParams);
        } catch (IllegalArgumentException e) {
            log.error("Failed to parse time parameters: " + e.getMessage());
            return Stream.empty();
        }
        Map<String, Object> periodA = periods.get("periodA");
        Map<String, Object> periodB = periods.get("periodB");

        double alpha = options.containsKey("alpha") ? ((Number) options.get("alpha")).doubleValue() : 0.05;


        // Schritt 1: Hole die Rohdaten für beide Zeiträume mithilfe der existierenden Util-Methode.
        List<Double> valuesA = getTimeSeriesValues(node, metric, periodA);
        List<Double> valuesB = getTimeSeriesValues(node, metric, periodB);

        // Schritt 2: Validierung für den gepaarten t-Test.
        if (valuesA.isEmpty() || valuesB.isEmpty()) {
            log.warn("Data for one or both periods is empty. Cannot perform paired t-test.");
            return Stream.empty();
        }

        if (valuesA.size() != valuesB.size()) {
            log.warn("Cannot perform paired t-test: sample sizes are different. Size A: %d, Size B: %d", valuesA.size(), valuesB.size());
            return Stream.empty();
        }

        double meanA = meanFromList(valuesA);
        double meanB = meanFromList(valuesB);

        double pValue = Double.NaN;
        boolean significant = false;

        try {
            // Für einen t-Test werden mindestens 2 Datenpunkte (Paare) benötigt.
            if (valuesA.size() >= 2) {
                double[] arrA = valuesA.stream().mapToDouble(Double::doubleValue).toArray();
                double[] arrB = valuesB.stream().mapToDouble(Double::doubleValue).toArray();
                TTest tTest = new TTest();
                // Führe den gepaarten t-Test durch.
                pValue = tTest.pairedTTest(arrA, arrB);
                significant = !Double.isNaN(pValue) && pValue < alpha;
            } else {
                log.info("Not enough paired samples to run t-test: samples=" + valuesA.size());
            }
        } catch (Exception ex) {
            log.error("Exception during paired t-test for node %s: %s", node.getElementId(), ex.getMessage());
        }

        ComparisonResult result = new ComparisonResult(
                meanA,
                meanB,
                pValue,
                significant,
                valuesA.size(),
                valuesB.size()
        );

        return Stream.of(result);
    }

    /**
     * Private Hilfsmethode, um die Rohwerte einer Zeitreihe zu extrahieren.
     * Sie kapselt den Aufruf von TimeSeriesUtil und die Extraktion der Werteliste.
     */
    private List<Double> getTimeSeriesValues(Node node, String metric, Map<String, Object> period) {
        try (Stream<TimeSeriesResult> resultsStream = TimeSeriesUtil.getFilteredTimeSeries(node, metric, period, db, log)) {
            Optional<TimeSeriesResult> firstResult = resultsStream.findFirst();

            if (firstResult.isPresent()) {
                TimeSeriesResult tsResult = firstResult.get();
                if (tsResult.values != null) {
                    // Primärversuch: Metrik mit dem exakten Namen holen.
                    if (tsResult.values.containsKey(metric)) {
                        return tsResult.values.get(metric) != null ? tsResult.values.get(metric) : Collections.emptyList();
                    }
                    // Fallback: Wenn nur eine Serie da ist, diese nehmen.
                    if (tsResult.values.size() == 1) {
                        List<Double> values = tsResult.values.values().iterator().next();
                        return values != null ? values : Collections.emptyList();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error extracting time series values for node %d and metric '%s': %s", node.getElementId(), metric, e.getMessage());
        }
        return Collections.emptyList();
    }



    private double meanFromList(List<Double> list) {
        if (list == null || list.isEmpty()) return Double.NaN;
        double s = 0.0;
        for (Double d : list) {
            if (d == null || d.isNaN() || d.isInfinite()) continue;
            s += d;
        }
        return s / list.size();
    }




    /**
     * Parst den `timeParams`-Parameter und gibt eine Map mit `periodA` und `periodB` zurück.
     * Unterstützt das neue Format {start1, start2, range} und das alte {periodA, periodB}.
     */
    private Map<String, Map<String, Object>> parseTimeParameterization(Map<String, Object> timeParams) {
        if (timeParams == null) {
            return Map.of("periodA", Collections.emptyMap(), "periodB", Collections.emptyMap());
        }

        // Neues, vereinfachtes Format: {start1, start2, range}
        if (timeParams.containsKey("start1") && timeParams.containsKey("start2") && timeParams.containsKey("range")) {
            try {
                long startTimeA = TimeSeriesUtil.parseTimeParameterToMillis(timeParams.get("start1"));
                long startTimeB = TimeSeriesUtil.parseTimeParameterToMillis(timeParams.get("start2"));
                long durationMillis = TimeSeriesUtil.parseDuration(timeParams.get("range"));

                if (durationMillis < 0) {
                    throw new IllegalArgumentException("'range' cannot be negative.");
                }

                Map<String, Object> periodA = Map.of("startTime", startTimeA, "endTime", startTimeA + durationMillis);
                Map<String, Object> periodB = Map.of("startTime", startTimeB, "endTime", startTimeB + durationMillis);

                return Map.of("periodA", periodA, "periodB", periodB);

            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid parameters in {start1, start2, range} format. " + e.getMessage(), e);
            }
        }

        // Altes (explizites) Format: {periodA: {...}, periodB: {...}}
        if (timeParams.containsKey("periodA") && timeParams.containsKey("periodB")) {
            try {
                Map<String, Object> periodA = (Map<String, Object>) timeParams.get("periodA");
                Map<String, Object> periodB = (Map<String, Object>) timeParams.get("periodB");

                return Map.of(
                        "periodA", periodA != null ? periodA : Collections.emptyMap(),
                        "periodB", periodB != null ? periodB : Collections.emptyMap()
                );
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("'periodA' and 'periodB' must be maps.", e);
            }
        }

        throw new IllegalArgumentException("Invalid time parameters. Provide either {start1, start2, range} or {periodA: {...}, periodB: {...}}.");
    }
}
