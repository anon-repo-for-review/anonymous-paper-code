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
 * Procedure to compare mean metric values between two explicit groups of Pods using a two-sample t-test.
 *
 * Usage example in Cypher:
 *   MATCH (p:Pod) WHERE p.group = "A"
 *   WITH collect(p) as groupA
 *   MATCH (q:Pod) WHERE q.group = "B"
 *   WITH groupA, collect(q) as groupB
 *   CALL timegraph.comparison.compare_two_groups(groupA, groupB, "cpu_usage", {startTime: "...", endTime: "..."}, {alpha: 0.05}) YIELD *
 */
public class CompareTwoGroupsProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(name = "graphobs.comparison.compare_two_groups", mode = Mode.READ)
    @Description("Compare means of a metric across two explicitly provided groups of Pod nodes. " +
            "Params can contain time-window filters forwarded to the TimeSeriesUtil (e.g. time/range or startTime/endTime).")
    public Stream<ComparisonResult> compareTwoGroups(
            @Name("groupA") List<Node> groupA,
            @Name("groupB") List<Node> groupB,
            @Name("metric") String metric,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params,
            @Name(value = "options", defaultValue = "{}") Map<String, Object> options
    ) {
        if (groupA == null || groupB == null || groupA.isEmpty() || groupB.isEmpty()) {
            log.warn("One or both groups are empty.");
            return Stream.empty();
        }
        if (metric == null || metric.isBlank()) {
            log.warn("No metric name provided.");
            return Stream.empty();
        }

        double alpha = options != null && options.containsKey("alpha")
                ? ((Number) options.get("alpha")).doubleValue()
                : 0.05;

        // Compute per-pod means (uses TimeSeriesUtil with params)
        double[] valuesA = collectPodMeans(groupA, metric, params);
        double[] valuesB = collectPodMeans(groupB, metric, params);

        double meanA = Double.NaN, meanB = Double.NaN, pValue = Double.NaN;
        boolean significant = false;
        TTest tTest = new TTest();


        try {
            meanA = mean(valuesA);
            meanB = mean(valuesB);

            // require at least 2 samples per group
            if (valuesA.length >= 2 && valuesB.length >= 2) {
                pValue = tTest.tTest(valuesA, valuesB);
                significant = !Double.isNaN(pValue) && pValue < alpha;
            } else {
                log.info("Not enough samples for t-test: groupA=%d, groupB=%d", valuesA.length, valuesB.length);
            }
        } catch (Exception ex) {
            log.error("Exception while performing t-test: %s", ex.getMessage());
        }

        ComparisonResult result = new ComparisonResult(
                meanA,
                meanB,
                pValue,
                significant,
                valuesA.length,
                valuesB.length
        );
        return Stream.of(result);
    }

    // -----------------------
    // Helpers
    // -----------------------

    private double[] collectPodMeans(List<Node> pods, String metric, Map<String, Object> params) {
        List<Double> means = new ArrayList<>();
        for (Node pod : pods) {
            try {
                List<Double> meanList = computeMeansForNodeAndPeriod(pod, metric, params, db, log);
                if (meanList != null && !meanList.isEmpty()) {
                    means.addAll(meanList);
                }
            } catch (Exception e) {
                log.warn("Failed to compute mean for pod id %s metric %s: %s", pod.getElementId(), metric, e.getMessage());
            }
        }
        return means.stream().mapToDouble(Double::doubleValue).toArray();
    }


    private double mean(double[] arr) {
        if (arr == null || arr.length == 0) return Double.NaN;
        double s = 0.0;
        for (double v : arr) s += v;
        return s / arr.length;
    }


}
