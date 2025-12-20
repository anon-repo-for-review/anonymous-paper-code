package comparison;

import org.apache.commons.math3.stat.inference.TTest;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.ComparisonResult;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static util.ComparisonUtil.computeMeansForNodeAndPeriod;

/**
 * Procedure to compare mean metric values between two groups of Pods using a two-sample t-test.
 *
 * Usage example in Cypher:
 *   MATCH (p:Pod)
 *   WITH collect(p) as pods
 *   CALL timegraph.comparison.compare_ts_mean(
 *       pods, "cpu_usage",
 *       { property: "x > 10", alpha: 0.05, start: 1710000000000, end: 1710003600000 }
 *   ) YIELD *
 */
public class CompareTimeSeriesProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private static final Pattern PROPERTY_EXPR =
            Pattern.compile("^\\s*([A-Za-z0-9_.]+)\\s*(==|=|!=|>=|<=|>|<)\\s*(.+)\\s*$");

    @Procedure(name = "graphobs.comparison.compare_ts_mean", mode = Mode.READ)
    @Description("Compare means of a metric across two groups of nodes. " +
            "Signature: compare_ts_mean(nodes, metric, options) where nodes is a list of Pod nodes.")
    public Stream<ComparisonResult> compareTsMean(
            @Name("nodes") List<Node> nodes,
            @Name("metric") String metric,
            @Name(value = "options", defaultValue = "{}") Map<String, Object> options
    ) {
        if (nodes == null || nodes.isEmpty()) {
            log.warn("No nodes provided to compare_ts_mean.");
            return Stream.empty();
        }
        if (metric == null || metric.isBlank()) {
            log.warn("No metric name provided to compare_ts_mean.");
            return Stream.empty();
        }

        // Parse grouping options
        String propertyFilter = options.containsKey("property") ? Objects.toString(options.get("property"), null) : null;
        String labelFilter    = options.containsKey("label") ? Objects.toString(options.get("label"), null) : null;
        String relation       = options.containsKey("relation") ? Objects.toString(options.get("relation"), null) : null;
        double alpha          = options.containsKey("alpha") ? ((Number) options.get("alpha")).doubleValue() : 0.05;

        List<Node> groupA = new ArrayList<>();
        List<Node> groupB = new ArrayList<>();

        // Gruppenzuordnung
        for (Node node : nodes) {
            boolean inA = false;
            try {
                if (propertyFilter != null) {
                    inA = evaluatePropertyExpression(node, propertyFilter);
                } else if (labelFilter != null) {
                    inA = node.hasLabel(Label.label(labelFilter));
                } else if (relation != null) {
                    inA = nodeMatchesRelation(node, relation);
                }
            } catch (Exception ex) {
                log.warn("Error while evaluating group membership for node id %s: %s", node.getElementId(), ex.getMessage());
                inA = false;
            }

            if (inA) groupA.add(node); else groupB.add(node);

            if (inA) {
                log.info("A: " + node.toString());
            } else {
                log.info("B: " + node.toString());
            }
        }

        // Mittelwerte für beide Gruppen berechnen
        double[] valuesA = collectPodMeans(groupA, metric, options);
        double[] valuesB = collectPodMeans(groupB, metric, options);

        log.info("S: " + String.valueOf(valuesA.length));
        log.info("S: " + String.valueOf(valuesB.length));

        double meanA = Double.NaN, meanB = Double.NaN, pValue = Double.NaN;
        boolean significant = false;
        TTest tTest = new TTest();

        try {
            meanA = mean(valuesA);
            meanB = mean(valuesB);

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
                meanA, meanB, pValue, significant,
                valuesA.length, valuesB.length
        );
        return Stream.of(result);
    }

    // -----------------------
    // Helper methods
    // -----------------------

    private boolean evaluatePropertyExpression(Node node, String expr) {
        Matcher m = PROPERTY_EXPR.matcher(expr);
        if (!m.matches()) {
            throw new IllegalArgumentException("Unsupported property expression: " + expr);
        }
        String propName = m.group(1);
        String op = m.group(2);
        String rhsRaw = m.group(3).trim();

        Object propVal = node.hasProperty(propName) ? node.getProperty(propName) : null;
        if (propVal == null) return false;

        Double leftNum = toDouble(propVal);
        Double rightNum = parseMaybeNumber(rhsRaw);

        if (leftNum != null && rightNum != null) {
            switch (op) {
                case "==":
                case "=":  return Double.compare(leftNum, rightNum) == 0;
                case "!=": return Double.compare(leftNum, rightNum) != 0;
                case ">":  return leftNum > rightNum;
                case "<":  return leftNum < rightNum;
                case ">=": return leftNum >= rightNum;
                case "<=": return leftNum <= rightNum;
                default: throw new IllegalArgumentException("Unsupported operator: " + op);
            }
        }

        String leftStr = propVal.toString();
        String rightStr = stripQuotes(rhsRaw);
        switch (op) {
            case "==":
            case "=":  return leftStr.equals(rightStr);
            case "!=": return !leftStr.equals(rightStr);
            default: throw new IllegalArgumentException("Operator " + op + " not supported for string operands.");
        }
    }

    private String stripQuotes(String s) {
        if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private Double parseMaybeNumber(String s) {
        try {
            return Double.parseDouble(stripQuotes(s));
        } catch (Exception e) {
            return null;
        }
    }

    private Double toDouble(Object o) {
        if (o instanceof Number) return ((Number) o).doubleValue();
        try { return Double.parseDouble(o.toString()); } catch (Exception e) { return null; }
    }

    private boolean nodeMatchesRelation(Node pod, String relationFragment) {
        String rel = relationFragment.trim();
        if (!rel.startsWith("-") && !rel.startsWith("(") && !rel.startsWith("<") && !rel.startsWith(":")) {
            rel = " " + rel;
        } else {
            rel = " " + rel;
        }

        String query = "MATCH (p) WHERE elementId(p) = $eid MATCH (p)" + rel + " RETURN count(*) AS c";
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> params = Map.of("eid", pod.getElementId());
            Result res = tx.execute(query, params);
            if (res.hasNext()) {
                Object cObj = res.next().get("c");
                long count = (cObj instanceof Number) ? ((Number) cObj).longValue() : Long.parseLong(cObj.toString());
                return count > 0;
            }
            return false;
        } catch (Exception e) {
            log.warn("Failed to evaluate relation pattern '%s' for pod id %s: %s",
                    relationFragment, pod.getElementId(), e.getMessage());
            return false;
        }
    }

    /**
     * Holt Mittelwerte für alle Pods in einer Gruppe.
     */
    private double[] collectPodMeans(List<Node> pods, String metric, Map<String,Object> options) {
        List<Double> means = new ArrayList<>();
        for (Node pod : pods) {
            try {
                List<Double> meanOpt = computeMeansForNodeAndPeriod(pod, metric, options, db, log);
                //meanOpt.ifPresent(means::add);
                means.addAll(meanOpt);
            } catch (Exception e) {
                log.warn("Failed to compute mean for pod id %s metric %s: %s",
                        pod.getElementId(), metric, e.getMessage());
            }
        }
        return means.stream().mapToDouble(Double::doubleValue).toArray();
    }



    /**
     * Mittelwert einer Liste berechnen.
     */
    private double mean(double[] arr) {
        if (arr == null || arr.length == 0) return Double.NaN;
        double s = 0.0;
        for (double v : arr) s += v;
        return s / arr.length;
    }

}
