package comparison;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.ComparisonResult;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.util.*;
import java.util.stream.Stream;

public class ProportionComparison {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    @Procedure(name = "graphobs.comparison.compare_proportions", mode = Mode.READ)
    @Description("Compares two proportions by fetching time series data for trials and events from two nodes. " +
            "Signature: compare_proportions(node1, params1, node2, params2, trialMetric, eventMetric, options)")
    public Stream<ComparisonResult> compareProportionsFromMetrics(
            @Name("node1") Node node1,
            @Name("params1") Map<String, Object> params1,
            @Name("node2") Node node2,
            @Name("params2") Map<String, Object> params2,
            @Name("trialMetric") String trialMetric,
            @Name("eventMetric") String eventMetric,
            @Name(value = "options", defaultValue = "{}") Map<String, Object> options
    ) {
        // Basic validation
        if (node1 == null || node2 == null) throw new IllegalArgumentException("Node parameters cannot be null.");
        if (trialMetric == null || eventMetric == null)
            throw new IllegalArgumentException("Metric names cannot be null.");

        params1 = (params1 == null) ? Collections.emptyMap() : params1;
        params2 = (params2 == null) ? Collections.emptyMap() : params2;
        options = (options == null) ? Collections.emptyMap() : options;

        String eventType = options.getOrDefault("eventType", "relative").toString().toLowerCase();
        double alpha = parseAlpha(options.get("alpha"), 0.05);

        // Aggregate per-bucket successes and trials for both groups (corrected semantics)
        double[] aggA = aggregateSuccessesAndTrials(node1, trialMetric, eventMetric, params1, eventType);
        double successesADouble = aggA[0];
        double trialsADouble = aggA[1];

        double[] aggB = aggregateSuccessesAndTrials(node2, trialMetric, eventMetric, params2, eventType);
        double successesBDouble = aggB[0];
        double trialsBDouble = aggB[1];

        // compute proportions (use doubles to avoid rounding bias)
        double pA = (trialsADouble == 0.0) ? 0.0 : successesADouble / trialsADouble;
        double pB = (trialsBDouble == 0.0) ? 0.0 : successesBDouble / trialsBDouble;

        double pValue = Double.NaN;
        boolean significant = false;

        try {
            if (trialsADouble == 0.0 || trialsBDouble == 0.0) {
                log.warn(String.format("Cannot perform test, number of trials in one or both groups is zero. nodeA=%d, nodeB=%d", node1.getElementId(), node2.getElementId()));
                long samplesA = Math.round(trialsADouble);
                long samplesB = Math.round(trialsBDouble);
                return Stream.of(new ComparisonResult(pA, pB, Double.NaN, false, samplesA, samplesB));
            }

            // pooled proportion under H0 (use double sums)
            double pooledP = (successesADouble + successesBDouble) / (trialsADouble + trialsBDouble);
            double pooledVarTerm = pooledP * (1.0 - pooledP) * (1.0 / trialsADouble + 1.0 / trialsBDouble);
            double standardError = Math.sqrt(Math.max(0.0, pooledVarTerm));

            double zStatistic;
            if (standardError == 0) {
                zStatistic = (pA == pB) ? 0.0 : Double.POSITIVE_INFINITY * Math.signum(pA - pB);
            } else {
                zStatistic = (pA - pB) / standardError;
            }

            NormalDistribution standardNormal = new NormalDistribution(0, 1);
            pValue = 2 * (1 - standardNormal.cumulativeProbability(Math.abs(zStatistic)));
            significant = !Double.isNaN(pValue) && pValue < alpha;

            // Warn if counts are small (normal approx may be poor)
            long successesARounded = Math.round(successesADouble);
            long successesBRounded = Math.round(successesBDouble);
            long trialsARounded = Math.round(trialsADouble);
            long trialsBRounded = Math.round(trialsBDouble);
            if (successesARounded < 5 || successesBRounded < 5 || (trialsARounded - successesARounded) < 5 || (trialsBRounded - successesBRounded) < 5) {
                log.warn(String.format("Small-sample warning: successesA=%d, trialsA=%d, successesB=%d, trialsB=%d -- Normal approximation may be unreliable.",
                        successesARounded, trialsARounded, successesBRounded, trialsBRounded));
            }

        } catch (Exception ex) {
            log.error("Exception during proportion Z-test: " + ex.getMessage());
        }

        long samplesA = Math.round(trialsADouble);
        long samplesB = Math.round(trialsBDouble);
        return Stream.of(new ComparisonResult(pA, pB, pValue, significant, samplesA, samplesB));
    }


    /**
     * Aggregiert auf Bucket-Ebene: für jeden Index i werden trialsSeries[i] und eventSeries[i] kombiniert.
     * Korrekte Semantik (wie von dir gewünscht):
     *   - trials: Summe der *Werte* (raw, nicht gerundet) über alle Zeitschritte
     *   - successes: Summe über Zeitschritte von (trials_i * (proportion_i / 100))
     *
     * Rückgabe: double[2] = {sumSuccesses, sumTrials}
     *
     * Verhalten bei unterschiedlich langen Serien:
     *   - Solange sowohl trial- als auch event-Wert für einen Index existieren, werden sie gepaart.
     *   - Wenn es zusätzlich trial-Werte ohne zugehöriges event gibt, werden diese zu sumTrials addiert (angenommen event=0 für diese Buckets).
     *   - Wenn es event-Werte ohne trial-Werte gibt, werden sie ignoriert (mit Warnung), weil ohne trials keine Erfolge berechnet werden können.
     */
    private double[] aggregateSuccessesAndTrials(Node node, String trialMetric, String eventMetric, Map<String, Object> params, String eventType) {
        List<Double> trialsSeries = getTimeSeriesValues(node, trialMetric, params);
        List<Double> eventSeries = getTimeSeriesValues(node, eventMetric, params);

        int minLen = Math.min(trialsSeries.size(), eventSeries.size());
        double sumTrials = 0.0;
        double sumSuccesses = 0.0;

        // Pairing only while both series have values
        for (int i = 0; i < minLen; i++) {
            double rawT = trialsSeries.get(i) != null ? trialsSeries.get(i) : 0.0;
            double rawE = eventSeries.get(i) != null ? eventSeries.get(i) : 0.0;

            if (!Double.isFinite(rawT) || rawT < 0) rawT = 0.0;
            if (!Double.isFinite(rawE)) rawE = 0.0;

            // Accumulate raw trial value (do NOT round per-bucket)
            sumTrials += rawT * 60; //Umrechnung da rps pro minute gemittelt angeben werden. Daten einlesen sollte verbessert werden!!!!

            if ("absolute".equals(eventType)) {
                // treat event value as absolute count per bucket
                double eCount = Math.max(0.0, rawE);
                sumSuccesses += eCount;
            } else { // relative percent per bucket
                // rawE expected to be percent (0..100). Use as fraction.
                sumSuccesses += rawT * 60 * (rawE / 100.0); //Umrechnung da rps pro minute gemittelt angeben werden. Daten einlesen sollte verbessert werden!!!!
            }
        }

        // If trialsSeries has extra buckets without matching eventSeries, add them as trials with event=0
        if (trialsSeries.size() > minLen) {
            for (int i = minLen; i < trialsSeries.size(); i++) {
                double rawT = trialsSeries.get(i) != null ? trialsSeries.get(i) : 0.0;
                if (!Double.isFinite(rawT) || rawT < 0) rawT = 0.0;
                sumTrials += rawT; // assume event=0 for these buckets
            }
            log.warn(String.format("Node %d: trial series is longer than event series for metric '%s' (trials=%d, events=%d). Remaining trials treated as event=0.", node.getElementId(), eventMetric, trialsSeries.size(), eventSeries.size()));
        }

        // If eventSeries has extra buckets without trials, log and ignore (can't compute successes without trials)
        if (eventSeries.size() > minLen) {
            log.warn(String.format("Node %d: event series is longer than trial series for metric '%s' (trials=%d, events=%d). Extra event buckets ignored.", node.getElementId(), eventMetric, trialsSeries.size(), eventSeries.size()));
        }

        // Ensure non-negative
        if (sumTrials < 0) sumTrials = 0.0;
        if (sumSuccesses < 0) sumSuccesses = 0.0;

        return new double[]{sumSuccesses, sumTrials};
    }


    /**
     * Private Hilfsmethode, die eine Zeitreihe abruft und deren Werte als Liste zurückgibt.
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
            log.error(String.format("Error extracting time series values for node %d and metric '%s': %s", node.getElementId(), metric, e.getMessage()));
        }
        return Collections.emptyList();
    }

    private double parseAlpha(Object val, double defaultAlpha) {
        if (val == null) return defaultAlpha;
        try {
            if (val instanceof Number) return ((Number) val).doubleValue();
            if (val instanceof String) return Double.parseDouble((String) val);
        } catch (Exception ex) {
            log.warn(String.format("Could not parse alpha value '%s', using default=%f", val, defaultAlpha));
        }
        return defaultAlpha;
    }

}
