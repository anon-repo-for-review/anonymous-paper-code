package util;

import java.util.*;
import java.util.stream.DoubleStream;

public final class StatisticUtil {

    private StatisticUtil() {
        // Utility-Klasse
    }

    /**
     * Berechnet Standardstatistiken für eine oder mehrere Zeitreihen.
     * @param valueSeries Map von Series-Name zu List<Double> Werten
     * @param percentileValue gewünschtes Perzentil (z.B. 95)
     * @return Map mit min, max, mean, median, stddev, sum, count, percentile
     */
    public static Map<String, Double> calculateStatistics(Map<String, List<Double>> valueSeries, double percentileValue) {
        List<Double> allValues = new ArrayList<>();
        valueSeries.values().forEach(allValues::addAll);

        double[] sorted = allValues.stream()
                .map(v -> (v == null || v.isNaN() || v.isInfinite()) ? 0.0 : v)
                .mapToDouble(Double::doubleValue)
                .sorted()
                .toArray();

        if (sorted.length == 0) return Map.of();

        double min = sorted[0];
        double max = sorted[sorted.length - 1];
        double sum = DoubleStream.of(sorted).sum();
        double mean = sum / sorted.length;
        double median = sorted.length % 2 == 0
                ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0
                : sorted[sorted.length / 2];
        double variance = DoubleStream.of(sorted).map(v -> Math.pow(v - mean, 2)).sum() / sorted.length;
        double stddev = Math.sqrt(variance);
        double percentile = calcPercentile(sorted, percentileValue);

        Map<String, Double> stats = new LinkedHashMap<>();
        stats.put("min", min);
        stats.put("max", max);
        stats.put("mean", mean);
        stats.put("median", median);
        stats.put("stddev", stddev);
        stats.put("sum", sum);
        stats.put("count", (double) sorted.length);
        stats.put(String.format("percentile_%s", percentileValue), percentile);

        return stats;
    }

    private static double calcPercentile(double[] sorted, double percentile) {
        if (sorted.length == 0) return Double.NaN;
        double index = percentile / 100.0 * (sorted.length - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) return sorted[lower];
        return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
    }
}
