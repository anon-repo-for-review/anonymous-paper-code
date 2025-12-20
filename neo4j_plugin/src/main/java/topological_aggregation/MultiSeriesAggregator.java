package topological_aggregation;

import result_classes.TimeSeriesResult;
import temporal_joins.TimeSeriesConverter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Eine Hilfsklasse zur Aggregation von N Zeitreihen zu einer einzigen.
 * Führt eine Union aller Zeitstempel durch, interpoliert fehlende Werte linear
 * und berechnet dann Summe oder Durchschnitt.
 */
public class MultiSeriesAggregator {

    public enum AggregationType {
        SUM,
        AVERAGE
    }

    /**
     * Aggregiert eine Liste von TimeSeriesResults zu einem einzigen Result.
     */
    public static TimeSeriesResult aggregate(
            List<TimeSeriesResult> seriesList,
            String metric,
            AggregationType type
    ) {
        if (seriesList == null || seriesList.isEmpty()) {
            return new TimeSeriesResult(Collections.emptyList(), Collections.emptyMap());
        }

        // 1. Konvertiere alle Inputs in Maps<Long, Double> (Epoch Millis -> Wert)
        // Wir nutzen den Converter einzeln für jedes Ergebnis, da wir sie getrennt brauchen.
        List<Map<Long, Double>> dataMaps = new ArrayList<>();
        for (TimeSeriesResult ts : seriesList) {
            // Wir verpacken das einzelne Result in eine Liste, damit der Converter funktioniert
            Map<Long, Double> map = TimeSeriesConverter.convert(Collections.singletonList(ts), metric);
            if (!map.isEmpty()) {
                dataMaps.add(map);
            }
        }

        if (dataMaps.isEmpty()) {
            return new TimeSeriesResult(Collections.emptyList(), Collections.emptyMap());
        }

        // 2. Erstelle die "Union"-Timeline: Alle Zeitpunkte aller Serien sammeln
        // Wir beschränken uns auf den Bereich, in dem mindestens eine Serie Daten hat (global min/max)
        TreeSet<Long> timeline = new TreeSet<>();
        for (Map<Long, Double> map : dataMaps) {
            timeline.addAll(map.keySet());
        }

        if (timeline.isEmpty()) {
            return new TimeSeriesResult(Collections.emptyList(), Collections.emptyMap());
        }

        // Optional: Hier könnte man filtern auf [max(starts), min(ends)] wenn man nur vollen Overlap will.
        // Deine Logik war jedoch "Union", also behalten wir alle Punkte.

        // 3. Vorbereiten der Interpolations-Arrays für jede Map (für Performance)
        List<InterpolationHelper> helpers = dataMaps.stream()
                .map(InterpolationHelper::new)
                .collect(Collectors.toList());

        List<String> outTimestamps = new ArrayList<>(timeline.size());
        List<Double> outValues = new ArrayList<>(timeline.size());

        DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC"));

        // 4. Über die gemeinsame Zeitachse iterieren
        for (Long t : timeline) {
            double sum = 0.0;
            int countPresent = 0;

            for (InterpolationHelper helper : helpers) {
                Double v = helper.interpolate(t);

                if (v != null) {
                    sum += v;
                    countPresent++;
                }
            }

            double resultValue;
            if (type == AggregationType.SUM) {
                // Bei Summe: Fehlende Werte werden als 0 angenommen (sofern gewünscht)
                // oder man ignoriert sie. Deine Logik implizierte "addiere was da ist".
                resultValue = sum;
            } else { // AVERAGE
                if (countPresent == 0) {
                    resultValue = 0.0; // Oder NaN, je nach Anforderung
                } else {
                    resultValue = sum / countPresent;
                }
            }

            outTimestamps.add(formatter.format(Instant.ofEpochMilli(t)));
            outValues.add(resultValue);
        }

        Map<String, List<Double>> resultMap = new HashMap<>();
        resultMap.put(metric, outValues);

        return new TimeSeriesResult(outTimestamps, resultMap);
    }

    /**
     * Interne Helferklasse für effiziente Interpolation
     */
    private static class InterpolationHelper {
        private final long[] xs;
        private final double[] ys;

        public InterpolationHelper(Map<Long, Double> map) {
            // Sortierte Arrays erstellen
            this.xs = map.keySet().stream().mapToLong(l -> l).sorted().toArray();
            this.ys = new double[xs.length];
            for(int i=0; i<xs.length; i++) {
                this.ys[i] = map.get(xs[i]);
            }
        }

        public Double interpolate(long t) {
            // Standard Interpolations-Logik (binary search)
            if (xs.length == 0) return null;
            if (t < xs[0] || t > xs[xs.length - 1]) return null;

            int idx = Arrays.binarySearch(xs, t);
            if (idx >= 0) return ys[idx];

            int ins = -idx - 1;
            if (ins == 0 || ins >= xs.length) return null;

            long x0 = xs[ins - 1];
            long x1 = xs[ins];
            double y0 = ys[ins - 1];
            double y1 = ys[ins];

            if (x1 == x0) return y0;
            return y0 + (double)(t - x0) / (x1 - x0) * (y1 - y0);
        }
    }
}
