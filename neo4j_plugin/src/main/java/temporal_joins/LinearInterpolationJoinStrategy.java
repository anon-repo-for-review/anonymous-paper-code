package temporal_joins;

import result_classes.TimeSeriesResult;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Eine Temporal Join Strategie, die lineare Interpolation verwendet.
 * <p>
 * Diese Strategie erstellt eine gemeinsame, hochauflösende Zeitachse aus allen
 * einzigartigen Zeitpunkten beider Serien. Für jeden dieser Zeitpunkte wird der Wert
 * der jeweils anderen Serie linear interpoliert.
 * <p>
 * Dies ist ideal für kontinuierliche Daten wie Sensorwerte.
 */
public class LinearInterpolationJoinStrategy implements TemporalJoinStrategy {

    @Override
    public AlignedData align(
            List<TimeSeriesResult> seriesListA, String metricA,
            List<TimeSeriesResult> seriesListB, String metricB,
            Map<String, Object> params) {

        // Schritt 1: Konvertiere die komplexen Input-Objekte in einfache Maps.
        Map<Long, Double> mapA = TimeSeriesConverter.convert(seriesListA, metricA);
        Map<Long, Double> mapB = TimeSeriesConverter.convert(seriesListB, metricB);

        if (mapA.isEmpty() || mapB.isEmpty()) {
            return AlignedData.EMPTY;
        }

        // Überlappenden Zeitraum finden
        long minA = Collections.min(mapA.keySet());
        long maxA = Collections.max(mapA.keySet());
        long minB = Collections.min(mapB.keySet());
        long maxB = Collections.max(mapB.keySet());

        long overlapStart = Math.max(minA, minB);
        long overlapEnd = Math.min(maxA, maxB);

        if (overlapStart >= overlapEnd) {
            return AlignedData.EMPTY;
        }

        // 2. Gemeinsame, sortierte Zeitachse im Überlappungsbereich erstellen
        List<Long> mergedTimestamps = Stream.concat(mapA.keySet().stream(), mapB.keySet().stream())
                .filter(t -> t >= overlapStart && t <= overlapEnd)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        if (mergedTimestamps.size() < 2) {
            return AlignedData.EMPTY;
        }

        // 3. Arrays für Interpolation vorbereiten (müssen sortiert sein)
        long[] xsA = mapA.keySet().stream().sorted().mapToLong(Long::longValue).toArray();
        double[] ysA = Arrays.stream(xsA).mapToDouble(mapA::get).toArray();

        long[] xsB = mapB.keySet().stream().sorted().mapToLong(Long::longValue).toArray();
        double[] ysB = Arrays.stream(xsB).mapToDouble(mapB::get).toArray();

        // 4. Werte für jeden gemeinsamen Zeitpunkt interpolieren
        List<Double> alignedValuesA = new ArrayList<>(mergedTimestamps.size());
        List<Double> alignedValuesB = new ArrayList<>(mergedTimestamps.size());

        for (Long t : mergedTimestamps) {
            Double vA = interpolateAt(xsA, ysA, t);
            Double vB = interpolateAt(xsB, ysB, t);

            // Nur Paare hinzufügen, bei denen beide Werte erfolgreich interpoliert werden konnten
            if (vA != null && vB != null) {
                alignedValuesA.add(vA);
                alignedValuesB.add(vB);
            }
        }

        if (alignedValuesA.isEmpty()){
            return AlignedData.EMPTY;
        }

        double[] finalA = alignedValuesA.stream().mapToDouble(Double::doubleValue).toArray();
        double[] finalB = alignedValuesB.stream().mapToDouble(Double::doubleValue).toArray();

        return new AlignedData(finalA, finalB);
    }

    /**
     * Berechnet einen interpolierten Wert an einem bestimmten Zeitpunkt t.
     *
     * @param xs Sortiertes Array von Zeitstempeln (x-Achse).
     * @param ys Array von Werten (y-Achse).
     * @param t  Der Zeitpunkt, für den der Wert interpoliert werden soll.
     * @return Den interpolierten Wert oder null, wenn t außerhalb des Bereichs von xs liegt.
     */
    private static Double interpolateAt(long[] xs, double[] ys, long t) {
        if (t < xs[0] || t > xs[xs.length - 1]) {
            return null;
        }

        int idx = Arrays.binarySearch(xs, t);
        if (idx >= 0) {
            return ys[idx]; // Exakter Treffer
        }

        int ins = -idx - 1; // Einfügepunkt
        if (ins == 0 || ins >= xs.length) {
            return null; // Sollte durch die erste Prüfung abgedeckt sein, aber zur Sicherheit
        }

        int left = ins - 1;
        int right = ins;

        long x0 = xs[left];
        long x1 = xs[right];
        double y0 = ys[left];
        double y1 = ys[right];

        if (x1 == x0) return y0; // Verhindert Division durch Null

        double ratio = (double) (t - x0) / (double) (x1 - x0);
        return y0 + ratio * (y1 - y0);
    }
}