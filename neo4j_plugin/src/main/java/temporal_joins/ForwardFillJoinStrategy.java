package temporal_joins;



import result_classes.TimeSeriesResult;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Eine Temporal Join Strategie, die "Forward Fill" oder "Last Observation Carried Forward" (LOCF) anwendet.
 * <p>
 * Diese Strategie erzeugt eine "Treppenfunktion". Der Wert eines Messpunktes wird so lange
 * konstant gehalten, bis ein neuer Messpunkt in der Serie auftritt.
 * <p>
 * Dies ist ideal für diskrete, zustandsbasierte Daten (z.B. Status-Änderungen).
 */
public class ForwardFillJoinStrategy implements TemporalJoinStrategy {

    @Override
    public AlignedData align(
            List<TimeSeriesResult> seriesListA, String metricA,
            List<TimeSeriesResult> seriesListB, String metricB,
            Map<String, Object> params) {

        // Schritt 1: Konvertiere die komplexen Input-Objekte in einfache Maps.
        Map<Long, Double> mapA = TimeSeriesConverter.convert(seriesListA, metricA);
        Map<Long, Double> mapB = TimeSeriesConverter.convert(seriesListB, metricB);

        // Ab hier ist der Code identisch zur vorherigen Version!
        if (mapA.isEmpty() || mapB.isEmpty()) {
            return AlignedData.EMPTY;
        }

        // 1. Überlappenden Zeitraum finden (hier nicht zwingend nötig, aber konsistent)
        long minA = Collections.min(mapA.keySet());
        long maxA = Collections.max(mapA.keySet());
        long minB = Collections.min(mapB.keySet());
        long maxB = Collections.max(mapB.keySet());

        long overlapStart = Math.max(minA, minB);
        long overlapEnd = Math.min(maxA, maxB);

        if (overlapStart > overlapEnd) {
            return AlignedData.EMPTY;
        }

        // 2. Gemeinsame Zeitachse erstellen
        List<Long> mergedTimestamps = Stream.concat(mapA.keySet().stream(), mapB.keySet().stream())
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        // 3. Sortierte Timestamps für schnelle Suche vorbereiten
        List<Long> sortedKeysA = new ArrayList<>(mapA.keySet());
        Collections.sort(sortedKeysA);
        List<Long> sortedKeysB = new ArrayList<>(mapB.keySet());
        Collections.sort(sortedKeysB);

        // 4. Werte für jeden gemeinsamen Zeitpunkt mittels "last known value" ermitteln
        List<Double> alignedValuesA = new ArrayList<>();
        List<Double> alignedValuesB = new ArrayList<>();

        for (Long t : mergedTimestamps) {
            Double vA = findLastValueAt(t, mapA, sortedKeysA);
            Double vB = findLastValueAt(t, mapB, sortedKeysB);

            // Nur Paare hinzufügen, wenn für beide Serien ein Wert existiert (d.h. der Zeitpunkt
            // liegt nach dem ersten Messpunkt beider Serien).
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
     * Findet den Wert des letzten Zeitpunkts, der kleiner oder gleich t ist.
     *
     * @param t Der Zeitpunkt für die Suche.
     * @param series Die Map der Zeitreihe.
     * @param sortedKeys Eine vorsortierte Liste der Schlüssel aus der Map.
     * @return Den letzten bekannten Wert oder null, wenn kein Wert vor oder bei t existiert.
     */
    private static Double findLastValueAt(long t, Map<Long, Double> series, List<Long> sortedKeys) {
        int idx = Collections.binarySearch(sortedKeys, t);

        if (idx >= 0) {
            // Exakter Treffer
            return series.get(sortedKeys.get(idx));
        }

        int ins = -idx - 1; // Einfügepunkt
        if (ins == 0) {
            // t liegt vor dem allerersten Datenpunkt
            return null;
        }

        // Der letzte Wert ist an der Position vor dem Einfügepunkt
        long lastKey = sortedKeys.get(ins - 1);
        return series.get(lastKey);
    }
}
