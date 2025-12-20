package temporal_joins;

import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil; // Annahme: Hier ist parseToZonedDateTime

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Eine Temporal Join Strategie, die Resampling durchführt.
 * <p>
 * Beide Zeitreihen werden auf ein festes Zeitraster heruntergebrochen ("resampled"),
 * indem die Werte innerhalb jedes Zeitintervalls aggregiert werden (z.B. Mittelwert).
 * Anschließend werden die Serien auf Basis der gemeinsamen Zeit-Buckets ausgerichtet.
 * <p>
 * Benötigt den Parameter 'intervalSeconds' im params-Map.
 */
public class ResamplingJoinStrategy implements TemporalJoinStrategy {

    @Override
    public AlignedData align(
            List<TimeSeriesResult> seriesListA, String metricA,
            List<TimeSeriesResult> seriesListB, String metricB,
            Map<String, Object> params) {

        // 1. Parameter für das Intervall extrahieren
        long intervalSeconds = ((Number) params.getOrDefault("intervalSeconds", 60L)).longValue();
        if (intervalSeconds <= 0) {
            throw new IllegalArgumentException("Parameter 'intervalSeconds' muss positiv sein.");
        }

        // 2. Rohdaten konvertieren und aggregieren
        Map<ZonedDateTime, Double> resampledA = aggregate(seriesListA, metricA, intervalSeconds);
        Map<ZonedDateTime, Double> resampledB = aggregate(seriesListB, metricB, intervalSeconds);

        if (resampledA.isEmpty() || resampledB.isEmpty()) {
            return AlignedData.EMPTY;
        }

        // 3. Gemeinsame Zeitstempel finden und sortieren
        List<ZonedDateTime> commonTimestamps = new ArrayList<>(resampledA.keySet());
        commonTimestamps.retainAll(resampledB.keySet());
        Collections.sort(commonTimestamps);

        if (commonTimestamps.isEmpty()) {
            return AlignedData.EMPTY;
        }

        // 4. Ausgerichtete Wert-Arrays erstellen
        double[] valuesA = new double[commonTimestamps.size()];
        double[] valuesB = new double[commonTimestamps.size()];

        for (int i = 0; i < commonTimestamps.size(); i++) {
            ZonedDateTime t = commonTimestamps.get(i);
            valuesA[i] = resampledA.get(t);
            valuesB[i] = resampledB.get(t);
        }

        return new AlignedData(commonTimestamps, valuesA, valuesB);
    }

    /**
     * Private Hilfsmethode, die eine Liste von Zeitreihen in ein aggregiertes Raster umwandelt.
     */
    private Map<ZonedDateTime, Double> aggregate(List<TimeSeriesResult> seriesList, String metric, long intervalSeconds) {
        Map<ZonedDateTime, List<Double>> bucketMap = new HashMap<>();

        // Nutzen Sie den Konverter, um eine einfache Map zu erhalten
        Map<Long, Double> rawData = TimeSeriesConverter.convert(seriesList, metric);

        for (Map.Entry<Long, Double> entry : rawData.entrySet()) {
            ZonedDateTime ts = ZonedDateTime.ofInstant(Instant.ofEpochMilli(entry.getKey()), ZoneId.of("UTC"));
            long epochSecond = ts.toEpochSecond();
            long bucketStart = (epochSecond / intervalSeconds) * intervalSeconds;

            ZonedDateTime bucketKey = ZonedDateTime.ofInstant(Instant.ofEpochSecond(bucketStart), ts.getZone());

            bucketMap.computeIfAbsent(bucketKey, k -> new ArrayList<>()).add(entry.getValue());
        }

        // Berechne den Durchschnitt für jeden Bucket
        return bucketMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN)
                ));
    }
}
