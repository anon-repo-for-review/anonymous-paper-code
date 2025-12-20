package time_series_analysis;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.*;
import result_classes.TimePointsResult;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//import static util.TimeSeriesUtil.getValueProperties;
import static util.TimeSeriesUtil.parseToZonedDateTime;

public class ChangepointDetection_new {

    @Context
    public GraphDatabaseService db;

    // DataPoint-Klasse bleibt unverändert, sie wird intern benötigt
    private static class DataPoint implements Comparable<DataPoint> {
        final ZonedDateTime timestamp;
        final double value;

        DataPoint(ZonedDateTime timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        @Override
        public int compareTo(DataPoint other) {
            return this.timestamp.compareTo(other.timestamp);
        }
    }

    @Procedure(name = "graphobs.analysis.pelt", mode = Mode.READ)
    @Description("Wendet den PELT-Algorithmus auf alle Zeitreihen-Properties eines Knotens an und gibt die Changepoints für jede Property zurück.")
    // GEÄNDERTER RÜCKGABETYP und ENTFERNTER PARAMETER
    public Stream<TimePointsResult> detectChangepointsPELT(
            //@Name("tsNode") Node tsNode,
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> valueProperties,
            @Name(value = "penalty", defaultValue = "0.0") Double penalty) {

        // --- 1. Master-Zeitstempel und alle passenden Wert-Properties auslesen ---
        /*if (!tsNode.hasProperty("timestamps")) {
            throw new IllegalArgumentException("Knoten " + tsNode.getElementId() + " hat keine 'timestamps'-Property.");
        }
        Object[] timestampsRaw = (Object[]) tsNode.getProperty("timestamps");
        int n = timestampsRaw.length;

        // Finde alle Properties, die numerische Arrays der gleichen Länge sind
        Map<String, Object[]> valueProperties = getValueProperties(tsNode, n);

        if (valueProperties.isEmpty()) {
            return Stream.of(new TimePointsResult(Collections.emptyMap()));
        }*/

        // Map zum Sammeln aller Ergebnisse
        Map<String, List<String>> finalResults = new HashMap<>();
        int n = timestamps.size();
        final double beta = (penalty == null) ? Math.log(n) : penalty;

        // --- 2. Iteriere über jede gefundene Zeitreihen-Property ---
        for (Map.Entry<String, List<Double>> propertyEntry : valueProperties.entrySet()) {
            String propertyName = propertyEntry.getKey();
            List<Double> rawValues = propertyEntry.getValue();

            // --- 2a. Daten für DIESE Property parsen und sortieren ---
            List<DataPoint> sortedData = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                ZonedDateTime ts = parseToZonedDateTime(timestamps.get(i));
                // Wir müssen das Object aus dem Array in ein double konvertieren
                double val = ((Number) rawValues.get(i)).doubleValue();
                sortedData.add(new DataPoint(ts, val));
            }
            Collections.sort(sortedData); // Essentiell für Zeitreihen-Algorithmen*/

            if (sortedData.size() < 2) {
                finalResults.put(propertyName, Collections.emptyList());
                continue; // Nächste Property
            }

            double[] values = sortedData.stream().mapToDouble(dp -> dp.value).toArray();

            // --- 2b. PELT-Algorithmus (Logik ist unverändert) ---
            double[] cumsum = new double[n + 1];
            double[] cumsum_sq = new double[n + 1];
            for (int i = 0; i < n; i++) {
                cumsum[i + 1] = cumsum[i] + values[i];
                cumsum_sq[i + 1] = cumsum_sq[i] + values[i] * values[i];
            }

            double[] F = new double[n + 1];
            int[] lastChangepoints = new int[n + 1];
            F[0] = 0.0;//-beta;

            List<Integer> R = new ArrayList<>();
            R.add(0);

            for (int t = 1; t <= n; t++) {
                double minCost = Double.POSITIVE_INFINITY;
                int bestTau = 0;

                for (int tau : R) {
                    double cost = getSegmentCost(tau, t, cumsum, cumsum_sq) + F[tau] + beta;
                    if (cost < minCost) {
                        minCost = cost;
                        bestTau = tau;
                    }
                }
                F[t] = minCost;
                lastChangepoints[t] = bestTau;

                List<Integer> R_new = new ArrayList<>();
                for (int tau : R) {
                    if (getSegmentCost(tau, t, cumsum, cumsum_sq) + F[tau] + beta <= F[t]) { // +beta
                        R_new.add(tau);
                    }
                }
                R_new.add(t);
                R = R_new;
            }

            // --- 2c. Backtracking (unverändert) ---
            List<Integer> changepointIndices = new ArrayList<>();
            int cp_index = lastChangepoints[n];
            while (cp_index > 0) {
                changepointIndices.add(cp_index);
                cp_index = lastChangepoints[cp_index];
            }
            Collections.reverse(changepointIndices);

            // --- 2d. Ergebnisse für DIESE Property umwandeln und speichern ---
            List<String> changepointTimestamps = changepointIndices.stream()
                    .map(index -> sortedData.get(index).timestamp)
                    .map(ts -> ts.format(DateTimeFormatter.ISO_DATE_TIME)) // Konvertiere zu String
                    .collect(Collectors.toList());

            finalResults.put(propertyName, changepointTimestamps);
        }

        // --- 3. Einzelnes Ergebnisobjekt mit allen gesammelten Daten erstellen und zurückgeben ---
        return Stream.of(new TimePointsResult(finalResults));
    }

    /**
     * Berechnet die Kosten für ein Segment von Index 'start' (inklusiv) bis 'end' (exklusiv).
     * Verwendet kumulative Summen für eine O(1) Berechnung.
     */
    private double getSegmentCost(int start, int end, double[] cumsum, double[] cumsum_sq) {
        double sum_y = cumsum[end] - cumsum[start];
        double sum_y_sq = cumsum_sq[end] - cumsum_sq[start];
        int n_segment = end - start;
        // Kosten = Summe der quadrierten Fehler vom Mittelwert
        return Math.max(0.0, sum_y_sq - (sum_y * sum_y) / n_segment); //return sum_y_sq - (sum_y * sum_y) / n_segment;
    }
}
