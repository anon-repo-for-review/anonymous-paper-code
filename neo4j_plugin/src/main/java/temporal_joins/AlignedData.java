package temporal_joins;

import java.util.Objects;

/**
 * Eine unveränderliche Container-Klasse, die das Ergebnis eines Temporal Joins speichert.
 * <p>
 * Sie enthält zwei double-Arrays gleicher Länge, die die ausgerichteten Werte der
 * ursprünglichen Zeitreihen A und B repräsentieren. Jeder Index in den Arrays
 * entspricht demselben Zeitpunkt auf der neu erstellten, gemeinsamen Zeitachse.
 */
import java.time.ZonedDateTime; // Wichtig: ZonedDateTime importieren
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class AlignedData {

    // Leeres Ergebnisobjekt aktualisieren
    public static final AlignedData EMPTY = new AlignedData(Collections.emptyList(), new double[0], new double[0]);

    public final List<ZonedDateTime> timestamps; // NEU
    public final double[] valuesA;
    public final double[] valuesB;

    /**
     * Erstellt ein neues AlignedData-Objekt mit Zeitstempeln.
     * @param timestamps Die gemeinsame Zeitachse.
     * @param valuesA Die ausgerichteten Werte der Zeitreihe A.
     * @param valuesB Die ausgerichteten Werte der Zeitreihe B.
     */
    public AlignedData(List<ZonedDateTime> timestamps, double[] valuesA, double[] valuesB) {
        this.timestamps = Objects.requireNonNull(timestamps, "timestamps dürfen nicht null sein.");
        this.valuesA = Objects.requireNonNull(valuesA, "valuesA darf nicht null sein.");
        this.valuesB = Objects.requireNonNull(valuesB, "valuesB darf nicht null sein.");

        if (valuesA.length != valuesB.length || (valuesA.length != timestamps.size() && !timestamps.isEmpty())) {
            throw new IllegalArgumentException("Timestamps und beide Werte-Arrays müssen die gleiche Länge haben.");
        }
    }

    /**
     * Konstruktor für Fälle, in denen nur die Werte benötigt werden (rückwärtskompatibel).
     */
    public AlignedData(double[] valuesA, double[] valuesB) {
        this(Collections.emptyList(), valuesA, valuesB);
    }

    public int size() {
        return valuesA.length;
    }

    public boolean isEmpty() {
        return valuesA.length == 0;
    }
}