package temporal_joins;

import result_classes.TimeSeriesResult; // Annahme: TimeSeriesResult ist in diesem Package
import util.TimeSeriesUtil; // Annahme: Ihr Util ist hier

import java.time.ZonedDateTime;
import java.util.*;

/**
 * Eine Hilfsklasse zur Konvertierung von TimeSeriesResult-Objekten in ein
 * für Join-Algorithmen optimiertes Format.
 */
public final class TimeSeriesConverter {

    // Privater Konstruktor, um Instanziierung zu verhindern.
    private TimeSeriesConverter() {}

    /**
     * Konvertiert und kombiniert eine Liste von TimeSeriesResult-Objekten in eine einzige Map.
     * <p>
     * Diese Methode durchläuft alle Zeitreihen, wählt den spezifizierten Metrik-Wert aus,
     * parst die String-Timestamps zu Epochen-Millisekunden und erstellt eine flache
     * Map von Timestamp zu Wert. Bei doppelten Timestamps gewinnt der letzte Wert.
     *
     * @param seriesList Die Liste der zu konvertierenden TimeSeriesResult-Objekte.
     * @param preferredMetricKey Der Name der Metrik, deren Werte extrahiert werden sollen.
     * @return Eine Map, die Zeitstempel in Millisekunden auf Double-Werte abbildet.
     *         Gibt eine leere Map zurück, wenn keine gültigen Daten gefunden wurden.
     */
    public static Map<Long, Double> convert(List<TimeSeriesResult> seriesList, String preferredMetricKey) {
        Map<Long, Double> map = new HashMap<>();
        if (seriesList == null || preferredMetricKey == null) {
            return map;
        }

        for (TimeSeriesResult ts : seriesList) {
            if (ts == null || ts.timestamps == null || ts.values == null || ts.values.isEmpty()) {
                continue;
            }

            // Finde die korrekte Werteliste.
            List<Double> valuesList = ts.values.get(preferredMetricKey);
            // Fallback: Wenn der bevorzugte Key nicht existiert, nimm die erste verfügbare Liste.
            if (valuesList == null) {
                valuesList = ts.values.values().iterator().next();
            }

            if (valuesList == null) {
                continue;
            }

            for (int i = 0; i < ts.timestamps.size(); i++) {
                try {
                    // Stelle sicher, dass für den Zeitstempel auch ein Wert existiert.
                    if (i >= valuesList.size()) {
                        break; // Mehr Timestamps als Werte, Schleife sicher beenden.
                    }

                    String timestampStr = ts.timestamps.get(i);
                    Double value = valuesList.get(i);

                    // Ungültige Werte überspringen
                    if (timestampStr == null || value == null || value.isNaN() || value.isInfinite()) {
                        continue;
                    }

                    ZonedDateTime zdt = TimeSeriesUtil.parseToZonedDateTime(timestampStr);
                    long epochMillis = zdt.toInstant().toEpochMilli();

                    map.put(epochMillis, value);

                } catch (Exception ignored) {
                    // Ignoriere unparsierbare Zeitstempel oder Index-Probleme und fahre fort.
                }
            }
        }
        return map;
    }
}