package temporal_joins;

import result_classes.TimeSeriesResult;

import java.util.List;
import java.util.Map;

/**
 * Interface für das Strategy Pattern zur Definition von Algorithmen für den Temporal Join.
 * <p>
 * Jede Implementierung dieses Interfaces stellt eine andere Methode dar, um zwei Zeitreihen,
 * die durch Maps von Timestamps (Long) zu Werten (Double) repräsentiert werden, auf eine
 * gemeinsame Zeitachse zu bringen.
 */
public interface TemporalJoinStrategy {

    /**
     * Richtet zwei Zeitreihen anhand einer spezifischen Strategie zeitlich aus.
     *
     * @param seriesListA Die Liste von Zeitreihen-Objekten für die erste Serie.
     * @param metricA     Der Name der Metrik, die aus den seriesListA extrahiert werden soll.
     * @param seriesListB Die Liste von Zeitreihen-Objekten für die zweite Serie.
     * @param metricB     Der Name der Metrik, die aus den seriesListB extrahiert werden soll.
     * @param params      Zusätzliche, für die Strategie spezifische Parameter.
     * @return Ein {@link AlignedData}-Objekt, das die ausgerichteten Wertereihen enthält.
     */
    AlignedData align(
            List<TimeSeriesResult> seriesListA, String metricA,
            List<TimeSeriesResult> seriesListB, String metricB,
            Map<String, Object> params
    );
}

