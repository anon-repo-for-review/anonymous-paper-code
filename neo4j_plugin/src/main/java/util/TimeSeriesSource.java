package util;

import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import result_classes.TimeSeriesResult;

import java.util.Map;
import java.util.Optional;

/**
 * Interface für Zeitreihen-Quellen.
 * Implementierungen liefern optional eine TimeSeriesResult für einen sourceNode / tsName / window.
 */

import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import result_classes.TimeSeriesResult;
import java.util.Map;
import java.util.Optional;

public interface TimeSeriesSource {
    // Änderung: Parameter 'String relName' wird zu 'Node startNode'
    Optional<TimeSeriesResult> fetch(Node sourceNode, Node startNode, String tsName, TimeWindow window, Log log, Map<String, Object> params);
}
/*public interface TimeSeriesSource {
    Optional<TimeSeriesResult> fetch(Node sourceNode, String relName, String tsName, TimeWindow window, Log log, Map<String, Object> params);
}*/
