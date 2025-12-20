package topological_aggregation;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil; // Dein bestehendes Util

import java.util.*;
import java.util.stream.Stream;

public class ServiceAggregation {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Procedure(name = "graphobs.data.get_all_for_service", mode = Mode.READ)
    @Description("Aggregiert eine Metrik über alle Pods eines Services. rps/cpu -> Summe, latency/error -> Durchschnitt.")
    public Stream<TimeSeriesResult> aggregatePodMetric(
            @Name("node") Node startNode,
            @Name("metric") String metric,
            @Name(value = "params", defaultValue = "{}") Map<String,Object> params
    ) {
        if (startNode == null) return Stream.empty();

        try {
            // --- 1) Pods suchen ---
            List<Node> pods = new ArrayList<>();
            for (Relationship rel : startNode.getRelationships(Direction.BOTH)) {
                Node other = rel.getOtherNode(startNode);
                if (other.hasLabel(Label.label("Pod"))) {
                    pods.add(other);
                }
            }

            if (pods.isEmpty()) {
                log.warn("aggregatePodMetric: keine Pods für Node " + startNode.getElementId() + " gefunden.");
                return Stream.empty();
            }

            // --- 2) Zeitreihen laden ---
            List<TimeSeriesResult> seriesList = new ArrayList<>();
            for (Node pod : pods) {
                // Wir sammeln alle Ergebnisse in einer flachen Liste
                TimeSeriesUtil.getFilteredTimeSeries(pod, metric, params, db, log)
                        .forEach(seriesList::add);
            }

            if (seriesList.isEmpty()) {
                return Stream.empty();
            }

            // --- 3) Aggregation an den MultiSeriesAggregator delegieren ---
            switch (metric) {
                case "rps":
                case "cpu_total":
                    // SUMME
                    return Stream.of(
                            MultiSeriesAggregator.aggregate(seriesList, metric, MultiSeriesAggregator.AggregationType.SUM)
                    );

                case "latency_ms":
                case "error_rate_pct":
                    // DURCHSCHNITT
                    return Stream.of(
                            MultiSeriesAggregator.aggregate(seriesList, metric, MultiSeriesAggregator.AggregationType.AVERAGE)
                    );

                default:
                    // Keine Aggregation -> Alle einzeln zurückgeben (wie bisher)
                    return seriesList.stream();
            }

        } catch (Exception e) {
            log.error("Fehler in aggregatePodMetric: " + e.getMessage());
            return Stream.empty();
        }
    }
}
