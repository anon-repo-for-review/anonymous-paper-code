package topological_aggregation;


import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult; // Wichtig: Import Ihrer Standard-Ergebnisklasse
import util.TimeSeriesUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TopologicalAggregation {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    // HINWEIS: Die Klasse AggregationOutput wurde entfernt,
    // wir nutzen jetzt direkt TimeSeriesResult.

    @Procedure(name = "graphobs.aggregation.aggregate_nodes", mode = Mode.READ)
    @Description("Aggregiert eine Metrik über eine beliebige Liste von Knoten (Topologie). " +
            "Gibt ein TimeSeriesResult zurück. Parameter 'aggregation' kann 'sum' (default) oder 'avg' sein.")
    public Stream<TimeSeriesResult> aggregateNodes(
            @Name("nodes") List<Node> nodes,
            @Name("metric") String metric,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (nodes == null || nodes.isEmpty()) {
            return Stream.empty();
        }
        if (metric == null || metric.isBlank()) {
            log.warn("aggregate_nodes: Keine Metrik angegeben.");
            return Stream.empty();
        }

        try {
            // 1. Aggregations-Typ bestimmen (Default: SUM)
            String aggParam = (String) params.getOrDefault("top_aggregation", "sum");
            MultiSeriesAggregator.AggregationType type;
            if (aggParam.equalsIgnoreCase("avg") || aggParam.equalsIgnoreCase("average") || aggParam.equalsIgnoreCase("mean")) {
                type = MultiSeriesAggregator.AggregationType.AVERAGE;
            } else {
                type = MultiSeriesAggregator.AggregationType.SUM;
            }

            // 2. Zeitreihen für alle Knoten in der Liste laden
            List<TimeSeriesResult> allSeries = new ArrayList<>();
            for (Node node : nodes) {
                if (node == null) continue;
                TimeSeriesUtil.getFilteredTimeSeries(node, metric, params, db, log)
                        .forEach(allSeries::add);
            }

            if (allSeries.isEmpty()) {
                log.info("aggregate_nodes: Keine Zeitreihen für die angegebenen Knoten gefunden.");
                return Stream.empty();
            }

            for (int i=0; i<allSeries.size(); i++) {
                log.info("ts: " + i + ": " + allSeries.get(i).values.toString());
            }

            // 3. Aggregation durchführen
            // Der MultiSeriesAggregator liefert bereits das gewünschte TimeSeriesResult-Format
            TimeSeriesResult result = MultiSeriesAggregator.aggregate(allSeries, metric, type);

            log.info("result ts: " + result.values.toString());

            if (result.timestamps == null || result.timestamps.isEmpty()) {
                return Stream.empty();
            }

            // 4. TimeSeriesResult direkt zurückgeben
            return Stream.of(result);

        } catch (Exception e) {
            log.error("Fehler in timegraph.aggregation.aggregate_nodes: " + e.getMessage(), e);
            return Stream.empty();
        }
    }
}