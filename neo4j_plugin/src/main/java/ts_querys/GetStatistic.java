package ts_querys;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.StatisticResult;
import result_classes.TimeSeriesResult;
import util.StatisticUtil;
import util.TimeSeriesUtil;
import util.TimeWindow;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetStatistic {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    // --- Öffentliche Prozeduren ---

    @Procedure(name = "graphobs.data.get_statistic_from_node", mode = Mode.READ)
    @Description("Get statistics (min, max, mean, median, stddev, sum, count, percentile) for a time series from a given node.")
    public Stream<StatisticResult> getStatisticFromNode(@Name("node") Node node,
                                                        @Name("tsName") String tsName,
                                                        @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {
        if (node == null) {
            log.warn("Start node was null.");
            return Stream.empty();
        }
        return processStatisticRequest(node, tsName, params);
    }

    @Procedure(name = "graphobs.data.get_statistic_from_pod", mode = Mode.READ)
    @Description("Finds a Pod by its 'service' property and gets statistics for a connected time series.")
    public Stream<StatisticResult> getStatisticFromPod(@Name("podServiceName") String podServiceName,
                                                       @Name("tsName") String tsName,
                                                       @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {
        try (Transaction tx = db.beginTx()) {
            Node startNode = tx.findNode(Label.label("Pod"), "name", podServiceName);
            if (startNode == null) {
                log.warn("Pod with service '%s' not found.", podServiceName);
                return Stream.empty();
            }
            return processStatisticRequest(startNode, tsName, params);
        }
    }

    // --- Zentrale Verarbeitungslogik ---

    private Stream<StatisticResult> processStatisticRequest(Node startNode, String tsName, Map<String, Object> params) {

        double percentileValue = params != null && params.containsKey("percentile")
                ? ((Number) params.get("percentile")).doubleValue()
                : 95.0;

        // Hole alle passenden TimeSeriesResult (lokal oder Prometheus) via TimeSeriesUtil
        Stream<TimeSeriesResult> seriesStream = TimeSeriesUtil.getFilteredTimeSeries(startNode, tsName, params, db, log);

        List<TimeSeriesResult> seriesList;
        try {
            seriesList = seriesStream.collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Error collecting time series results for '%s': %s", tsName, e.getMessage());
            return Stream.empty();
        }

        return seriesList.stream()
                .map(ts -> {
                    Map<String, Double> stats = StatisticUtil.calculateStatistics(ts.values, percentileValue);
                    return new StatisticResult(stats);
                });
    }
}
