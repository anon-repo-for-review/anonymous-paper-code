package time_relations;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static util.TimeSeriesUtil.parseToZonedDateTime;


public class Overlap_by_Val {
    @Context
    public GraphDatabaseService db;


    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }

    @Procedure(name = "graphobs.time_search.overlap_by_value", mode = Mode.READ)
    @Description("Filters nodes based on the provided time range using indexes")
    public Stream<NodeResult> filterNodesByTime(@Name("start") String start, @Name("end") String end,
                                                @Name(value = "nodeElementIds", defaultValue = "[]") List<String> nodeElementIds) { //@Name(value = "nodes", defaultValue = "[]") List<Node> nodes
        ZonedDateTime startDateTime = parseToZonedDateTime(start);
        ZonedDateTime endDateTime = parseToZonedDateTime(end);

        Map<String, Object> params = Map.of("start", startDateTime, "end", endDateTime);
        String cypherQuery_II;

        String cypherQuery = """
            MATCH (n)
            WHERE (n:time_series OR n:time_period) AND n.start <= $end AND n.end >= $start
               OR (n:event AND n.time >= $start AND n.time <= $end)
            RETURN n
        """;

        if (nodeElementIds == null || nodeElementIds.isEmpty()){
            cypherQuery_II = """
                MATCH (n:time_series)
                WHERE n.start <= $end AND n.end >= $start
                RETURN n
                UNION
                MATCH (n:time_period)
                WHERE n.start <= $end AND n.end >= $start
                RETURN n
                UNION
                MATCH (n:event)
                WHERE n.time >= $start AND n.time <= $end
                RETURN n
                """;
        }
        else {
            String cypherQuery_collect = """
                UNWIND $nodes AS n
                MATCH (n:time_series)
                WHERE n.start <= $end AND n.end >= $start
                RETURN n
                UNION
                MATCH (n:time_period)
                WHERE n.start <= $end AND n.end >= $start
                RETURN n
                UNION
                MATCH (n:event)
                WHERE n.time >= $start AND n.time <= $end
                RETURN n
                """;


            cypherQuery_II = """
                MATCH (n:time_series)
                WHERE elementId(n) IN $nodeElementIds
                AND n.start <= $end AND n.end >= $start
                RETURN n
                UNION
                MATCH (n:time_period)
                WHERE elementId(n) IN $nodeElementIds
                AND n.start <= $end AND n.end >= $start
                RETURN n
                UNION
                MATCH (n:event)
                WHERE elementId(n) IN $nodeElementIds
                AND n.time >= $start AND n.time <= $end
                RETURN n
                """;
            params = Map.of("start", startDateTime, "end", endDateTime, "nodeElementIds", nodeElementIds);
            //params.put("nodes", nodes);
        }



        Transaction tx = db.beginTx();  // Transaktion manuell starten
        Result result = tx.execute(cypherQuery_II, params);

        return result.stream()
                .map(row -> new NodeResult((Node) row.get("n")))
                .onClose(tx::close);
    }

}
