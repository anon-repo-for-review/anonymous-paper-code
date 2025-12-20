package time_relations;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Within_by_Object {
    @Context
    public GraphDatabaseService db;


    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }



    @Procedure(name = "graphobs.time_search.within_by_object", mode = Mode.READ)
    @Description("Gibt Knoten zurück, deren Zeiträume vollständig im Zeitbereich des übergebenen time_series oder time_period Knotens liegen")
    public Stream<NodeResult> filterNodesContainedByTimeNode(
            @Name("timeNode") Node timeNode,
            @Name(value = "nodeElementIds", defaultValue = "[]") List<String> nodeElementIds) {

        if (!timeNode.hasProperty("start") || !timeNode.hasProperty("end")) {
            throw new IllegalArgumentException("Der gegebene Knoten muss 'start' und 'end' Eigenschaften besitzen.");
        }

        ZonedDateTime startDateTime = parseToZonedDateTime(timeNode.getProperty("start"));
        ZonedDateTime endDateTime = parseToZonedDateTime(timeNode.getProperty("end"));

        Map<String, Object> params;
        String cypherQuery;

        if (nodeElementIds == null || nodeElementIds.isEmpty()) {
            cypherQuery = """
            MATCH (n:time_series)
            WHERE n.start >= $start AND n.end <= $end
            RETURN n
            UNION
            MATCH (n:time_period)
            WHERE n.start >= $start AND n.end <= $end
            RETURN n
            UNION
            MATCH (n:event)
            WHERE n.time >= $start AND n.time <= $end
            RETURN n
            """;

            params = Map.of("start", startDateTime, "end", endDateTime);
        } else {
            cypherQuery = """
            MATCH (n:time_series)
            WHERE elementId(n) IN $nodeElementIds
              AND n.start >= $start AND n.end <= $end
            RETURN n
            UNION
            MATCH (n:time_period)
            WHERE elementId(n) IN $nodeElementIds
              AND n.start >= $start AND n.end <= $end
            RETURN n
            UNION
            MATCH (n:event)
            WHERE elementId(n) IN $nodeElementIds
              AND n.time >= $start AND n.time <= $end
            RETURN n
            """;

            params = Map.of("start", startDateTime, "end", endDateTime, "nodeElementIds", nodeElementIds);
        }

        Transaction tx = db.beginTx();
        Result result = tx.execute(cypherQuery, params);

        return result.stream()
                .map(row -> new NodeResult((Node) row.get("n")))
                .onClose(tx::close);
    }





    private ZonedDateTime parseToZonedDateTime(Object value) {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        } else if (value instanceof String) {
            return ZonedDateTime.parse((String) value, DateTimeFormatter.ISO_DATE_TIME);
        } else if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault());
        } else {
            throw new IllegalArgumentException("Unsupported date format: " + value);
        }
    }
}
