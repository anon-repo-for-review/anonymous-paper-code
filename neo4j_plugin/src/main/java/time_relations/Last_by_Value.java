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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Last_by_Value {

    @Context
    public GraphDatabaseService db;


    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }

    @Procedure(name = "graphobs.time_search.last_by_value", mode = Mode.READ)
    @Description("Gibt das vorherige Objekt zurück, das vor einem gegebenen Zeitpunkt oder Zeitraum liegt – optional eingeschränkt auf bestimmte elementId-Liste.")
    public Stream<Last_by_Object.NodeResult> previousObjectBeforeTime(
            @Name("start") String start,
            @Name(value = "end", defaultValue = "") String end,
            @Name(value = "nodeElementIds", defaultValue = "[]") List<String> nodeElementIds
    ) {
        ZonedDateTime referenceTime;
        if (end != null && !end.isBlank()) {
            referenceTime = parseToZonedDateTime(end);
        } else if (start != null && !start.isBlank()) {
            referenceTime = parseToZonedDateTime(start);
        } else {
            throw new IllegalArgumentException("Mindestens ein Zeitwert (start oder end) muss angegeben werden.");
        }

        Map<String, Object> params = new HashMap<>();
        params.put("refTime", referenceTime);
        String idFilter = "";

        if (nodeElementIds != null && !nodeElementIds.isEmpty()) {
            params.put("nodeElementIds", nodeElementIds);
            idFilter = "AND elementId(n) IN $nodeElementIds";
        }

        String cypherQuery = String.format("""
        CALL {
            MATCH (n:event)
            WHERE n.time < $refTime %s
            RETURN n, n.time AS t
            UNION
            MATCH (n:time_period)
            WHERE n.end < $refTime %s
            RETURN n, n.end AS t
            UNION
            MATCH (n:time_series)
            WHERE n.end < $refTime %s
            RETURN n, n.end AS t
        }
        RETURN n ORDER BY t DESC LIMIT 1
        """, idFilter, idFilter, idFilter);

        Transaction tx = db.beginTx();
        Result result = tx.execute(cypherQuery, params);

        return result.stream()
                .map(row -> new Last_by_Object.NodeResult((Node) row.get("n")))
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
