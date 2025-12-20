package time_relations;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class After_by_Object {

    @Context
    public GraphDatabaseService db;


    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }

    @Procedure(name = "graphobs.time_search.after_by_object", mode = Mode.READ)
    @Description("Gibt alle Objekte (event, time_period, time_series) zurück, die vollständig NACH dem gegebenen Zeit-Knoten liegen.")
    public Stream<NodeResult> filterObjectsAfterNode(
            @Name("referenceNode") Node referenceNode,
            @Name(value = "nodeElementIds", defaultValue = "[]") List<String> nodeElementIds
    ) {
        Set<String> labels = StreamSupport.stream(referenceNode.getLabels().spliterator(), false)
                .map(Label::name)
                .collect(Collectors.toSet());

        ZonedDateTime referenceTime;

        if (labels.contains("event")) {
            if (!referenceNode.hasProperty("time")) {
                throw new IllegalArgumentException("event-Knoten muss ein 'time'-Feld haben.");
            }
            referenceTime = parseToZonedDateTime(referenceNode.getProperty("time"));
        } else if (labels.contains("time_period") || labels.contains("time_series")) {
            if (!referenceNode.hasProperty("end")) {
                throw new IllegalArgumentException("Knoten mit Label 'time_period' oder 'time_series' muss ein 'end'-Feld haben.");
            }
            referenceTime = parseToZonedDateTime(referenceNode.getProperty("end"));
        } else {
            throw new IllegalArgumentException("Knoten muss eines der Labels 'event', 'time_period' oder 'time_series' haben.");
        }

        Map<String, Object> params = new HashMap<>();
        params.put("refTime", referenceTime);
        String idFilter = "";

        if (nodeElementIds != null && !nodeElementIds.isEmpty()) {
            params.put("nodeElementIds", nodeElementIds);
            idFilter = "AND elementId(n) IN $nodeElementIds";
        }

        String query = String.format("""
        CALL {
            MATCH (n:event)
            WHERE n.time > $refTime %s
            RETURN n
            UNION
            MATCH (n:time_period)
            WHERE n.start > $refTime %s
            RETURN n
            UNION
            MATCH (n:time_series)
            WHERE n.start > $refTime %s
            RETURN n
        }
        RETURN n
        """, idFilter, idFilter, idFilter);

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, params);
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