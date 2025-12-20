package time_relations;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.time.Duration;
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



public class Range_by_Values {

    @Context
    public GraphDatabaseService db;


    public static class NodeResult {
        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }

    @Procedure(name = "graphobs.time_search.range_by_values", mode = Mode.READ)
    @Description("Gibt Knoten zurück, die im Verhältnis zu einem Zeitpunkt +/–/± einem Zeitbereich liegen.")
    public Stream<NodeResult> filterObjectsByTimeWindow(
            @Name("centerTime") String centerTime,
            @Name("range") String rangeIso8601,  // z.B. "PT30M", "PT1H"
            @Name("mode") String mode,           // "plus", "minus", "around"
            @Name(value = "nodeElementIds", defaultValue = "[]") List<String> nodeElementIds
    ) {
        ZonedDateTime center = parseToZonedDateTime(centerTime);//, DateTimeFormatter.ISO_DATE_TIME);

        ZonedDateTime start = center;
        ZonedDateTime end = center;
        Duration duration = Duration.parse(rangeIso8601);

        switch (mode.toLowerCase()) {
            case "plus" -> end = end.plus(duration);
            case "minus" -> start = start.minus(duration);  // verschiebt den Anfang nach vorne (kürzt)
            case "around" -> {
                start = start.minus(duration);
                end = end.plus(duration);
            }
            default -> throw new IllegalArgumentException("Modus muss 'plus', 'minus' oder 'around' sein.");
        }

        String filterClause;
        filterClause = """
            (
              (n:time_series OR n:time_period) AND n.start > $start AND n.end < $end
              OR (n:event AND n.time > $start AND n.time < $end)
            )
           """;

        Map<String, Object> params = new HashMap<>();
        params.put("start", start);
        params.put("end", end);


        String idFilter = (nodeElementIds != null && !nodeElementIds.isEmpty()) ? "AND elementId(n) IN $nodeElementIds" : "";

        if (!idFilter.isEmpty()) {
            params.put("nodeElementIds", nodeElementIds);
        }

        String query = String.format("""
        MATCH (n)
        WHERE (%s) %s
        RETURN n
        """, filterClause, idFilter);

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