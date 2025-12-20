package core;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.logging.internal.LogService;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

public class MyTransactionEventListener extends TransactionEventListenerAdapter<Void> {

    private final GraphDatabaseService db;
    private final LogService logsvc;

    public MyTransactionEventListener(GraphDatabaseService graphDatabaseService, LogService logsvc) {
        db = graphDatabaseService;
        this.logsvc = logsvc;
    }

    @Override
    public Void beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) throws Exception {
        // Für alle neu erstellten Knoten
        data.createdNodes().forEach(node -> {
            if (node.hasLabel(Label.label("time_series"))) {

                String[] timestamps = (String[]) node.getProperty("timestamps", new String[0]);

                if (timestamps.length > 0) {

                    ZonedDateTime[] zonedTimestamps = Arrays.stream(timestamps)
                            .map(this::parseToZonedDateTime)
                            .toArray(ZonedDateTime[]::new);

                    // Optional: original Liste ersetzen mit formatierten Strings
                    String[] formatted = Arrays.stream(zonedTimestamps)
                            .map(zdt -> zdt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME))
                            .toArray(String[]::new);

                    node.setProperty("timestamps", formatted);  // <--- wichtig!

                    // Zusätzlich: start & end
                    ZonedDateTime start = Arrays.stream(zonedTimestamps).min(ZonedDateTime::compareTo).orElse(null);
                    ZonedDateTime end = Arrays.stream(zonedTimestamps).max(ZonedDateTime::compareTo).orElse(null);

                    if (start != null && end != null) {
                        node.setProperty("start", start.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
                        node.setProperty("end", end.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
                    }
                }
            }
            else if (node.hasLabel(Label.label("time_list"))) {
                // Überprüfe, ob der Knoten eine Zeitreihe enthält
                if (node.hasProperty("time_series_data")) {
                    long[] timeSeriesData = (long[]) node.getProperty("time_series_data", new int[0]);

                    // Finde den frühesten und spätesten Wert
                    long start = Arrays.stream(timeSeriesData).min().orElse(Integer.MAX_VALUE);
                    long end = Arrays.stream(timeSeriesData).max().orElse(Integer.MIN_VALUE);

                    // Setze die Properties "start" und "end"
                    node.setProperty("start", start);
                    node.setProperty("end", end);
                } else {
                    // Wenn keine Zeitreihe vorhanden ist, kannst du hier eine Fehlerbehandlung einbauen
                    throw new IllegalArgumentException("time_series Knoten muss eine Zeitreihe enthalten.");
                }
            }

            // Prüfe, ob der Knoten das Label "time_period" hat
            if (node.hasLabel(Label.label("time_period"))) {
                // Falls die "start" und "end" Werte nicht gesetzt sind, setze sie auf aktuelle Zeitstempel oder Default-Werte
                if (!node.hasProperty("start")) {
                    node.setProperty("start", System.currentTimeMillis());  // Beispiel: aktueller Zeitstempel
                } else {
                    node.setProperty("start", parseToZonedDateTime(node.getProperty("start")));//, DateTimeFormatter.ISO_DATE_TIME));
                }

                if (!node.hasProperty("end")) {
                    node.setProperty("end", System.currentTimeMillis());  // Beispiel: aktueller Zeitstempel
                } else {
                    node.setProperty("end", parseToZonedDateTime(node.getProperty("end")));//, DateTimeFormatter.ISO_DATE_TIME));
                }
            }

            // Prüfe, ob der Knoten das Label "event" hat
            if (node.hasLabel(Label.label("event"))) {
                // Überprüfe, ob ein "time" Wert angegeben ist
                if (!node.hasProperty("time")) {
                    throw new IllegalArgumentException("event Knoten muss einen 'time'-Wert enthalten.");
                } else {
                    node.setProperty("time", parseToZonedDateTime(node.getProperty("time")));//, DateTimeFormatter.ISO_DATE_TIME));
                }
            }
        });

        return null;
    }




    private ZonedDateTime parseToZonedDateTime(Object value) {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        } else if (value instanceof String) {
            String str = (String) value;
            try {
                return ZonedDateTime.parse(str, DateTimeFormatter.ISO_ZONED_DATE_TIME);
            } catch (DateTimeParseException e1) {
                try {
                    return LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                            .atZone(ZoneId.systemDefault());
                } catch (DateTimeParseException e2) {
                    throw new IllegalArgumentException("Unparseable date string: " + str, e2);
                }
            }
        } else if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault());
        } else {
            throw new IllegalArgumentException("Unsupported date format: " + value);
        }
    }


    @Override
    public void afterCommit(TransactionData data, Void state, GraphDatabaseService databaseService) {
        logsvc
                .getUserLog(MyTransactionEventListener.class)
                .info("Logging after commit on transaction with ID %s for database %s", data.getTransactionId(), db.databaseName());
    }
}
