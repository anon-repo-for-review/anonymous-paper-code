package ts_querys;

import aggregation_functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class GetTimeSeries_old {

        /*@Context
        public GraphDatabaseService db;

        @Context
        public Log log;

        // Statische Instanzen für Effizienz
        private static final HttpClient client = HttpClient.newHttpClient();
        private static final ObjectMapper objectMapper = new ObjectMapper();



        @Procedure(name = "timegraph.data.get_time_series.decrapted", mode = Mode.READ)
        @Description("Get a time series by name connected to a node via HAS_TIME_SERIES")
        public Stream<TimeSeriesResult> getTimeSeriesFromNode(@Name("node") Node node,
                                                              @Name("tsName") String tsName,
                                                              //@Name(value = "aggregation", defaultValue = "none") String aggregation,
                                                              //@Name(value = "period", defaultValue = "1") long period,
                                                              @Name("params") Map<String,Object> params) {


            String aggregation = (String) params.getOrDefault("aggregation", "none");
            long period = ((Number) params.getOrDefault("window", 1)).longValue();

            // Zeitparameter verarbeiten
            Long startTime = (Long) params.get("startTime");
            Long endTime = (Long) params.get("endTime");

            if (params.containsKey("time") && params.containsKey("range")) {
                long baseTime = parseTime(params.get("time"));     // z. B. "now" → System.currentTimeMillis()
                long rangeMillis = parseDuration(params.get("range")); // z. B. "-2m" → -120_000
                endTime = baseTime;
                startTime = baseTime + rangeMillis;
            }

            // Defaults setzen, falls immer noch null
            if (startTime == null) startTime = 0L;
            if (endTime == null) endTime = Long.MAX_VALUE;




            try (Transaction tx = db.beginTx()) {

                // KORREKTUR: tx.findNode statt db.findNodes verwenden


                if (node == null) {
                    // Frühes Beenden, wenn der Startknoten nicht gefunden wird
                    log.warn("Start node was null");
                    return Stream.empty();
                }

                for (Relationship rel : node.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_TIME_SERIES"))) {
                    Node tsNode = rel.getEndNode();

                    // Fall 1: Direkter time_series-Knoten
                    if (tsNode.hasLabel(Label.label("time_series")) && tsName.equals(tsNode.getProperty("name", null))) {

                        String[] timestampStrs = (String[]) tsNode.getProperty("timestamps", new String[0]);
                        //log.info(timestampStrs.toString());


                        Map<String, List<Double>> values =  TimeSeriesUtil.getDoubleTs(tsNode, timestampStrs.length);



                        if (aggregation.equals("binned_average")){
                            TimeSeriesResult binned_Avg =  BinnedAverage.calc_binned_average(timestampStrs, values, period);
                            return Stream.of(binned_Avg);
                        } else if (aggregation.equals("cum_sum")){
                            // List<Double> values_new = CumulativeSum_Integral.calc_cum_sum(values.values().iterator().next());
                            values.replaceAll((key, val) -> CumulativeSum_Integral.calc_cum_sum(val));
                        } else if (aggregation.equals("integral")) {
                            TimeSeriesResult integrals = CumulativeSum_Integral.calc_integral(timestampStrs, values);
                            return Stream.of(integrals);
                        } else if (aggregation.equals("difference")) {
                            TimeSeriesResult diff = Difference_Derivative.calc_difference(timestampStrs, values);
                            return Stream.of(diff);
                        } else if (aggregation.equals("derivative")) {
                            TimeSeriesResult der = Difference_Derivative.calc_derivative(timestampStrs, values);
                            return Stream.of(der);
                        } else if (aggregation.equals("linear_regression")) {
                            TimeSeriesResult reg = LinerRegression.calc_linear_regression(timestampStrs, values);
                            return Stream.of(reg);
                        } else if (aggregation.equals("moving_average")) {
                            TimeSeriesResult mov_avg = MovingAverage.calc_moving_average(timestampStrs, values, period);
                            return Stream.of(mov_avg);
                        }


                        log.info("Found local time series '%s' with %d data points.", tsName, timestampStrs.length);
                        return Stream.of(new TimeSeriesResult(Arrays.stream(timestampStrs).toList(), values));
                    }

                    // Fall 2: Prometheus-Knoten mit mehreren Serien
                    if (tsNode.hasLabel(Label.label("Prometheus")) && tsNode.hasProperty("names")) {
                        Object[] names = (Object[]) tsNode.getProperty("names", new String[0]);

                        if (Arrays.asList(names).contains(tsName)) {
                            String url = (String) tsNode.getProperty("url");
                            log.info("Fetching time series '%s' from Prometheus URL: %s", tsName, url);
                            // Beispiel: Daten der letzten Stunde abfragen
                            Instant now = Instant.now();
                            //long endTime = now.getEpochSecond();
                            //long startTime = endTime - 3600; // 1 Stunde früher
                            return fetchFromPrometheus(url, tsName, startTime, endTime, "60s"); // 60s Intervall
                        }
                    }
                }
            } catch (Exception e) {
                log.error("An error occurred while getting time series for node '%s'", e);
            }

            return Stream.empty();
        }



        // Hilfsfunktion für "now", "2025-08-06T12:00:00Z", etc.
        private long parseTime(Object value) {
            if (value instanceof String && ((String) value).equalsIgnoreCase("now")) {
                return System.currentTimeMillis();
            }
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            // ggf. ISO-8601 parsing
            return Instant.parse(value.toString()).toEpochMilli();
        }

        // Hilfsfunktion für "-2m", "-1h", etc.
        private long parseDuration(Object value) {
            String s = value.toString().trim().toLowerCase();
            long multiplier = 1;
            if (s.endsWith("ms")) multiplier = 1;
            else if (s.endsWith("s")) multiplier = 1000;
            else if (s.endsWith("m")) multiplier = 60_000;
            else if (s.endsWith("h")) multiplier = 3_600_000;
            else if (s.endsWith("d")) multiplier = 86_400_000;
            s = s.replaceAll("[^0-9-]", "");
            return Long.parseLong(s) * multiplier;
        }






    @Procedure(name = "timegraph.data.get_time_series_from_pod.decrapted", mode = Mode.READ)
    @Description("Get a time series by name connected to a node via HAS_TIME_SERIES")
    public Stream<TimeSeriesResult> getTimeSeries(@Name("nodeName") String nodeName,
                                                  @Name("tsName") String tsName) {

        try (Transaction tx = db.beginTx()) {

            // KORREKTUR: tx.findNode statt db.findNodes verwenden
            Node startNode = tx.findNode(Label.label("Pod"), "service", nodeName);

            if (startNode == null) {
                // Frühes Beenden, wenn der Startknoten nicht gefunden wird
                log.warn("Start node with name '%s' not found.", nodeName);
                return Stream.empty();
            }

            for (Relationship rel : startNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_TIME_SERIES"))) {
                Node tsNode = rel.getEndNode();

                // Fall 1: Direkter time_series-Knoten
                if (tsNode.hasLabel(Label.label("time_series")) && tsName.equals(tsNode.getProperty("name", null))) {

                    String[] timestampStrs = (String[]) tsNode.getProperty("timestamps", new String[0]);
                    //log.info(timestampStrs.toString());


                    Map<String, List<Double>> values =  TimeSeriesUtil.getDoubleTs(tsNode, timestampStrs.length);

                    //double[] valueArray = (double[]) tsNode.getProperty("values", new double[0]);
                    //List<Double> values = Arrays.stream(valueArray).boxed().collect(Collectors.toList());



                    log.info("Found local time series '%s' with %d data points.", tsName, timestampStrs.length);
                    return Stream.of(new TimeSeriesResult(Arrays.stream(timestampStrs).toList(), values));
                }

                // Fall 2: Prometheus-Knoten mit mehreren Serien
                if (tsNode.hasLabel(Label.label("Prometheus")) && tsNode.hasProperty("names")) {
                    Object[] names = (Object[]) tsNode.getProperty("names", new String[0]);

                    if (Arrays.asList(names).contains(tsName)) {
                        String url = (String) tsNode.getProperty("url");
                        log.info("Fetching time series '%s' from Prometheus URL: %s", tsName, url);
                        // Beispiel: Daten der letzten Stunde abfragen
                        Instant now = Instant.now();
                        long endTime = now.getEpochSecond();
                        long startTime = endTime - 3600; // 1 Stunde früher
                        return fetchFromPrometheus(url, tsName, startTime, endTime, "60s"); // 60s Intervall
                    }
                }
            }
        } catch (Exception e) {
            log.error("An error occurred while getting time series for node '%s'", e, nodeName);
        }

        return Stream.empty();
    }

    private Stream<TimeSeriesResult> fetchFromPrometheus(String prometheusUrl, String tsName, long startTime, long endTime, String step) {

        String encodedQuery = URLEncoder.encode(tsName, StandardCharsets.UTF_8);
        String queryUrl = String.format("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%s",
                prometheusUrl, encodedQuery, startTime, endTime, step);

        try {
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(queryUrl)).build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("Failed to fetch from Prometheus. Status: %d, Body: %s", response.statusCode(), response.body());
                return Stream.empty();
            }

            JsonNode root = objectMapper.readTree(response.body());
            JsonNode results = root.path("data").path("result");

            if (!results.isArray() || results.isEmpty()) {
                log.warn("Prometheus query for '%s' returned no results.", tsName);
                return Stream.empty();
            }

            // Wir nehmen die erste Zeitreihe aus dem Ergebnis-Array.
            JsonNode firstSeriesValues = results.get(0).path("values");

            // Listen für die neuen Typen initialisieren
            List<String> timestamps = new ArrayList<>();
            List<Double> values = new ArrayList<>();

            for (JsonNode pair : firstSeriesValues) {
                // Prometheus liefert den Timestamp als Unix-Epoche (long)
                long timestampEpoch = pair.get(0).asLong();

                // Konvertiere den long-Timestamp in einen ISO-8601 String
                timestamps.add(Instant.ofEpochSecond(timestampEpoch).toString());

                // Der Wert ist der zweite Teil des Paares
                values.add(Double.parseDouble(pair.get(1).asText()));
            }

            // Erstelle das neue TimeSeriesResult-Objekt gemäß der neuen Struktur
            // Der übergebene tsName wird als Schlüssel für die Werte-Map verwendet.
            return Stream.of(new TimeSeriesResult(timestamps, Map.of(tsName, values)));

        } catch (IOException | InterruptedException e) {
            log.error("Exception while fetching data from Prometheus for query '%s'", e, tsName);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return Stream.empty();
        }
    }
*/
}