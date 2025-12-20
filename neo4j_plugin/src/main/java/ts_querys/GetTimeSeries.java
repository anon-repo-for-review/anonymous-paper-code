package ts_querys;

import aggregation_functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import util.AggregationUtil;
import util.TimeSeriesUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;



import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class GetTimeSeries {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(name = "graphobs.data.get_time_series", mode = Mode.READ)
    @Description("Get a time series by name connected to a node, with optional filtering and aggregation.")
    public Stream<TimeSeriesResult> getTimeSeriesFromNode(
            @Name("node") Node node,
            @Name("tsName") String tsName,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (node == null) {
            log.warn("Start node was null.");
            return Stream.empty();
        }
        return processTimeSeriesRequest(node, tsName, params);
    }

    // ------------------------------------------------------------
    // Prozedur 2: Zugriff über Pod-Service-Name
    // ------------------------------------------------------------
    @Procedure(name = "graphobs.data.get_time_series_from_pod", mode = Mode.READ)
    @Description("Finds a Pod by its 'service' property and gets a time series connected to it.")
    public Stream<TimeSeriesResult> getTimeSeriesFromPod(
            @Name("podServiceName") String podServiceName,
            @Name("tsName") String tsName,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        try (Transaction tx = db.beginTx()) {
            Node startNode = tx.findNode(Label.label("Pod"), "name", podServiceName);
            if (startNode == null) {
                log.warn("Pod node with service name '%s' not found.", podServiceName);
                return Stream.empty();
            }
            return processTimeSeriesRequest(startNode, tsName, params);
        }
    }

    // ------------------------------------------------------------
    // Zentrale Logik: Filterung & Aggregation
    // ------------------------------------------------------------
    private Stream<TimeSeriesResult> processTimeSeriesRequest(Node startNode,
                                                              String tsName,
                                                              Map<String, Object> params) {
        // Aggregation & Period aus params extrahieren
        String aggregation = (String) params.getOrDefault("aggregation", "");
        long period = ((Number) params.getOrDefault("period", 0L)).longValue();

        String podName = (String) params.getOrDefault("pod", "");
        String operationName = (String) params.getOrDefault("operation", "");


        // 1. Rohdaten abrufen über Utils
        Stream<TimeSeriesResult> raw = TimeSeriesUtil.getFilteredTimeSeries(
                startNode,
                tsName,
                params,
                db,
                log
        );

        return raw;

        // 2. Falls Aggregation angegeben: anwenden
        //return raw.flatMap(result -> AggregationUtil.apply(aggregation, result, (int) period, log));
    }
}


/*
public class GetTimeSeries {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    // Statische Instanzen für Effizienz über alle Prozeduraufrufe hinweg
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Interne Record-Klasse zum sauberen Halten des Zeitfensters
    private record TimeWindow(long startTime, long endTime) {}

    // --- ÖFFENTLICHE PROZEDUREN ---

    @Procedure(name = "timegraph.data.get_time_series", mode = Mode.READ)
    @Description("Get a time series by name connected to a node, with optional filtering and aggregation.")
    public Stream<TimeSeriesResult> getTimeSeriesFromNode(@Name("node") Node node,
                                                          @Name("tsName") String tsName,
                                                          @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {
        if (node == null) {
            log.warn("Start node was null.");
            return Stream.empty();
        }
        // Die Verarbeitung wird an die zentrale Logik delegiert
        return processTimeSeriesRequest(node, tsName, params);
    }

    @Procedure(name = "timegraph.data.get_time_series_from_pod", mode = Mode.READ)
    @Description("Finds a Pod by its 'service' property and gets a time series connected to it.")
    public Stream<TimeSeriesResult> getTimeSeriesFromPod(@Name("podServiceName") String podServiceName,
                                                         @Name("tsName") String tsName,
                                                         @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {
        // Diese Prozedur sucht zuerst den Knoten und delegiert dann
        try (Transaction tx = db.beginTx()) {
            Node startNode = tx.findNode(Label.label("Pod"), "name", podServiceName);
            if (startNode == null) {
                log.warn("Start node 'Pod' with service name '%s' not found.", podServiceName);
                return Stream.empty();
            }
            return processTimeSeriesRequest(startNode, tsName, params);
        }
    }


    private Stream<TimeSeriesResult> processTimeSeriesRequest(Node startNode, String tsName, Map<String, Object> params) {
        TimeWindow window = extractTimeWindow(params);
        String aggregation = (String) params.getOrDefault("aggregation", "none");
        long period = ((Number) params.getOrDefault("window", 1)).longValue();


        List<TimeSeriesResult> results = new ArrayList<>();

        try {
            for (Relationship rel : startNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_TIME_SERIES"))) {
                Node tsNode = rel.getEndNode();

                // Fall 1: Lokaler `time_series`-Knoten wird gefunden
                if (tsNode.hasLabel(Label.label("time_series")) && tsName.equals(tsNode.getProperty("name", null))) {
                    results.addAll(handleLocalTimeSeriesNode(tsNode, window, aggregation, period).toList());
                }

                // Fall 2: `Prometheus`-Knoten wird gefunden
                if (tsNode.hasLabel(Label.label("Prometheus")) && tsNode.hasProperty("names")) {
                    if (Arrays.asList((Object[]) tsNode.getProperty("names", new Object[0])).contains(tsName)) {
                        String url = (String) tsNode.getProperty("url");
                        return fetchFromPrometheus(url, tsName, window.startTime, window.endTime, "60s");
                    }
                }
            }
            log.warn("No time series named '%s' found connected to node %d.", tsName, startNode.getElementId());
        } catch (Exception e) {
            log.error("An error occurred while getting time series '%s' for node %d.", e, tsName, startNode.getElementId());
        }
        return results.stream();
    }


    private Stream<TimeSeriesResult> handleLocalTimeSeriesNode(Node tsNode, TimeWindow window, String aggregation, long period) {
        log.info("Found local time series '%s'. Filtering by time window...", tsNode.getProperty("name", "N/A"));

        // 1. Filtern der Zeitreihe mit der Util-Methode
        TimeSeriesResult filteredResult = TimeSeriesUtil.getFilteredTimeSeries(tsNode, window.startTime, window.endTime);
        log.info("Filtered to %d data points.", filteredResult.timestamps.size());

        // 2. Wenn keine Aggregation gefordert, gefiltertes Ergebnis zurückgeben
        if ("none".equals(aggregation)) {
            return Stream.of(filteredResult);
        }

        // 3. Aggregationen auf den gefilterten Daten anwenden
        Map<String, List<Double>> values = filteredResult.values;
        List<String> timestamps = filteredResult.timestamps;

        switch (aggregation) {
            case "binned_average":
                return Stream.of(BinnedAverage.calc_binned_average(timestamps.toArray(new String[0]), values, period));
            case "cu_sum":
                return Stream.of(CumulativeSum_Integral.calc_cu_sum(timestamps.toArray(new String[0]), values));
                //values.replaceAll((key, val) -> CumulativeSum_Integral.calc_cum_sum(val));
                //return Stream.of(new TimeSeriesResult(timestamps, values));
            case "integral":
                return Stream.of(CumulativeSum_Integral.calc_integral(timestamps.toArray(new String[0]), values));
            case "difference":
                return Stream.of(Difference_Derivative.calc_difference(timestamps.toArray(new String[0]), values));
            case "derivative":
                return Stream.of(Difference_Derivative.calc_derivative(timestamps.toArray(new String[0]), values));
            case "linear_regression":
                return Stream.of(LinerRegression.calc_linear_regression(timestamps.toArray(new String[0]), values));
            case "moving_average":
                return Stream.of(MovingAverage.calc_moving_average(timestamps.toArray(new String[0]), values, period));
            default:
                log.warn("Unknown aggregation type: '%s'. Returning filtered data without aggregation.", aggregation);
                return Stream.of(filteredResult);
        }
    }

    // --- HILFSFUNKTIONEN FÜR ZEIT-PARSING ---


    private TimeWindow extractTimeWindow(Map<String, Object> params) {
        // Fall 1: Relative Zeitangabe (z.B. "letzte 5 Minuten" oder "nächste 1 Sekunde")
        if (params.containsKey("time") && params.containsKey("range")) {
            long baseTime = parseTimeParameterToMillis(params.get("time"));
            long rangeMillis = parseDuration(params.get("range"));

            // *** HIER IST DIE NEUE LOGIK ***
            if (rangeMillis >= 0) {
                // Positiver Bereich: schaut in die Zukunft.
                // startTime ist der baseTime, endTime ist in der Zukunft.
                log.info("Creating a forward-looking time window: from %s for %d ms", Instant.ofEpochMilli(baseTime), rangeMillis);
                return new TimeWindow(baseTime, baseTime + rangeMillis);
            } else {
                // Negativer Bereich: schaut in die Vergangenheit (bisheriges Verhalten).
                // startTime ist in der Vergangenheit, endTime ist der baseTime.
                log.info("Creating a backward-looking time window: from %s for %d ms", Instant.ofEpochMilli(baseTime), rangeMillis);
                return new TimeWindow(baseTime + rangeMillis, baseTime);
            }
        }

        // Fall 2: Absolute Zeitangabe (bleibt unverändert)
        Object startTimeRaw = params.get("startTime");
        Object endTimeRaw = params.get("endTime");
        long startTime = (startTimeRaw != null) ? parseTimeParameterToMillis(startTimeRaw) : 0L;
        long endTime = (endTimeRaw != null) ? parseTimeParameterToMillis(endTimeRaw) : Long.MAX_VALUE;

        return new TimeWindow(startTime, endTime);
    }


    private long parseTimeParameterToMillis(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Time parameter cannot be null.");
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        String sValue = value.toString().trim();
        if ("now".equalsIgnoreCase(sValue)) {
            return System.currentTimeMillis();
        }

        try {
            // Versucht, den String als ZonedDateTime zu parsen
            return ZonedDateTime.parse(sValue).toInstant().toEpochMilli();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Unsupported time format for '" + sValue + "'. Use ISO-8601, 'now', or epoch milliseconds.", e);
        }
    }



    private long parseDuration(Object value) {
        String s = value.toString().trim().toLowerCase();
        String numberPart = s;
        long multiplier = 1; // Default-Multiplier ist 1 (für Millisekunden)

        // WICHTIG: Die Reihenfolge muss vom längsten Suffix ("ms") zum kürzesten ("s") gehen,
        // um zu verhindern, dass "ms" fälschlicherweise als "s" erkannt wird.
        if (s.endsWith("ms")) {
            multiplier = 1L; // Explizit 1 für Millisekunden
            numberPart = s.substring(0, s.length() - 2);
        } else if (s.endsWith("s")) {
            multiplier = 1000L; // KORREKTER WERT: 1 Sekunde = 1000 Millisekunden
            numberPart = s.substring(0, s.length() - 1);
        } else if (s.endsWith("m")) {
            multiplier = 60_000L; // 1 Minute = 60 * 1000 Millisekunden
            numberPart = s.substring(0, s.length() - 1);
        } else if (s.endsWith("h")) {
            multiplier = 3_600_000L; // 1 Stunde = 60 * 60 * 1000 Millisekunden
            numberPart = s.substring(0, s.length() - 1);
        } else if (s.endsWith("d")) {
            multiplier = 86_400_000L; // 1 Tag = 24 * 60 * 60 * 1000 Millisekunden
            numberPart = s.substring(0, s.length() - 1);
        }

        try {
            long parsedNumber = Long.parseLong(numberPart.trim());
            long result = parsedNumber * multiplier;

            // --- DEBUG LOGGING ---
            // Dieses Logging hilft uns, das Problem zu verifizieren.
            log.info("parseDuration: input='%s', numberPart='%s', multiplier=%d, result=%d ms",
                    s, numberPart, multiplier, result);

            return result;

        } catch (NumberFormatException e) {
            log.error("Could not parse duration string: '%s'. Extracted number part was '%s'.", e, s, numberPart);
            throw new IllegalArgumentException("Invalid duration format: " + s);
        }
    }



    // --- HILFSFUNKTION FÜR PROMETHEUS ---


    private Stream<TimeSeriesResult> fetchFromPrometheus(String prometheusUrl, String tsName, long startTimeMillis, long endTimeMillis, String step) {
        // Konvertiere Millisekunden zu Sekunden für die Prometheus-API
        long startSeconds = startTimeMillis / 1000;
        long endSeconds = endTimeMillis / 1000;

        String encodedQuery = URLEncoder.encode(tsName, StandardCharsets.UTF_8);
        String queryUrl = String.format("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%s",
                prometheusUrl, encodedQuery, startSeconds, endSeconds, step);

        try {
            log.info("Fetching from Prometheus: %s", queryUrl);
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

            JsonNode firstSeriesValues = results.get(0).path("values");
            List<String> timestamps = new ArrayList<>();
            List<Double> values = new ArrayList<>();

            for (JsonNode pair : firstSeriesValues) {
                long timestampEpoch = pair.get(0).asLong();
                timestamps.add(Instant.ofEpochSecond(timestampEpoch).toString()); // Konvertiere zu ISO-String
                values.add(Double.parseDouble(pair.get(1).asText()));
            }

            return Stream.of(new TimeSeriesResult(timestamps, Map.of(tsName, values)));

        } catch (IOException | InterruptedException e) {
            log.error("Exception while fetching data from Prometheus for query '%s'", e, tsName);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return Stream.empty();
        }
    }
}*/