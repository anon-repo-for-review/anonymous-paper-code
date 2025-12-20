package util.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import result_classes.TimeSeriesResult;
import util.TimeSeriesSource;
import util.TimeWindow;
import util.TimeSeriesUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class PrometheusTimeSeriesSource implements TimeSeriesSource {

    private final String defaultStep;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_RATE_WINDOW = "5m";

    private static final List<String> POSSIBLE_OPERATION_LABELS = List.of(
            "operation",    // Legacy Jaeger / Custom
            "span_name"     // OpenTelemetry Standard (spanmetrics)
    );

    // Liste aller Label-Keys, die wir nacheinander probieren wollen
    private static final List<String> POSSIBLE_IDENTIFIER_LABELS = List.of(
            "name",         // Legacy / Custom
            "pod",          // Kubernetes Standard
            "pod_name",     // cAdvisor / Kubelet
            "instance",     // Prometheus Target Standard
            "host",         // VM / Node Exporter
            "hostname",     // Oft in VM Umgebungen genutzt
            "container",    // Manchmal ist der Container-Name das Identifier-Merkmal
            "job"           // Selten als ID, aber manchmal nützlich
    );

    public PrometheusTimeSeriesSource() { this("60s"); }
    public PrometheusTimeSeriesSource(String defaultStep) { this.defaultStep = defaultStep; }

    @Override
    public Optional<TimeSeriesResult> fetch(Node sourceNode, Node startNode, String tsName, TimeWindow window, Log log, Map<String, Object> params) {
        if (sourceNode == null || startNode == null) return Optional.empty();
        if (!sourceNode.hasLabel(Label.label("Prometheus"))) return Optional.empty();

        String url = (String) sourceNode.getProperty("url", null);
        if (url == null) {
            log.warn("Prometheus node lacks 'url' property.");
            return Optional.empty();
        }

        String resolution = params.getOrDefault("resolution", defaultStep).toString();

        // -------------------------------------------------------------
        // WEICHE: Entscheiden ob "Alter Pfad" (Pod/Instance) oder "Neuer Pfad" (Service/Op)
        // -------------------------------------------------------------

        boolean isServiceOrOp = startNode.hasLabel(Label.label("Service")) || startNode.hasLabel(Label.label("Operation"));

        if (!isServiceOrOp) {
            // === PFAD A: EXISTING FUNCTIONALITY (Pod / Instance / VM) ===

            // 1. Check if tsName is allowed via 'names' property
            if (!sourceNode.hasProperty("names")) return Optional.empty();
            Object[] names = TimeSeriesUtil.convertToObjectArray(sourceNode.getProperty("names", new Object[0]));
            List<String> nameList = Arrays.stream(names).map(Object::toString).collect(Collectors.toList());
            if (!nameList.contains(tsName)) return Optional.empty();

            // 2. Build Query using Multi-Label Strategy
            String targetName = (String) startNode.getProperty("name", "");

            // Erzeugt: metric{name="xyz"} or metric{pod="xyz"} or metric{instance="xyz"} ...
            String metricQuery = buildMultiLabelQuery(tsName, targetName);

            // 3. Execute
            return executeRawQuery(url, metricQuery, window, resolution, tsName, log);

        } else {
            // === PFAD B: NEW FUNCTIONALITY (Jaeger/Performance) ===

            // 1. Build Complex Query
            String metricQuery = buildComplexPromQL(startNode, tsName, params);
            if (metricQuery == null) return Optional.empty();

            // 2. Execute
            return executeRawQuery(url, metricQuery, window, resolution, tsName, log);
        }
    }

    /**
     * Baut eine PromQL-Query, die prüft, ob der `targetName` in irgendeinem
     * der gängigen Identifier-Labels vorkommt.
     */
    private String buildMultiLabelQuery(String tsName, String targetName) {
        // Wir nutzen Java Streams um die "OR" Kette zu bauen
        return POSSIBLE_IDENTIFIER_LABELS.stream()
                .map(labelKey -> String.format("%s{%s=\"%s\"}", tsName, labelKey, targetName))
                .collect(Collectors.joining(" or "));
    }

    // --- NEUE LOGIK FÜR KOMPLEXE QUERIES ---

    private String buildComplexPromQL(Node startNode, String tsName, Map<String, Object> params) {
        // 1. Gemeinsame Labels sammeln (z.B. Service Name und externe Filter)
        Map<String, String> baseLabels = new HashMap<>();

        if (startNode.hasLabel(Label.label("Service")) || startNode.hasLabel(Label.label("Operation"))) {
            // Manche Setups nutzen "service", andere "service_name".
            // Hier gehen wir erst mal von dem Property-Wert des Nodes aus.
            String svcName = (String) startNode.getProperty("service",
                    startNode.getProperty("name", ""));

            // Standardmäßig heißt das Label in Prometheus oft "service_name" (Otel) oder "service"
            // Wir setzen hier hart "service_name", ggf. anpassbar machen.
            baseLabels.put("service_name", svcName);
        }

        // Filter aus Params hinzufügen
        if (params.containsKey("filters") && params.get("filters") instanceof Map) {
            Map<String, Object> filters = (Map<String, Object>) params.get("filters");
            for (Map.Entry<String, Object> entry : filters.entrySet()) {
                baseLabels.put(entry.getKey(), entry.getValue().toString());
            }
        }

        // 2. Unterscheidung: Ist es eine Operation?
        if (startNode.hasLabel(Label.label("Operation"))) {
            String opName = (String) startNode.getProperty("name", "");

            // Wir bauen für JEDEN möglichen Key (operation, span_name) eine eigene Query
            // und verknüpfen sie mit " or ".
            return POSSIBLE_OPERATION_LABELS.stream()
                    .map(labelKey -> {
                        Map<String, String> distinctLabels = new HashMap<>(baseLabels);
                        distinctLabels.put(labelKey, opName);
                        return buildSingleQueryExpression(tsName, distinctLabels, params);
                    })
                    .collect(Collectors.joining(" or "));

        } else {
            // Nur Service oder andere Nodes (keine Operation-Keys nötig)
            return buildSingleQueryExpression(tsName, baseLabels, params);
        }
    }

    /**
     * Erzeugt den eigentlichen PromQL-String (Rate, Histogram, etc.) für ein festes Set an Labels.
     */
    private String buildSingleQueryExpression(String tsName, Map<String, String> labelsMap, Map<String, Object> params) {
        // Labels zu String zusammenbauen: key="value", key2="value2"
        String labels = labelsMap.entrySet().stream()
                .map(e -> String.format("%s=\"%s\"", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "));

        String rateWindow = (String) params.getOrDefault("rate_window", DEFAULT_RATE_WINDOW);

        // Fall A: Histogram Quantile
        if (params.containsKey("percentile")) {
            double quantile = ((Number) params.get("percentile")).doubleValue();
            // WICHTIG: sum by (le) ist notwendig, aber oft will man auch by (service_name, ...) behalten?
            // Für reine Graphen reicht oft sum by (le).
            return String.format(
                    "histogram_quantile(%s, sum(rate(%s{%s}[%s])) by (le))",
                    String.valueOf(quantile), tsName, labels, rateWindow
            );
        }

        boolean forceRate = params.containsKey("rate") && (Boolean) params.get("rate");
        boolean isCounter = tsName.endsWith("_total") ||
                tsName.endsWith("_count") ||
                tsName.endsWith("_sum")   ||
                tsName.endsWith("_bucket");
        //boolean isCounter =false;

        // Fall B: Rate (für Counter)
        if (forceRate) {//|| isCounter
            return String.format("sum(rate(%s{%s}[%s]))", tsName, labels, rateWindow);
        } else if (isCounter){
            return String.format("sum(%s{%s})", tsName, labels);
        }

        // Fall C: Raw Gauge
        return labels.isEmpty() ? tsName : String.format("%s{%s}", tsName, labels);
    }

    // --- REFACTORED CORE EXECUTION ---

    private Optional<TimeSeriesResult> executeRawQuery(String prometheusUrl, String metricQuery, TimeWindow window, String resolution, String tsName, Log log) {
        try {
            long startTimeMillis = window.startTime;
            long endTimeMillis = window.endTime;

            final String finalResolution = (resolution != null && !resolution.trim().isEmpty()) ? resolution : "60s";

            if (startTimeMillis == 0 && endTimeMillis == Long.MAX_VALUE) {
                long nowMillis = System.currentTimeMillis();
                endTimeMillis = nowMillis;
                startTimeMillis = nowMillis - (10 * 60 * 60 * 1000);
                log.info("Start- und Endzeit sind auf den Standardwert (letzte 10 Stunden) gesetzt.");
            }

            long startSeconds = startTimeMillis / 1000;
            long endSeconds = endTimeMillis / 1000;

            String baseUrl = prometheusUrl.endsWith("/")
                    ? prometheusUrl.substring(0, prometheusUrl.length() - 1)
                    : prometheusUrl;

            // Wichtig: Query Encoding
            String encodedQuery = URLEncoder.encode(metricQuery, StandardCharsets.UTF_8);

            String queryUrl = String.format(
                    "%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%s",
                    baseUrl, encodedQuery, startSeconds, endSeconds, finalResolution
            );

            log.info("Fetching from Prometheus (TsName: %s): %s", tsName, queryUrl);

            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(queryUrl)).build();
            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("Failed to fetch from Prometheus. Status: %d, Body: %s", response.statusCode(), response.body());
                return Optional.empty();
            }

            JsonNode root = OBJECT_MAPPER.readTree(response.body());
            JsonNode results = root.path("data").path("result");

            if (!results.isArray() || results.size() == 0) {
                log.warn("Prometheus query for '%s' returned no results.", tsName);
                return Optional.empty();
            }

            JsonNode firstSeriesValues = results.get(0).path("values");

            List<String> timestamps = new ArrayList<>();
            List<Double> values = new ArrayList<>();

            for (JsonNode pair : firstSeriesValues) {
                long timestampEpoch = pair.get(0).asLong();
                timestamps.add(Instant.ofEpochSecond(timestampEpoch).toString());
                double val = pair.get(1).asDouble();
                values.add(val);
            }

            Map<String, List<Double>> map = Map.of(tsName, values);
            return Optional.of(new TimeSeriesResult(timestamps, map));

        } catch (IOException | InterruptedException e) {
            log.error("Exception while fetching data from Prometheus for query '%s': %s", tsName, e.getMessage());
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    // --- LEGACY SUPPORT METHODE ---
    public Optional<TimeSeriesResult> fetchFromPrometheus(String prometheusUrl, String relName, String tsName, long startTimeMillis, long endTimeMillis, String resolution, Log log) {
        // Nutzt jetzt auch die Multi-Label Logik
        String multiLabelQuery = buildMultiLabelQuery(tsName, relName);
        TimeWindow window = new TimeWindow(startTimeMillis, endTimeMillis);
        return executeRawQuery(prometheusUrl, multiLabelQuery, window, resolution, tsName, log);
    }
}