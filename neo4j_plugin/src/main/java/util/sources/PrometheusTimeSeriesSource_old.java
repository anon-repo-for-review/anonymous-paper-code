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

/*public class PrometheusTimeSeriesSource_old implements TimeSeriesSource {

    private final String defaultStep;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    public PrometheusTimeSeriesSource_old() { this("60s"); }
    public PrometheusTimeSeriesSource_old(String defaultStep) { this.defaultStep = defaultStep; }

    @Override
    public Optional<TimeSeriesResult> fetch(Node sourceNode, String relName, String tsName, TimeWindow window, Log log, Map<String, Object> params) {
        if (sourceNode == null) return Optional.empty();
        if (!sourceNode.hasLabel(Label.label("Prometheus"))) return Optional.empty();
        if (!sourceNode.hasProperty("names")) return Optional.empty();

        String resolution = params.getOrDefault("resolution", defaultStep).toString();

        Object[] names = TimeSeriesUtil.convertToObjectArray(sourceNode.getProperty("names", new Object[0]));
        List<String> nameList = Arrays.stream(names).map(Object::toString).collect(Collectors.toList());
        if (!nameList.contains(tsName)) return Optional.empty();

        String url = (String) sourceNode.getProperty("url");
        if (url == null) {
            log.warn("Prometheus node lacks 'url' property.");
            return Optional.empty();
        }

        //String url = "http://localhost:9090/";
        //relName = "socialnetwork-compose-post-service-1";

        return fetchFromPrometheus(url, relName, tsName, window.startTime, window.endTime, resolution, log);
    }


    // Prometheus fetch Methode ist public, damit Implementierung sie nutzen kann
    public Optional<TimeSeriesResult> fetchFromPrometheus(String prometheusUrl, String relName, String tsName, long startTimeMillis, long endTimeMillis, String resolution, Log log) {
        try {
            //long startSeconds = startTimeMillis / 1000;
            //long endSeconds = endTimeMillis / 1000;

            final String finalResolution = (resolution != null && !resolution.trim().isEmpty()) ? resolution : "60s";

            if (startTimeMillis == 0 && endTimeMillis == Long.MAX_VALUE) {
                long nowMillis = System.currentTimeMillis();
                endTimeMillis = nowMillis;
                // 10 Stunden in Millisekunden (10 * 60 Minuten * 60 Sekunden * 1000 Millisekunden)
                startTimeMillis = nowMillis - (10 * 60 * 60 * 1000);
                log.info("Start- und Endzeit sind auf den Standardwert (letzte 10 Stunden) gesetzt.");
            }

            long startSeconds = startTimeMillis / 1000;
            long endSeconds = endTimeMillis / 1000;

            /*String encodedQuery = URLEncoder.encode(tsName, StandardCharsets.UTF_8);
            String queryUrl = String.format("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%s",
                    prometheusUrl, encodedQuery, startSeconds, endSeconds, step);*/

            /*String baseUrl = prometheusUrl.endsWith("/")
                    ? prometheusUrl.substring(0, prometheusUrl.length() - 1)
                    : prometheusUrl;


            String metricQuery = String.format("%s{name=\"%s\"}", tsName, relName);
            String encodedQuery = URLEncoder.encode(metricQuery, StandardCharsets.UTF_8);
            String queryUrl = String.format(
                    "%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%s",
                    baseUrl, encodedQuery, startSeconds, endSeconds, finalResolution
            );

            log.info("DEBUG tsName='%s', relName='%s', step='%s'", tsName, relName, finalResolution);
            log.info("DEBUG metricQuery='%s'", metricQuery);

            log.info("Fetching from Prometheus: %s", queryUrl);
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
                values.add(Double.parseDouble(pair.get(1).asText()));
            }

            Map<String, List<Double>> map = Map.of(tsName, values);
            return Optional.of(new TimeSeriesResult(timestamps, map));
        } catch (IOException | InterruptedException e) {
            log.error("Exception while fetching data from Prometheus for query '%s': %s", tsName, e.getMessage());
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }
}*/
