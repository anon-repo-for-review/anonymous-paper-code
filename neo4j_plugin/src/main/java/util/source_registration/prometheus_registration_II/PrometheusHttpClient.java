package util.source_registration.prometheus_registration_II;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.logging.Log;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * Concrete implementation of PrometheusClient that encapsulates all HTTP/JSON logic.
 * - The instance is bound to a basePrometheusUrl passed in the constructor.
 * - All returned time objects are ZonedDateTime in UTC.
 */
public final class PrometheusHttpClient implements PrometheusClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String[] POD_LABEL_CANDIDATES = PrometheusClient.POD_LABEL_CANDIDATES; // ändern
    private static final String FALLBACK_QUERY_RANGE = "365d";
    private final Log log;
    private static final String SERVER_LABEL = "instance";

    private final String baseUrl;

    /**
     * @param basePrometheusUrl e.g. "http://localhost:9090" or "http://localhost:9090/"
     */
    public PrometheusHttpClient(final String basePrometheusUrl, Log log) {
        this.baseUrl = normalizeBaseUrl(Objects.requireNonNull(basePrometheusUrl, "basePrometheusUrl must not be null"));
        this.log = log;
    }

    // -------------------
    // Helper utilities
    // -------------------
    private static String normalizeBaseUrl(final String url) {
        return url.endsWith("/") ? url : url + "/";
    }

    private JsonNode httpGetJson(final String rawUrl) throws Exception {
        final URL url = new URL(rawUrl);
        final HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout(10_000);
        con.setReadTimeout(30_000);
        con.setRequestMethod("GET");
        con.setRequestProperty("Accept", "application/json");
        final int code = con.getResponseCode();

        try (final BufferedReader in = new BufferedReader(new InputStreamReader(
                (code >= 200 && code < 300) ? con.getInputStream() : (con.getErrorStream() != null ? con.getErrorStream() : con.getInputStream()),
                StandardCharsets.UTF_8))) {
            final StringBuilder sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) sb.append(line);
            return MAPPER.readTree(sb.toString());
        } finally {
            con.disconnect();
        }
    }

    private static String urlEncode(final String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    /**
     * Convert a Prometheus numeric timestamp (seconds with possible fraction) to ZonedDateTime (UTC).
     * Handles nulls.
     */
    private static ZonedDateTime toZonedDateTimeFromDouble(final Double d) {
        if (d == null) return null;
        final long seconds = d.longValue();
        final double frac = d - (double) seconds;
        final long nanos = (long) Math.round(frac * 1_000_000_000L);
        final Instant inst = Instant.ofEpochSecond(seconds, nanos);
        return ZonedDateTime.ofInstant(inst, ZoneOffset.UTC);
    }

    /**
     * Parse Prometheus instant query result `data.result` -> value field -> index 1 = numeric string
     * Returns Double value or null if not present.
     */
    private static Double extractFirstResultValueAsDouble(final JsonNode root) {
        if (root == null) return null;
        if (!"success".equals(root.path("status").asText(null))) return null;
        final JsonNode results = root.path("data").path("result");
        if (!results.isArray() || results.size() == 0) return null;

        final JsonNode first = results.get(0);
        final JsonNode valueNode = first.path("value");
        if (!valueNode.isArray() || valueNode.size() < 2) return null;
        final String valueText = valueNode.get(1).asText(null);
        if (valueText == null || valueText.isEmpty()) return null;
        try {
            return Double.parseDouble(valueText);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    // -------------------
    // Interface methods
    // -------------------

    @Override
    public List<String> fetchMetricNames() throws Exception {
        final String api = baseUrl + "api/v1/label/__name__/values";
        final JsonNode root = httpGetJson(api);
        if (root != null && "success".equals(root.path("status").asText(null))) {
            final JsonNode data = root.path("data");
            if (data.isArray()) {
                final List<String> metrics = new ArrayList<>();
                for (final JsonNode n : data) metrics.add(n.asText());
                return metrics;
            }
        }
        return Collections.emptyList();
    }

    @Override
    public LabelValuesResult fetchFirstAvailableLabelValuesAndLabel() throws Exception {
        for (final String label : POD_LABEL_CANDIDATES) {
            try {
                final List<String> vals = fetchLabelValues(label);
                if (vals != null && !vals.isEmpty()) {
                    return new LabelValuesResult(label, vals);
                }
            } catch (Exception ignored) {
                // try next candidate
            }
        }
        return new LabelValuesResult(null, Collections.emptyList());
    }

    /**
     * Fetch values for a label name (e.g. instance/pod/...).
     */
    public List<String> fetchLabelValues(final String label) throws Exception {
        final String api = baseUrl + "api/v1/label/" + urlEncode(label) + "/values";
        final JsonNode root = httpGetJson(api);
        if (root != null && "success".equals(root.path("status").asText(null))) {
            final JsonNode data = root.path("data");
            if (data.isArray()) {
                final List<String> vals = new ArrayList<>();
                for (final JsonNode n : data) vals.add(n.asText());
                return vals;
            }
        }
        return Collections.emptyList();
    }

    /**
     * Return the last seen timestamp as ZonedDateTime for the given pod label and podName.
     * Uses the same PromQL as before but converts to ZonedDateTime.
     */
    @Override
    public ZonedDateTime getPodLastSeenAsZdt(final String label, final String podName) throws Exception {
        final String promQl = String.format("max_over_time(container_last_seen{%s=\"%s\"}[30d])", label, escapeLabelValue(podName));
        final String api = baseUrl + "api/v1/query?query=" + urlEncode(promQl);
        log.info("api " + api);
        final JsonNode root = httpGetJson(api);
        final Double d = extractFirstResultValueAsDouble(root);
        log.info("last seen: " + String.valueOf(d));
        return toZonedDateTimeFromDouble(d);


        /*if (!"success".equals(root.path("status").asText())) return null;
        JsonNode results = root.path("data").path("result");
        if (results.isArray() && results.size() > 0) {
            JsonNode val = results.get(0).path("value");
            if (val.isArray() && val.size() == 2) {
                return toZonedDateTimeFromDouble(val.get(1).asDouble());
            }
        }
        return null;*/
    }

    @Override
    public long getPrometheusTime() throws Exception {
        final String api = baseUrl + "api/v1/query?query=time()";
        final JsonNode root = httpGetJson(api);
        /* Prometheus typically returns e.g.
           "data": { "result": [ { "metric": {}, "value": [ "<ts>", "<val>" ] } ] }
           Some installations may return slightly different shapes; we read first result's value[1].
         */
        final Double d = extractFirstResultValueAsDouble(root);
        if (d != null) return d.longValue();
        return Instant.now().getEpochSecond();
    }

    /**
     * Determine Pod lifetime range.
     *
     * Strategy:
     *  1) Try kube_pod_created{pod="..."} for start (exact if available).
     *  2) If absent, fall back to min_over_time(container_last_seen{label="..."}[FALLBACK_RANGE]).
     *  3) For end always use max_over_time(container_last_seen{label="..."}[FALLBACK_RANGE]).
     *
     * Returns TimeRange with ZonedDateTime in UTC (nulls possible when unknown).
     */
    @Override
    public TimeRange getPodTimeRange(final String podName, final String genericLabel) throws Exception {
        final long promNow = getPrometheusTime();
        final String timeParam = "&time=" + promNow;

        // 1) Try kube_pod_created
        ZonedDateTime startZdt = null;
        try {
            final String k8sPromQl = String.format("kube_pod_created{pod=\"%s\"}", escapeLabelValue(podName));
            final String k8sApi = baseUrl + "api/v1/query?query=" + urlEncode(k8sPromQl) + timeParam;
            final JsonNode k8sRoot = httpGetJson(k8sApi);
            final Double startDouble = extractFirstResultValueAsDouble(k8sRoot);
            startZdt = toZonedDateTimeFromDouble(startDouble);
        } catch (Exception ignored) {
            // fallback below
        }

        // fallback to min_over_time(container_last_seen) if start unknown
        if (startZdt == null) {
            final String minPromQl = String.format("min_over_time(container_last_seen{%s=\"%s\"}[%s])", genericLabel, escapeLabelValue(podName), FALLBACK_QUERY_RANGE);
            final String minApi = baseUrl + "api/v1/query?query=" + urlEncode(minPromQl) + timeParam;
            final JsonNode minRoot = httpGetJson(minApi);
            final Double minD = extractFirstResultValueAsDouble(minRoot);
            startZdt = toZonedDateTimeFromDouble(minD);
        }

        // determine end via max_over_time
        final String maxPromQl = String.format("max_over_time(container_last_seen{%s=\"%s\"}[%s])", genericLabel, escapeLabelValue(podName), FALLBACK_QUERY_RANGE);
        final String maxApi = baseUrl + "api/v1/query?query=" + urlEncode(maxPromQl) + timeParam;
        final JsonNode maxRoot = httpGetJson(maxApi);
        final Double maxD = extractFirstResultValueAsDouble(maxRoot);
        final ZonedDateTime endZdt = toZonedDateTimeFromDouble(maxD);

        return new TimeRange(startZdt, endZdt);
    }

    /**
     * Small helper to escape double quotes inside label values.
     */
    private static String escapeLabelValue(final String raw) {
        if (raw == null) return "";
        return raw.replace("\"", "\\\"");
    }


    @Override
    public List<String> fetchServerNames() throws Exception {
        // ANSTATT: fetchLabelValues("instance") <- Das holt "global" alles (auch Müll).

        // WIR MACHEN: Eine explizite Query auf die Metriken, die wir für Pods nutzen.
        // Das entspricht deiner manuellen Prüfung: count(container_last_seen) by (instance)
        // Wir nutzen "count" oder "group", um nur die unique keys zu bekommen.

        // Schritt 1: Versuche 'node' Label (K8s Standard), falls vorhanden
        List<String> nodes = fetchLabelValuesFromQuery("count(container_last_seen) by (node)", "node");
        if (!nodes.isEmpty()) {
            return nodes;
        }

        // Schritt 2: Fallback auf 'instance', aber NUR von container_last_seen
        // Damit filtern wir die ganzen "10.0.2.9:4317" Scrape-Targets aus, die an anderen Metriken hängen.
        return fetchLabelValuesFromQuery("count(container_last_seen) by (instance)", "instance");
    }

    /**
     * Hilfsmethode: Führt eine PromQL Query aus und extrahiert alle Werte eines bestimmten Labels aus dem Ergebnis.
     */
    private List<String> fetchLabelValuesFromQuery(String promQl, String labelName) throws Exception {
        String api = baseUrl + "api/v1/query?query=" + urlEncode(promQl);
        JsonNode root = httpGetJson(api);

        List<String> results = new ArrayList<>();
        if (root != null && "success".equals(root.path("status").asText(null))) {
            JsonNode data = root.path("data").path("result");
            if (data.isArray()) {
                for (JsonNode row : data) {
                    JsonNode metric = row.path("metric");
                    String val = metric.path(labelName).asText(null);
                    if (val != null && !val.isEmpty()) {
                        results.add(val);
                    }
                }
            }
        }
        return results;
    }

    @Override
    public TimeRange getServerTimeRange(String serverName) throws Exception {
        // Logik analog zu getPodTimeRange, aber für Server (instance)
        // Wir nutzen hier einfach container_last_seen aggregation für den Server
        return getEntityTimeRange(serverName, SERVER_LABEL);
    }

    // Refactored helper um Code-Duplizierung zu vermeiden (kannst du auch copy-pasten und anpassen)
    private TimeRange getEntityTimeRange(String name, String label) throws Exception {
        long promNow = getPrometheusTime();
        String timeParam = "&time=" + promNow;

        // Start: Min over time
        String minPromQl = String.format("min_over_time(container_last_seen{%s=\"%s\"}[%s])", label, escapeLabelValue(name), "365d");
        JsonNode minRoot = httpGetJson(baseUrl + "api/v1/query?query=" + urlEncode(minPromQl) + timeParam);
        Double minD = extractFirstResultValueAsDouble(minRoot);
        ZonedDateTime start = toZonedDateTimeFromDouble(minD);

        // End: Max over time
        String maxPromQl = String.format("max_over_time(container_last_seen{%s=\"%s\"}[%s])", label, escapeLabelValue(name), "365d");
        JsonNode maxRoot = httpGetJson(baseUrl + "api/v1/query?query=" + urlEncode(maxPromQl) + timeParam);
        Double maxD = extractFirstResultValueAsDouble(maxRoot);
        ZonedDateTime end = toZonedDateTimeFromDouble(maxD);

        return new TimeRange(start, end);
    }

    @Override
    public Map<String, String> fetchPodToServerMapping() throws Exception {
        // Wir suchen Metriken, die sowohl 'pod' als auch 'instance' haben.
        // topk(1, ...) by (pod, instance) ist effizient, um die Tupel zu bekommen.
        // Wir schauen uns die letzten 60 Minuten an (oder Instant query für 'jetzt').
        String promQl = "topk(1, container_last_seen{pod!=\"\", instance!=\"\"}) by (pod, instance)";

        String api = baseUrl + "api/v1/query?query=" + urlEncode(promQl);
        JsonNode root = httpGetJson(api);

        Map<String, String> mapping = new HashMap<>();
        if (root == null || !"success".equals(root.path("status").asText(null))) {
            return mapping;
        }

        JsonNode results = root.path("data").path("result");
        if (results.isArray()) {
            for (JsonNode r : results) {
                JsonNode metric = r.path("metric");
                String pod = metric.path("pod").asText(null);
                String instance = metric.path("instance").asText(null);
                if (pod != null && instance != null) {
                    mapping.put(pod, instance);
                }
            }
        }
        return mapping;
    }
}
