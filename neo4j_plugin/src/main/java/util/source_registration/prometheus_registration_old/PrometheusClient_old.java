package util.source_registration.prometheus_registration_old;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.logging.Log;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class PrometheusClient_old {

    private static final String[]  POD_LABEL_CANDIDATES = new String[] {
            "pod",
            "pod_name",
            "kubernetes_pod_name",
            "container",
            "container_name",
            "name",
            "container_label_io_kubernetes_pod_name",
            "exported_pod",
            "pod_id",
            "com_docker_compose_service"
    };


    private static final String[] SERVER_NODE_LABEL_CANDIDATES = new String[] {
            "instance",
            "nodename",
            "hostname",
            "node",
            "kubernetes_node",
            "exported_instance",
            "exported_nodename",
            "host"
    };

    private static final String FALLBACK_QUERY_RANGE = "365d";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class LabelValuesResult {
        public final String label;
        public final List<String> values;

        public LabelValuesResult(String label, List<String> values) {
            this.label = label;
            this.values = values;
        }
    }

    // -----------------------
    // HTTP + Prometheus helpers
    // -----------------------
    public static JsonNode httpGetJson(String rawUrl) throws Exception {
        URL url = new URL(rawUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout(10_000);
        con.setReadTimeout(30_000);
        con.setRequestMethod("GET");
        con.setRequestProperty("Accept", "application/json");
        int code = con.getResponseCode();
        BufferedReader in;
        if (code >= 200 && code < 300) {
            in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
        } else {
            in = new BufferedReader(new InputStreamReader(con.getErrorStream() != null ? con.getErrorStream() : con.getInputStream(), "UTF-8"));
        }
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = in.readLine()) != null) sb.append(line);
        in.close();
        return MAPPER.readTree(sb.toString());
    }

    public static List<String> fetchMetricNames(String prometheusBase) throws Exception {
        String url = prometheusBase;
        if (!url.endsWith("/")) url += "/";
        // /api/v1/label/__name__/values
        String api = url + "api/v1/label/__name__/values";
        JsonNode root = httpGetJson(api);
        if (root.has("status") && "success".equals(root.get("status").asText()) && root.has("data")) {
            List<String> list = new ArrayList<>();
            for (JsonNode n : root.get("data")) list.add(n.asText());
            return list;
        }
        return Collections.emptyList();
    }

    public static List<String> fetchLabelValues(String prometheusBase, String label) throws Exception {
        String url = prometheusBase;
        if (!url.endsWith("/")) url += "/";
        String api = url + "api/v1/label/" + encode(label) + "/values";
        JsonNode root = httpGetJson(api);
        if (root.has("status") && "success".equals(root.get("status").asText()) && root.has("data")) {
            List<String> list = new ArrayList<>();
            for (JsonNode n : root.get("data")) list.add(n.asText());
            return list;
        }
        return Collections.emptyList();
    }

    /**
     * Similar to previous helper but returns the label name that produced results and the values.
     */
    public static LabelValuesResult fetchFirstAvailableLabelValuesAndLabel(String prometheusBase) {
        for (String label : POD_LABEL_CANDIDATES) {
            try {
                List<String> vals = fetchLabelValues(prometheusBase, label);
                if (vals != null && !vals.isEmpty()) return new LabelValuesResult(label, vals);
            } catch (Exception ignore) { /* try next */ }
        }
        return new LabelValuesResult(null, Collections.emptyList());
    }

    private static String encode(String s) {
        return s.replace(" ", "%20");
    }

    /**
     * Returns the last seen timestamp (max) in seconds since epoch for the given podName using the provided label.
     * Returns null if no result.
     */
    public static Double getPodLastSeen(String prometheusBase, String label, String podName) throws Exception {
        String base = prometheusBase;
        if (!base.endsWith("/")) base += "/";
        // Ask Prometheus for the maximum timestamp observed for this pod within the last 30d window.
        String promQl = String.format("max_over_time(timestamp(container_last_seen{%s=\"%s\"}[30d]))", label, podName.replace("\"", "\\\""));
        String api = base + "api/v1/query?query=" + java.net.URLEncoder.encode(promQl, "UTF-8");
        JsonNode root = httpGetJson(api);
        if (!"success".equals(root.path("status").asText())) return null;
        JsonNode results = root.path("data").path("result");
        if (results.isArray() && results.size() > 0) {
            JsonNode val = results.get(0).path("value");
            if (val.isArray() && val.size() == 2) {
                return val.get(1).asDouble();
            }
        }
        return null;
    }

    // kleiner Hilfsrückgabewert
    public static class TimeRange {
        public final String start;
        public final String end;

        public TimeRange(String start, String end) {
            this.start = start;
            this.end = end;
        }
    }

    // Bestehende Methode konservieren (falls extern genutzt)
    // zeitraum relativ machen für historische daten !!!!
    /*public static TimeRange getPodTimeRange_II(String prometheusUrl, String podName, String label, Log log) throws Exception {
        String base = prometheusUrl.endsWith("/") ? prometheusUrl : prometheusUrl + "/";

        // Step 1: Get a consistent timestamp for the queries.
        long currentTs = getPrometheusTime(base);

        // Step 2: Query for the minimum timestamp using the provided label.
        String queryMin = String.format(
                "min_over_time(container_last_seen{%s=\"%s\"}[30d])", label, podName.replace("\"", "\\\"")
        );
        Double minVal = queryPrometheusDouble(base, queryMin, currentTs, log);

        // Step 3: Query for the maximum timestamp using the provided label.
        String queryMax = String.format(
                "max_over_time(container_last_seen{%s=\"%s\"}[30d])", label, podName.replace("\"", "\\\"")
        );
        Double maxVal = queryPrometheusDouble(base, queryMax, currentTs, log);

        // Step 4: Convert timestamps to ISO8601 strings.
        String startIso = minVal != null ? Instant.ofEpochSecond(minVal.longValue()).toString() : null;
        String endIso   = maxVal != null ? Instant.ofEpochSecond(maxVal.longValue()).toString()   : null;

        return new TimeRange(startIso, endIso);
    }*/

    /**
     * Ermittelt den Lebenszeitraum eines Containers/Pods.
     * Versucht zuerst, die Kubernetes-spezifische Metrik 'kube_pod_created' für eine exakte Startzeit zu verwenden.
     * Wenn dies fehlschlägt (z. B. weil es kein Kubernetes-Pod ist oder kube-state-metrics fehlt),
     * greift die Methode auf die generische 'container_last_seen'-Metrik zurück, um den Startzeitpunkt zu schätzen.
     *
     * @param prometheusUrl Die Basis-URL des Prometheus-Servers.
     * @param podName       Der Name des Pods/Containers.
     * @param genericLabel  Das Label, das für die generische Fallback-Abfrage verwendet wird (z. B. "pod_name", "container_name").
     * @param log           Ein Logger-Objekt.
     * @return Ein TimeRange-Objekt mit dem Start- und Endzeitpunkt als ISO-8601-Strings.
     * @throws Exception bei Fehlern während der Prometheus-Abfrage.
     */
    public static TimeRange getPodTimeRange(String prometheusUrl, String podName, String genericLabel, Log log) throws Exception {
        String base = prometheusUrl.endsWith("/") ? prometheusUrl : prometheusUrl + "/";
        long currentTs = getPrometheusTime(base);

        Double startTime = null;

        // --- Schritt 1: Versuch, die exakte Startzeit über die Kubernetes-Metrik zu erhalten ---
        try {
            // Annahme: Das Standard-Label in kube-state-metrics für den Pod-Namen ist 'pod'.
            String k8sQuery = String.format("kube_pod_created{pod=\"%s\"}", podName.replace("\"", "\\\""));
            startTime = queryPrometheusDouble(base, k8sQuery, currentTs, log);
            if (startTime != null) {
                log.info("Kubernetes metric 'kube_pod_created' found for pod: " + podName);
            }
        } catch (Exception e) {
            log.warn("Could not query 'kube_pod_created' for pod '" + podName + "'. Will try fallback method. Error: " + e.getMessage());
        }


        // --- Schritt 2: Fallback, falls die K8s-Metrik nicht erfolgreich war ---
        if (startTime == null) {
            log.info("Falling back to generic 'min_over_time' for pod: " + podName);
            String fallbackQueryMin = String.format(
                    "min_over_time(container_last_seen{%s=\"%s\"}[%s])",
                    genericLabel, podName.replace("\"", "\\\""), FALLBACK_QUERY_RANGE
            );
            startTime = queryPrometheusDouble(base, fallbackQueryMin, currentTs, log);
        }

        // --- Schritt 3: Den letzten bekannten Zeitpunkt ermitteln (dieser ist für beide Methoden gleich) ---
        // Annahme: Das Label für 'container_last_seen' ist 'pod'. Dies ist bei cAdvisor oft der Fall.
        String queryMax = String.format(
                "max_over_time(container_last_seen{%s=\"%s\"}[%s])",
                genericLabel, podName.replace("\"", "\\\""), FALLBACK_QUERY_RANGE
        );
        Double endTime = queryPrometheusDouble(base, queryMax, currentTs, log);

        // --- Schritt 4: Zeitstempel in ISO-8601-Strings umwandeln ---
        String startIso = startTime != null ? Instant.ofEpochSecond(startTime.longValue()).toString() : null;
        String endIso = endTime != null ? Instant.ofEpochSecond(endTime.longValue()).toString() : null;

        return new TimeRange(startIso, endIso);
    }

    public static long getPrometheusTime(String base) throws Exception {
        String api = base + "api/v1/query?query=time()";
        JsonNode root = httpGetJson(api);
        if (root.path("status").asText().equals("success")) {
            JsonNode result = root.path("data").path("result");
            if (result.isArray() && result.size() >= 2) {
                return (long) result.get(1).asDouble();
            }
        }
        return Instant.now().getEpochSecond();
    }

    private static Double queryPrometheusDouble(String base, String promQl, long ts, Log log) throws Exception {
        // The "&time=" parameter is crucial for querying historical data.
        String api = base + "api/v1/query?query=" + java.net.URLEncoder.encode(promQl, "UTF-8")
                + "&time=" + ts; // This line should be uncommented.

        JsonNode root = httpGetJson(api);
        if (!"success".equals(root.path("status").asText())) return null;
        JsonNode results = root.path("data").path("result");
        if (results.isArray() && results.size() > 0) {
            JsonNode val = results.get(0).path("value");
            if (val.isArray() && val.size() == 2) {
                return val.get(1).asDouble();
            }
        }
        return null;
    }


}



/*public class PrometheusClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // -----------------------
    // HTTP + Prometheus helpers
    // -----------------------
    public static JsonNode httpGetJson(String rawUrl) throws Exception {
        URL url = new URL(rawUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout(10_000);
        con.setReadTimeout(30_000);
        con.setRequestMethod("GET");
        con.setRequestProperty("Accept", "application/json");
        int code = con.getResponseCode();
        BufferedReader in;
        if (code >= 200 && code < 300) {
            in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
        } else {
            in = new BufferedReader(new InputStreamReader(con.getErrorStream() != null ? con.getErrorStream() : con.getInputStream(), "UTF-8"));
        }
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = in.readLine()) != null) sb.append(line);
        in.close();
        return MAPPER.readTree(sb.toString());
    }

    public static List<String> fetchMetricNames(String prometheusBase) throws Exception {
        String url = prometheusBase;
        if (!url.endsWith("/")) url += "/";
        // /api/v1/label/__name__/values
        String api = url + "api/v1/label/__name__/values";
        JsonNode root = httpGetJson(api);
        if (root.has("status") && "success".equals(root.get("status").asText()) && root.has("data")) {
            List<String> list = new ArrayList<>();
            for (JsonNode n : root.get("data")) list.add(n.asText());
            return list;
        }
        return Collections.emptyList();
    }

    public static List<String> fetchLabelValues(String prometheusBase, String label) throws Exception {
        String url = prometheusBase;
        if (!url.endsWith("/")) url += "/";
        String api = url + "api/v1/label/" + encode(label) + "/values";
        JsonNode root = httpGetJson(api);
        if (root.has("status") && "success".equals(root.get("status").asText()) && root.has("data")) {
            List<String> list = new ArrayList<>();
            for (JsonNode n : root.get("data")) list.add(n.asText());
            return list;
        }
        return Collections.emptyList();
    }

    public static List<String> fetchFirstAvailableLabelValues(String prometheusBase, String[] candidates) {
        for (String label : candidates) {
            try {
                List<String> vals = fetchLabelValues(prometheusBase, label);
                if (vals != null && !vals.isEmpty()) return vals;
            } catch (Exception ignore) { /* try next */ /*}
        }
        return Collections.emptyList();
    }

    private static String encode(String s) {
        return s.replace(" ", "%20");
    }


    /**
            * Fragt für einen Container (podName) die minimale und maximale Zeit ab, zu der
     * Prometheus Metriken (container_last_seen) vorliegen.
            *
            * @param prometheusUrl z. B. "http://localhost:9090"
            * @param podName z. B. "socialnetwork-compose-post-service-1"
            * @return ein einfaches Objekt mit start und end (ISO8601 Strings)
     */
    /*public static TimeRange getPodTimeRange(String prometheusUrl, String podName) throws Exception {
        String base = prometheusUrl.endsWith("/") ? prometheusUrl : prometheusUrl + "/";

        // --- Schritt 1: aktuellen Prometheus-Zeitstempel abfragen (jetzt-Zeitpunkt in Prometheus)
        long currentTs = getPrometheusTime(base);

        // --- Schritt 2: min_over_time(...)
        String queryMin = String.format(
                "min_over_time(timestamp(container_last_seen{name=\"%s\"}[30d]))", podName
        );
        Double minVal = queryPrometheusDouble(base, queryMin, currentTs);

        // --- Schritt 3: max_over_time(...)
        String queryMax = String.format(
                "max_over_time(timestamp(container_last_seen{name=\"%s\"}[30d]))", podName
        );
        Double maxVal = queryPrometheusDouble(base, queryMax, currentTs);

        // --- Schritt 4: ISO-Strings zurückgeben
        String startIso = minVal != null ? Instant.ofEpochSecond(minVal.longValue()).toString() : null;
        String endIso   = maxVal != null ? Instant.ofEpochSecond(maxVal.longValue()).toString()   : null;

        return new TimeRange(startIso, endIso);
    }

    // Kleine Hilfsmethode: aktueller Serverzeitpunkt von Prometheus (Unix seconds)
    /*private static long getPrometheusTime(String base) throws Exception {
        String api = base + "api/v1/query?query=time()";
        JsonNode root = httpGetJson(api);
        if (root.path("status").asText().equals("success")) {
            return root.path("data").path("result").get(0).path("value").get(1).asLong();
        }
        return Instant.now().getEpochSecond();
    }*/

    /*private static long getPrometheusTime(String base) throws Exception {
        String api = base + "api/v1/query?query=time()";
        JsonNode root = httpGetJson(api);
        if (root.path("status").asText().equals("success")) {
            // Für ein skalares Ergebnis wie time() ist 'result' ein Array: [timestamp, value]
            JsonNode result = root.path("data").path("result");
            if (result.isArray() && result.size() >= 2) {
                // Der Wert ist das zweite Element, eine String-Repräsentation des Timestamps.
                // Wir parsen dies als Double und konvertieren es dann in einen Long.
                return (long) result.get(1).asDouble();
            }
        }
        // Fallback auf die lokale Zeit, wenn der API-Aufruf fehlschlägt
        return Instant.now().getEpochSecond();
    }

    // Führt PromQL-Abfrage aus und gibt den Zahlenwert zurück (Double oder null)
    private static Double queryPrometheusDouble(String base, String promQl, long ts) throws Exception {
        String api = base + "api/v1/query?query=" + java.net.URLEncoder.encode(promQl, "UTF-8")
                + "&time=" + ts;
        JsonNode root = httpGetJson(api);
        if (!"success".equals(root.path("status").asText())) return null;
        JsonNode results = root.path("data").path("result");
        if (results.isArray() && results.size() > 0) {
            JsonNode val = results.get(0).path("value");
            if (val.isArray() && val.size() == 2) {
                return val.get(1).asDouble();
            }
        }
        return null;
    }



    // einfacher Rückgabetyp
    public static class TimeRange {
        public final String start;
        public final String end;

        public TimeRange(String start, String end) {
            this.start = start;
            this.end = end;
        }
    }

}*/
