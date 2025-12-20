package util.trace.sources;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;

public class JaegerGetSpansProcedure {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    private static final Gson gson = new Gson();

    // Reservierte Keys in der params-Map, die KEINE Tag-Filter sind
    private static final Set<String> CONFIG_KEYS = Set.of("range", "limit", "time", "operation", "service");

    @Procedure(name = "graphobs.data.get_spans", mode = Mode.READ)
    @Description("Fetches spans from Jaeger. Filters by operation and tags (including process tags like pod name).")
    public Stream<SpanResult> getSpans(
            @Name("service") String serviceName,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        if (serviceName == null || serviceName.isBlank()) return Stream.empty();
        Map<String, Object> safeParams = (params == null) ? Collections.emptyMap() : params;

        try {
            String baseUrl = loadBaseUrlFromGraph();
            if (baseUrl == null) return Stream.empty();

            // 1. Parameter extrahieren
            String opFilter = (String) safeParams.get("operation");
            Instant endInstant = parseTime(safeParams.get("time"));
            String rangeStr = Objects.toString(safeParams.getOrDefault("range", "-1h"), "-1h");
            long rangeMillis = parseRangeToMillis(rangeStr);
            Instant startInstant = endInstant.minusMillis(rangeMillis);

            int limit = 1000;
            if (safeParams.containsKey("limit")) {
                limit = Integer.parseInt(safeParams.get("limit").toString());
            }

            // 2. URL mit Filtern bauen
            StringBuilder urlBuilder = new StringBuilder(baseUrl)
                    .append("/traces?service=")
                    .append(URLEncoder.encode(serviceName, StandardCharsets.UTF_8));

            // DIREKT-FILTER: Operation
            if (opFilter != null && !opFilter.isBlank()) {
                urlBuilder.append("&operation=")
                        .append(URLEncoder.encode(opFilter, StandardCharsets.UTF_8));
            }

            // DIREKT-FILTER: Tags (Optional, reduziert Datenmenge enorm)
            for (Map.Entry<String, Object> entry : safeParams.entrySet()) {
                if (!CONFIG_KEYS.contains(entry.getKey())) {
                    // Jaeger API erwartet Tags im Format: &tag=key:value
                    String tagQuery = entry.getKey() + ":" + entry.getValue();
                    urlBuilder.append("&tag=")
                            .append(URLEncoder.encode(tagQuery, StandardCharsets.UTF_8));
                }
            }

            urlBuilder.append("&start=").append(startInstant.toEpochMilli() * 1000L);
            urlBuilder.append("&end=").append(endInstant.toEpochMilli() * 1000L);
            urlBuilder.append("&limit=").append(limit);

            // 3. Request absetzen
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .timeout(Duration.ofSeconds(20))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 300) return Stream.empty();

            JaegerResponseRoot responseRoot = gson.fromJson(resp.body(), JaegerResponseRoot.class);
            if (responseRoot == null || responseRoot.data == null) return Stream.empty();

            List<SpanResult> results = new ArrayList<>();

            for (JTrace trace : responseRoot.data) {
                if (trace.spans == null) continue;

                Map<String, Map<String, Object>> processTagsMap = new HashMap<>();
                if (trace.processes != null) {
                    for (Map.Entry<String, JProcess> entry : trace.processes.entrySet()) {
                        processTagsMap.put(entry.getKey(), convertTags(entry.getValue().tags));
                    }
                }

                for (JSpan span : trace.spans) {
                    // Da Jaeger die Operation und die meisten Tags schon gefiltert hat,
                    // müssen wir hier fast nichts mehr prüfen.

                    Map<String, Object> mergedTags = new HashMap<>();
                    if (span.processID != null && processTagsMap.containsKey(span.processID)) {
                        mergedTags.putAll(processTagsMap.get(span.processID));
                    }
                    mergedTags.putAll(convertTags(span.tags));

                    // Sicherheitshalber: Falls ein Tag in den 'processes' (z.B. pod.name)
                    // steckt, den Jaeger nicht über den &tag Parameter filtern konnte:
                    if (!matchesCustomFilters(safeParams, mergedTags)) {
                        continue;
                    }

                    results.add(new SpanResult(
                            trace.traceID, span.spanID, getParentId(span),
                            span.operationName, serviceName, span.startTime,
                            span.duration, mergedTags
                    ));
                }
            }
            return results.stream();

        } catch (Exception e) {
            log.error("Error in get_spans: " + e.getMessage());
            return Stream.empty();
        }
    }


    /**
     * Prüft, ob alle custom keys in params auch in den tags enthalten sind und übereinstimmen.
     */
    private boolean matchesCustomFilters(Map<String, Object> params, Map<String, Object> tags) {
        for (Map.Entry<String, Object> param : params.entrySet()) {
            String key = param.getKey();
            if (CONFIG_KEYS.contains(key)) continue; // Config-Keys ignorieren

            Object requiredValue = param.getValue();
            Object actualValue = tags.get(key);

            if (actualValue == null) return false; // Tag fehlt

            // Einfacher String-Vergleich um Typ-Probleme (Int vs Long) zu vermeiden
            if (!String.valueOf(requiredValue).equals(String.valueOf(actualValue))) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Object> convertTags(List<JTag> jTags) {
        Map<String, Object> map = new HashMap<>();
        if (jTags == null) return map;
        for (JTag t : jTags) {
            map.put(t.key, t.value);
        }
        return map;
    }

    private String getParentId(JSpan span) {
        if (span.references != null) {
            for (JReference ref : span.references) {
                if ("CHILD_OF".equalsIgnoreCase(ref.refType)) {
                    return ref.spanID;
                }
            }
        }
        return null;
    }

    // --- Helpers für Zeit/Config ---
    private Instant parseTime(Object t) {
        if (t == null) return Instant.now();
        if (t instanceof Number) return Instant.ofEpochMilli(((Number) t).longValue());
        try { return Instant.parse(t.toString()); }
        catch (Exception e) {
            try { return java.time.LocalDateTime.parse(t.toString()).atZone(ZoneId.systemDefault()).toInstant(); }
            catch (Exception ignored) { return Instant.now(); }
        }
    }

    private long parseRangeToMillis(String range) {
        if (range == null || range.isBlank()) return 3600_000L;
        String s = range.trim().replace("-", "");
        long mult = 1000L; // default seconds? Jaeger UI defaults usually vary, let's safe parse
        if (s.endsWith("ms")) { mult = 1L; s = s.substring(0, s.length()-2); }
        else if (s.endsWith("h")) { mult = 3600000L; s = s.substring(0, s.length()-1); }
        else if (s.endsWith("m")) { mult = 60000L; s = s.substring(0, s.length()-1); }
        else if (s.endsWith("s")) { mult = 1000L; s = s.substring(0, s.length()-1); }
        try { return Long.parseLong(s) * mult; } catch (Exception e) { return 3600_000L; }
    }

    private String loadBaseUrlFromGraph() {
        try (var tx = db.beginTx()) {
            var res = tx.execute("MATCH (j:Jaeger) RETURN j.baseUrl AS baseUrl LIMIT 1");
            if (res.hasNext()) return Objects.toString(res.next().get("baseUrl"), null);
        }
        return null;
    }

    // ==========================================
    // Interne DTO Klassen für Gson Deserialization
    // ==========================================
    private static class JaegerResponseRoot {
        public List<JTrace> data;
    }

    private static class JTrace {
        public String traceID;
        public List<JSpan> spans;
        public Map<String, JProcess> processes; // WICHTIG für Pod Name etc.
    }

    private static class JSpan {
        public String traceID;
        public String spanID;
        public String operationName;
        public String processID; // Link zu processes
        public long startTime; // micros
        public long duration;  // micros
        public List<JTag> tags;
        public List<JReference> references;
    }

    private static class JProcess {
        public String serviceName;
        public List<JTag> tags;
    }

    private static class JTag {
        public String key;
        public String type;
        public Object value;
    }

    private static class JReference {
        public String refType; // z.B. CHILD_OF
        public String traceID;
        public String spanID;
    }

    // ==========================================
    // Result Klasse für Neo4j Output
    // ==========================================
    public static class SpanResult {
        public String traceId;
        public String spanId;
        public String parentId;
        public String operation;
        public String service;
        public Long startTimeMicros;
        public Long durationMicros;
        public Map<String, Object> tags;

        public SpanResult(String traceId, String spanId, String parentId, String operation,
                          String service, Long startTimeMicros, Long durationMicros, Map<String, Object> tags) {
            this.traceId = traceId;
            this.spanId = spanId;
            this.parentId = parentId;
            this.operation = operation;
            this.service = service;
            this.startTimeMicros = startTimeMicros;
            this.durationMicros = durationMicros;
            this.tags = tags;
        }
    }
}