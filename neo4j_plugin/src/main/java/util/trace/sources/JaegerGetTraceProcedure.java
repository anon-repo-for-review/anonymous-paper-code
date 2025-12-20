/*
Example 1:
CALL timegraph.data.get_trace(
  "media-service",
  { range: "-1h", limit: 100})
YIELD traceId, service, spanCount, startTime, durationMs, traceJSON
RETURN * LIMIT 50;

Example 2:
CALL timegraph.data.get_trace(
  "compose-post-service",
  {
    time: toString(datetime()),
    range: "1h",
    limit: 100,
    operation: "compose_post_server",
    status_code: 200
  }
)
YIELD traceId, service, spanCount, startTime, durationMs, traceJSON
RETURN *;

*/
package util.trace.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import util.trace.datatype.datastructs.JaegerTraces;
import util.trace.datatype.datastructs.Span;
import util.trace.datatype.datastructs.Tag;
import util.trace.datatype.datastructs.Trace;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Stream;

public class JaegerGetTraceProcedure {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    private static final Gson gson = new Gson();

    @Procedure(name = "graphobs.data.get_trace", mode = Mode.READ)
    @Description("Fetches traces from Jaeger with filters: service, time, range, limit, operation, status_code")
    public Stream<TraceResult> getTraces(
            @Name("service") String serviceName,
            @Name("params") Map<String, Object> params
    ) {
        try {
            if (serviceName == null || serviceName.isBlank()) {
                log.warn("get_trace called with empty service name");
                return Stream.empty();
            }

            //
            String apiTracesPath = "/traces";
            String baseUrl = loadBaseUrlFromGraph();
            if (baseUrl == null || baseUrl.isBlank()) {
                log.error("No Jaeger baseUrl found in graph. Create (Jaeger {baseUrl:'...'}) first.");
                return Stream.empty();
            }


            // parse time
            Instant endInstant = Instant.now();
            if (params != null && params.containsKey("time")) {
                Object t = params.get("time");
                if (t instanceof Number) {
                    // epoch millis
                    endInstant = Instant.ofEpochMilli(((Number) t).longValue());
                } else {
                    try {
                        endInstant = Instant.parse(t.toString());
                    } catch (DateTimeParseException ex) {
                        // fallback: try parse as LocalDateTime without zone
                        try {
                            endInstant = java.time.LocalDateTime.parse(t.toString()).atZone(ZoneId.systemDefault()).toInstant();
                        } catch (Exception e2) {
                            log.warn("Could not parse 'time' param '%s', fallback to now: %s", t, e2.getMessage());
                            endInstant = Instant.now();
                        }
                    }
                }
            }
            //parse range:
            //checking if range is defined:
            String rangeStr = params != null ? Objects.toString(params.getOrDefault("range", "-1h"), "-1h") : "-1h";
            long rangeMillis = parseRangeToMillis(rangeStr);
            Instant startInstant = endInstant.minusMillis(rangeMillis);
            //parse limit:
            int limit = 1000;

            if (params != null && params.containsKey("limit")) {
                Object limObj = params.get("limit");
                try {
                    if (limObj instanceof Number) {
                        limit = ((Number) limObj).intValue();
                    } else {
                        limit = Integer.parseInt(limObj.toString());
                    }
                } catch (Exception e) {
                    log.warn("Could not parse limit '%s', using default 1000", limObj);
                }
            }

            //parse operations:
            String opFilter = params != null ? Objects.toString(params.getOrDefault("operation", null), null) : null;
            //parse statuscode:
            Integer statusFilter = null;
            if (params != null && params.containsKey("status_code")) {
                try {
                    statusFilter = ((Number) params.get("status_code")).intValue();
                } catch (ClassCastException ice) {
                    try { statusFilter = Integer.parseInt(params.get("status_code").toString()); } catch (Exception ignored) {}
                }
            }

            // Jaeger Convert millis -> micros.
            long startMicros = startInstant.toEpochMilli() * 1000L;
            long endMicros = endInstant.toEpochMilli() * 1000L;
            //Sending HTTP request to jaeger:
            String url = baseUrl + apiTracesPath
                    + "?service=" + URLEncoder.encode(serviceName, StandardCharsets.UTF_8)
                    + "&start=" + startMicros
                    + "&end=" + endMicros
                    + "&limit=" + limit;

            log.info("timegraph.data.get_trace - querying Jaeger: %s", url);

            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(20))
                    .header("Accept", "application/json")
                    .GET()
                    .build();
            //HTTP response:
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                log.error("Jaeger HTTP error: status=%d body=%s", resp.statusCode(), snippet(resp.body(), 500));
                return Stream.empty();
            }

            JaegerTraces jt = gson.fromJson(resp.body(), JaegerTraces.class);
            if (jt == null || jt.getData() == null || jt.getData().isEmpty()) {
                log.info("No traces returned from Jaeger for service=%s in window %s - %s", serviceName, startInstant, endInstant);
                return Stream.empty();
            }

            List<TraceResult> results = new ArrayList<>();
            //Iterating through Traces:
            for (Trace t : jt.getData()) {
                if (t == null || t.getSpans() == null || t.getSpans().isEmpty()) continue;

                // compute spanCount, startTime (min span.startTime), duration (max end - min start)
                long minStart = Long.MAX_VALUE;
                long maxEnd = Long.MIN_VALUE;
                int spanCount = 0;

                boolean opOk = (opFilter == null);
                boolean statusOk = (statusFilter == null);

                for (Span sp : t.getSpans()) {
                    if (sp == null) continue;
                    spanCount++;

                    long sStart = sp.getStartTime(); //treating micros if obviously large
                    long sDuration = sp.getDuration();
                    long sEnd = sStart + sDuration;

                    // normalize to millis for computations shown back to user:
                    long sStartMillis = sStart/1000L;
                    long sEndMillis = sStartMillis + sDuration/1000L;

                    if (sStartMillis < minStart) minStart = sStartMillis;
                    if (sEndMillis > maxEnd) maxEnd = sEndMillis;

                    // operation filter: exact match or substring
                    if (opFilter != null && sp.getOperationName() != null) {
                        if (sp.getOperationName().equals(opFilter) || sp.getOperationName().contains(opFilter)) {
                            opOk = true;
                        }
                    }

                    // status filter: try http.status_code tag (various key names)
                    if (statusFilter != null && sp.getTags() != null) {
                        for (Tag tag : sp.getTags()) {
                            if (tag == null || tag.getKey() == null) continue;
                            String key = tag.getKey();
                            Object val = tag.getValue();

                            //TODO: find out which one
                            if ("http.status_code".equals(key) || "status.code".equals(key) || "status".equals(key)) {
                                Integer sc = tryExtractInt(val);
                                if (sc != null && sc.equals(statusFilter)) {
                                    statusOk = true;
                                }
                            }
                        }
                    }
                }

                if (!opOk || !statusOk) {
                    // filtered out by client-side filters
                    continue;
                }

                // If we can't determine minStart/maxEnd, set them to trace-level approximations
                long durationMs = 0;
                String startIso = null;
                if (minStart != Long.MAX_VALUE && maxEnd != Long.MIN_VALUE) {
                    durationMs = Math.max(0, maxEnd - minStart);
                    startIso = Instant.ofEpochMilli(minStart).toString();
                }
                ObjectMapper mapper = new ObjectMapper();
                String traceJSON = mapper.writeValueAsString(t);
                results.add(new TraceResult(t.getTraceID(), serviceName, spanCount, startIso, durationMs,traceJSON));
            }

            if (results.isEmpty()) {
                log.info("No traces passed client-side filters for service=%s", serviceName);
            }

            return results.stream();

        } catch (Exception e) {
            log.error("Error in get_trace: %s", e.getMessage());
            return Stream.of(new TraceResult("ERROR", e.getMessage(), 0, null, 0,""));
        }
    }

    private static String snippet(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    // parse "-1h", "1h", "30m", "120s" to milliseconds
    private long parseRangeToMillis(String range) {
        if (range == null || range.isBlank()) return 3600_000L;
        String s = range.trim();
        if (s.startsWith("-")) s = s.substring(1);
        try {
            if (s.endsWith("h")) {
                long n = Long.parseLong(s.substring(0, s.length()-1));
                return n * 3600_000L;
            } else if (s.endsWith("m")) {
                long n = Long.parseLong(s.substring(0, s.length()-1));
                return n * 60_000L;
            } else if (s.endsWith("s")) {
                long n = Long.parseLong(s.substring(0, s.length()-1));
                return n * 1000L;
            } else {
                // treat as minutes if plain number
                long n = Long.parseLong(s);
                return n * 60_000L;
            }
        } catch (Exception ex) {
            log.warn("Could not parse range '%s', defaulting to 1h: %s", range, ex.getMessage());
            return 3600_000L;
        }
    }
    private String loadBaseUrlFromGraph() {
        try (var tx = db.beginTx()) {
            var res = tx.execute("MATCH (j:Jaeger) RETURN j.baseUrl AS baseUrl LIMIT 1");
            if (res.hasNext()) {
                Object val = res.next().get("baseUrl");
                if (val != null) return val.toString();
            }
        }
        return null; // nothing found
    }
    private Integer tryExtractInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(v.toString()); } catch (Exception e) { return null; }
    }

    // Result DTO
    public static class TraceResult {
        public String traceId;
        public String service;
        public long spanCount;
        public String startTime;   // ISO string, nullable
        public long durationMs;
        public String traceJSON;

        public TraceResult(String traceId, String service, long spanCount, String startTime, long durationMs, String traceJSON) {
            this.traceId = traceId;
            this.service = service;
            this.spanCount = spanCount;
            this.startTime = startTime;
            this.durationMs = durationMs;
            this.traceJSON = traceJSON;
        }
    }
}
