/*Examples
Example 1:
CALL timegraph.data.get_traces_for_subgraph({
        operations: ["compose-post-service::compose_media_client","media-service::compose_media_server"],
        range: "-1h",
        limit: 1
        })
        YIELD traceId, spanCount, matchedOps, durationMs
        RETURN *;
Example 2:

MATCH (o:Operation)
WHERE o.service IN ["media-service", "compose-post-service"]
WITH collect(o) AS ops

CALL timegraph.data.get_traces_for_subgraph({
  operationNodes: ops,
  time: toString(datetime()),
  range: "-2h",
  limit: 300
})

YIELD traceId, spanCount, matchedOps, startTime, durationMs, traceJSON
RETURN *
ORDER BY startTime DESC;

 */
package util.trace.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import util.trace.datatype.datastructs.*;
import util.trace.datatype.datastructs.Process;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JaegerSubgraphToTraceProcedure {

    @Context public GraphDatabaseService db;
    @Context public Log log;

    private static final Gson gson = new Gson();

    @Procedure(name = "graphobs.data.get_traces_for_subgraph", mode = Mode.READ)
    @Description("Get traces touching a subgraph. Params: { operationNodes: [...], operations: [...], time, range, limit}")
    public Stream<SubgraphTraceResult> getTracesForSubgraph(
            @Name("params") Map<String, Object> params
    ) {
        try {
            if (params == null) params = Map.of();

            Set<String> operationIds = new HashSet<>();

            // operationNodes: LIST<Node>
            if (params.containsKey("operationNodes")) {
                Object raw = params.get("operationNodes");
                if (raw instanceof List<?>) {
                    for (Object obj : (List<?>) raw) {
                        if (obj instanceof Node) {
                            Node n = (Node) obj;
                            Object idVal = n.getProperty("id", null);
                            if (idVal != null) {
                                operationIds.add(idVal.toString());
                            }
                        }
                    }
                }
            }

            // operations: LIST<String>
            if (params.containsKey("operations")) {
                Object rawOps = params.get("operations");
                if (rawOps instanceof List<?>) {
                    for (Object o : (List<?>) rawOps) {
                        if (o != null) operationIds.add(o.toString());
                    }
                }
            }

            if (operationIds.isEmpty()) {
                log.warn("get_traces_for_subgraph: no operations provided (no operationNodes, no operations)");
                return Stream.empty();
            }
            //derive services from operationIds
            Set<String> services = operationIds.stream()
                    .map(op -> {
                        int idx = op.indexOf("::");
                        return idx > 0 ? op.substring(0, idx) : null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            //parse time/range
            Instant endInstant = Instant.now();
            if (params.containsKey("time")) {
                Object t = params.get("time");
                try {
                    if (t instanceof Number)
                        endInstant = Instant.ofEpochMilli(((Number) t).longValue());
                    else
                        endInstant = Instant.parse(t.toString());
                } catch (Exception ignore) {}
            }

            String rangeStr = Objects.toString(params.getOrDefault("range", "-1h"), "-1h");
            long rangeMillis = parseRangeToMillis(rangeStr);
            Instant startInstant = endInstant.minusMillis(rangeMillis);

            int limit = params.containsKey("limit") ? ((Number) params.get("limit")).intValue() : 200;
            String baseUrl = loadBaseUrlFromGraph();
            if (baseUrl == null || baseUrl.isBlank()) {
                log.error("No Jaeger baseUrl found in graph. Create (Jaeger {baseUrl:'...'}) first.");
                return Stream.empty();
            }


            long startMicros = startInstant.toEpochMilli() * 1000L;
            long endMicros = endInstant.toEpochMilli() * 1000L;

            // Query Jaeger

            HttpClient client = HttpClient.newBuilder().build();

            List<SubgraphTraceResult> results = new ArrayList<>();
            Set<String> seenTraceIds = new HashSet<>();

            for (String svc : services) {
                String url = baseUrl + "/traces?service="
                        + URLEncoder.encode(svc, StandardCharsets.UTF_8)
                        + "&start=" + startMicros
                        + "&end=" + endMicros
                        + "&limit=" + limit;

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Accept", "application/json")
                        .GET()
                        .build();

                HttpResponse<String> resp;
                try { resp = client.send(req, HttpResponse.BodyHandlers.ofString()); }
                catch (Exception e) { continue; }

                if (resp.statusCode() < 200 || resp.statusCode() >= 300) continue;

                JaegerTraces jt = gson.fromJson(resp.body(), JaegerTraces.class);
                if (jt == null || jt.getData() == null) continue;

                for (Trace t : jt.getData()) {
                    if (t == null || t.getSpans() == null) continue;

                    // Map spans → service::operation
                    Set<String> traceOps = new HashSet<>();
                    Map<String,Process> processes = t.getProcesses() == null ? Map.of() : t.getProcesses();

                    for (Span sp : t.getSpans()) {
                        Process p = processes.get(sp.getProcessId());
                        String svcName = (p != null && p.getServiceName() != null) ? p.getServiceName() : "unknown-service";
                        String opName = sp.getOperationName() == null ? "unknown-operation" : sp.getOperationName();
                        traceOps.add(svcName + "::" + opName);
                    }

                    // MUST contain ALL required ops
                    boolean matches = traceOps.containsAll(operationIds);

                    if (matches && seenTraceIds.add(t.getTraceID())) {

                        int spanCount = t.getSpans().size();
                        long minStart = Long.MAX_VALUE;
                        long maxEnd = Long.MIN_VALUE;

                        for (Span sp : t.getSpans()) {
                            long s = sp.getStartTime() / 1000L; //jaeger operates in microseconds
                            long d = sp.getDuration() / 1000L;
                            minStart = Math.min(minStart, s);
                            maxEnd   = Math.max(maxEnd, s + d);
                        }

                        String startIso = minStart == Long.MAX_VALUE ? null : Instant.ofEpochMilli(minStart).toString();
                        long durationMs = (minStart == Long.MAX_VALUE) ? 0 : maxEnd - minStart;

                        ObjectMapper mapper = new ObjectMapper();
                        String traceJSON = mapper.writeValueAsString(t);

                        results.add(new SubgraphTraceResult(
                                t.getTraceID(),
                                spanCount,
                                new ArrayList<>(operationIds),
                                startIso,
                                durationMs,
                                traceJSON
                        ));
                    }
                }
            }

            return results.stream();

        } catch (Exception e) {
            log.error("get_traces_for_subgraph error: %s", e.getMessage());
            return Stream.empty();
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
    private static long parseRangeToMillis(String range) {
        if (range == null || range.isBlank()) return 3600_000L;
        String s = range.startsWith("-") ? range.substring(1) : range;
        try {
            if (s.endsWith("h")) return Long.parseLong(s.substring(0, s.length()-1)) * 3600_000L;
            if (s.endsWith("m")) return Long.parseLong(s.substring(0, s.length()-1)) * 60_000L;
            if (s.endsWith("s")) return Long.parseLong(s.substring(0, s.length()-1)) * 1000L;
            return Long.parseLong(s);
        } catch (Exception ex) {
            return 3600_000L;
        }
    }

    public static class SubgraphTraceResult {
        public String traceId;
        public long spanCount;
        public List<String> matchedOps;
        public String startTime;
        public long durationMs;
        public String traceJSON;

        public SubgraphTraceResult(String traceId, long spanCount, List<String> matchedOps, String startTime, long durationMs, String traceJSON) {
            this.traceId = traceId;
            this.spanCount = spanCount;
            this.matchedOps = matchedOps;
            this.startTime = startTime;
            this.durationMs = durationMs;
            this.traceJSON = traceJSON;
        }
    }
}

