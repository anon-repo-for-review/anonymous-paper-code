package util.trace.sources;


import com.google.gson.Gson;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import util.trace.datatype.datastructs.Dependency;
import util.trace.datatype.datastructs.DependencyData;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Stream;

/*
*
Exemplary:
CALL timegraph.jaeger.call_count_between(
  "compose-post-service",
  "media-service",
  {}
) YIELD parent, child, callCount
RETURN *;

CALL timegraph.jaeger.call_count_between(
  "compose-post-service",
  "media-service",
  { time: "2025-11-21T10:24:03.717Z", lookback: "6h" }
) YIELD parent, child, callCount
RETURN *;
* */

public class JaegerDependencyProcedure {

    @Context
    public Log log;

    private static final Gson gson = new Gson();

    @Procedure(name = "graphobs.jaeger.call_count_between", mode = Mode.READ)
    @Description("returning the callcount between two services. Params: { time, lookback, baseUrl }")
    public Stream<CallCountResult> callCountBetween(
            @Name("parent") String parentService,
            @Name("child") String childService,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        try {
            if (parentService == null || parentService.isBlank() || childService == null || childService.isBlank()) {
                log.warn("call_count_between: parent or child is empty");
                return Stream.empty();
            }

            // Defaults
            String defaultBase = "http://10.1.20.236:30212";
            String baseUrl = params != null && params.containsKey("baseUrl")
                    ? params.get("baseUrl").toString()
                    : defaultBase;

            long endTsMillis = parseTimeParam(params);
            long lookbackMillis = parseLookbackParam(params);

            // Build URL (jaeger expects millis here, according to your example)
            String url = baseUrl + "/api/dependencies?endTs=" + endTsMillis + "&lookback=" + lookbackMillis;
            log.info("Querying Jaeger dependencies: %s", url);

            HttpClient client = HttpClient.newBuilder().build();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                log.error("Jaeger dependencies HTTP error: status=%d body=%s", resp.statusCode(), snippet(resp.body(), 500));
                return Stream.empty();
            }

            Dependency dr = gson.fromJson(resp.body(), Dependency.class);
            if (dr == null || dr.getData() == null || dr.getData().isEmpty()) {
                log.info("No dependencies returned from jaeger (service=%s).", baseUrl);
                return Stream.of(new CallCountResult(parentService, childService, 0));
            }

            // Search for entry (exact match)
            for (DependencyData d : dr.getData()) {
                if (d == null) continue;
                if (parentService.equals(d.getParent()) && childService.equals(d.getChild())) {
                    return Stream.of(new CallCountResult(d.getParent(), d.getChild(), d.getCallCount()));
                }
            }

            // not found -> return 0
            return Stream.of(new CallCountResult(parentService, childService, 0));

        } catch (Exception e) {
            log.error("Exception in call_count_between: %s", e.getMessage());
            return Stream.of(new CallCountResult(parentService, childService, 0));
        }
    }

    //Util functions:

    private static String snippet(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    // parse "time" param: number (epoch ms) or ISO string; default = now
    private long parseTimeParam(Map<String, Object> params) {
        Instant now = Instant.now();
        if (params == null || !params.containsKey("time")) return now.toEpochMilli();
        Object t = params.get("time");
        if (t instanceof Number) {
            return ((Number) t).longValue();
        } else {
            String s = t.toString();
            try {
                // try ISO
                Instant inst = Instant.parse(s);
                return inst.toEpochMilli();
            } catch (DateTimeParseException ex) {
                // try as long string
                try {
                    return Long.parseLong(s);
                } catch (Exception e) {
                    return now.toEpochMilli();
                }
            }
        }
    }

    // parse "lookback": millis OR strings like "1h","-1h","30m","60s"; default 1h
    private long parseLookbackParam(Map<String, Object> params) {
        long defaultMillis = 3600_000L; // 1h
        if (params == null || !params.containsKey("lookback")) return defaultMillis;
        Object lb = params.get("lookback");
        if (lb instanceof Number) {
            return Math.abs(((Number) lb).longValue());
        } else {
            String s = lb.toString().trim();
            if (s.startsWith("-")) s = s.substring(1);
            try {
                if (s.endsWith("h")) {
                    long n = Long.parseLong(s.substring(0, s.length()-1));
                    return Math.abs(n * 3600_000L);
                } else if (s.endsWith("m")) {
                    long n = Long.parseLong(s.substring(0, s.length()-1));
                    return Math.abs(n * 60_000L);
                } else if (s.endsWith("s")) {
                    long n = Long.parseLong(s.substring(0, s.length()-1));
                    return Math.abs(n * 1000L);
                } else {
                    // treat as millis or minutes (if small)
                    long v = Long.parseLong(s);
                    // Heuristik: > 1000 => millis, else minutes
                    if (Math.abs(v) > 1000L) return Math.abs(v);
                    return Math.abs(v) * 60_000L;
                }
            } catch (Exception ex) {
                return defaultMillis;
            }
        }
    }

    // DTO: Rückgabe
    public static class CallCountResult {
        public String parent;
        public String child;
        public long callCount;

        public CallCountResult(String parent, String child, long callCount) {
            this.parent = parent;
            this.child = child;
            this.callCount = callCount;
        }
    }
}
