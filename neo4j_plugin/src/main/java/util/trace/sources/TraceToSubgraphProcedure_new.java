package util.trace.sources;

import com.google.gson.Gson;
import org.neo4j.graphdb.*;
import util.trace.datatype.datastructs.*;
import util.trace.datatype.datastructs.Process;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

public class TraceToSubgraphProcedure_new {

    @Context public Log log;
    @Context public GraphDatabaseService db;
    private static final Gson gson = new Gson();

    @Procedure(name = "graphobs.data.cypher_for_trace_subgraph_visualize", mode = Mode.READ)
    @Description("Builds the cypher, executes it and returns the result plus nodes/relationships for visualization.")
    public Stream<VizResult> cypherForTrace(
            @Name("traceId") String traceId
    ) {
        try {
            if (traceId == null || traceId.isBlank()) {
                return Stream.of(new VizResult("ERROR: traceId empty.", Collections.emptyList(), Collections.emptyList()));
            }

            String baseUrl = loadBaseUrlFromGraph();
            if (baseUrl == null || baseUrl.isBlank()) {
                log.error("No Jaeger baseUrl found in graph. Create (Jaeger {baseUrl:'...'}) first.");
                return Stream.empty();
            }

            // --- 1. Fetching Data (HTTP part remains largely the same) ---
            String url = baseUrl + "/traces/" + traceId;
            log.info("Fetching trace: %s", url);

            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                return Stream.of(new VizResult("ERROR: Jaeger returned " + resp.statusCode(), Collections.emptyList(), Collections.emptyList()));
            }

            JaegerTraces jt = gson.fromJson(resp.body(), JaegerTraces.class);
            if (jt == null || jt.getData() == null || jt.getData().isEmpty()) {
                return Stream.of(new VizResult("ERROR: Trace not found", Collections.emptyList(), Collections.emptyList()));
            }

            Trace t = jt.getData().get(0);
            if (t.getSpans() == null) {
                return Stream.of(new VizResult("ERROR: Trace has no spans", Collections.emptyList(), Collections.emptyList()));
            }

            // --- 2. Efficient Data Collection ---
            // Instead of building a string, we collect IDs into Sets/Lists for parameters.

            Map<String, Process> processes = t.getProcesses() == null ? Collections.emptyMap() : t.getProcesses();
            Map<String, String> spanToOpId = new HashMap<>();

            Set<String> serviceIds = new HashSet<>();
            Set<String> operationIds = new HashSet<>();

            // For dependencies we use a List of Maps to pass to UNWIND
            Set<String> dependencyDedup = new HashSet<>();
            List<Map<String, String>> dependencyParams = new ArrayList<>();

            for (Span sp : t.getSpans()) {
                if (sp == null) continue;

                Process p = processes.get(sp.getProcessId());
                String service = (p != null && p.getServiceName() != null) ? p.getServiceName() : "unknown-service";
                String opName = sp.getOperationName() == null ? "unknown-operation" : sp.getOperationName();
                String opId = service + "::" + opName;

                serviceIds.add(service);
                operationIds.add(opId);
                spanToOpId.put(sp.getSpanID(), opId);
            }

            for (Span sp : t.getSpans()) {
                if (sp == null || sp.getReferences() == null) continue;
                String childOp = spanToOpId.get(sp.getSpanID());
                if (childOp == null) continue;

                for (Reference r : sp.getReferences()) {
                    if (!"CHILD_OF".equals(r.getRefType())) continue;
                    String parentOp = spanToOpId.get(r.getSpanID());
                    if (parentOp == null) continue;

                    String key = parentOp + "->" + childOp;
                    if (dependencyDedup.add(key)) {
                        Map<String, String> depMap = new HashMap<>();
                        depMap.put("from", parentOp);
                        depMap.put("to", childOp);
                        dependencyParams.add(depMap);
                    }
                }
            }

            // --- 3. Optimized Cypher Query ---
            // We use one static query with parameters.
            // This allows Neo4j to compile the plan once and use index lookups efficiently.

            String query =
                    "MATCH (s:Service) WHERE s.id IN $svcIds \n" +
                            "MATCH (o:Operation) WHERE o.id IN $opIds \n" +
                            "OPTIONAL MATCH (s)-[r1:HAS_OPERATION]->(o) \n" +
                            "WITH collect(distinct s) + collect(distinct o) AS nodes, collect(distinct r1) AS initialRels \n" +

                            // Fix 1: Handle empty dependency list (UNWIND [] kills the query otherwise)
                            "UNWIND (CASE WHEN size($deps) > 0 THEN $deps ELSE [null] END) AS dep \n" +

                            "OPTIONAL MATCH (from:Operation {id: dep.from})-[r2:DEPENDS_ON]->(to:Operation {id: dep.to}) \n" +

                            // Fix 2: Split Aggregation and List concatenation into two steps
                            "WITH nodes, initialRels, collect(distinct r2) AS depRels \n" +
                            "WITH nodes, initialRels + depRels AS allRels \n" +

                            "UNWIND nodes AS n \n" +
                            "UNWIND allRels AS r \n" +
                            "RETURN DISTINCT n, r";

            Map<String, Object> params = new HashMap<>();
            params.put("svcIds", new ArrayList<>(serviceIds));
            params.put("opIds", new ArrayList<>(operationIds));
            params.put("deps", dependencyParams);

            // --- 4. Execution ---

            Transaction tx = db.beginTx();
            Result result = tx.execute(query, params);

            LinkedHashSet<Node> nodeSet = new LinkedHashSet<>();
            LinkedHashSet<Relationship> relSet = new LinkedHashSet<>();

            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                // collectElements handles 'n' and 'r' (which might be null if OPTIONAL MATCH fails slightly, though UNWIND filters nicely)
                collectElements(row.get("n"), nodeSet, relSet);
                collectElements(row.get("r"), nodeSet, relSet);
            }
            tx.close();

            // Note: The returned "cypher" string is now the generic parameterized query.
            // If you absolutely need the literal string with IDs for some UI, you would have to manually format it,
            // but for performance, this is the way to go.
            return Stream.of(new VizResult(query, new ArrayList<>(nodeSet), new ArrayList<>(relSet)));

        } catch (Exception e) {
            e.printStackTrace(); // Good for server logs
            log.error("cypher_for_trace_subgraph_visualize: %s", e.getMessage());
            return Stream.of(new VizResult("ERROR: " + e.getMessage(), Collections.emptyList(), Collections.emptyList()));
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
        return null;
    }

    private static void collectElements(Object v, Set<Node> nodes, Set<Relationship> rels) {
        if (v == null) return;
        if (v instanceof Node) {
            nodes.add((Node) v);
            return;
        }
        if (v instanceof Relationship) {
            rels.add((Relationship) v);
            return;
        }
        if (v instanceof Path) {
            Path p = (Path) v;
            for (Node n : p.nodes()) nodes.add(n);
            for (Relationship r : p.relationships()) rels.add(r);
            return;
        }
        if (v instanceof Iterable) {
            for (Object it : (Iterable<?>) v) collectElements(it, nodes, rels);
            return;
        }
        if (v instanceof Map) {
            for (Object it : ((Map<?, ?>) v).values()) collectElements(it, nodes, rels);
            return;
        }
    }

    public static class VizResult {
        public String cypher;
        public List<Node> nodes;
        public List<Relationship> relationships;

        public VizResult(String cypher, List<Node> nodes, List<Relationship> relationships) {
            this.cypher = cypher;
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }
}