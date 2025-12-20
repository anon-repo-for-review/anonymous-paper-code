/* Example:
CALL timegraph.data.cypher_for_trace_subgraph_visualize("1733d3c388b1faa1")
YIELD cypher, nodes, relationships
RETURN *;
 */
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


public class JaegerTraceToSubgraphProcedure {

    @Context public Log log;
    @Context public GraphDatabaseService db;
    private static final Gson gson = new Gson();

    @Procedure(name = "timegraph.data.cypher_for_trace_subgraph_visualize_old", mode = Mode.READ)
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
            // Extract services/operations

            Map<String, Process> processes = t.getProcesses() == null ? Collections.emptyMap() : t.getProcesses();

            Set<String> services = new LinkedHashSet<>();
            Set<String> operations = new LinkedHashSet<>();
            List<String[]> dependencies = new ArrayList<>();

            Map<String,String> spanToOpId = new HashMap<>();

            for (Span sp : t.getSpans()) {
                if (sp == null) continue;

                Process p = processes.get(sp.getProcessId());
                String service = (p != null && p.getServiceName() != null)
                        ? p.getServiceName()
                        : "unknown-service";

                String opName = sp.getOperationName() == null
                        ? "unknown-operation"
                        : sp.getOperationName();

                String opId = service + "::" + opName;

                services.add(service);
                operations.add(opId);
                spanToOpId.put(sp.getSpanID(), opId);
            }

            // collect dependencies

            Set<String> depSeen = new HashSet<>();

            for (Span sp : t.getSpans()) {
                if (sp == null || sp.getReferences() == null) continue;

                String childOp = spanToOpId.get(sp.getSpanID());
                if (childOp == null) continue;

                for (Reference r : sp.getReferences()) {
                    if (!"CHILD_OF".equals(r.getRefType())) continue;

                    String parentOp = spanToOpId.get(r.getSpanID());
                    if (parentOp == null) continue;

                    String key = parentOp + "->" + childOp;
                    if (depSeen.add(key)) {
                        dependencies.add(new String[]{ parentOp, childOp });
                    }
                }
            }
            // Build Cypher
            StringBuilder cypher = new StringBuilder();
            cypher.append("MATCH ");

            List<String> matchParts = new ArrayList<>();

            // Service to Operation edges
            for (String op : operations) {
                int idx = op.indexOf("::");
                if (idx <= 0) continue;

                String service = op.substring(0, idx);
                String opName = op.substring(idx + 2);

                matchParts.add(
                        String.format("(s_%s:Service {id:'%s'})-[s_%s_o_%s:HAS_OPERATION]->(o_%s:Operation {id:'%s'})",
                                safeName(service), service, safeName(service), safeName(op), safeName(op), op)
                );
            }

            // Operation dependencies
            for (String[] dep : dependencies) {
                String from = dep[0];
                String to = dep[1];
                matchParts.add(
                        String.format("(o_%s)-[o_%s_o_%s:DEPENDS_ON]->(o_%s)",
                                safeName(from), safeName(from), safeName(to), safeName(to))
                );
            }

            // join match parts
            if (matchParts.isEmpty()) {
                // nothing to match -> return empty result but still give cypher
                String emptyCy = "/* no nodes/edges derived from trace */";
                return Stream.of(new VizResult(emptyCy, Collections.emptyList(), Collections.emptyList()));
            }
            cypher.append(String.join(",\n      ", matchParts));
            cypher.append("\nRETURN *;");

            String cypherStr = cypher.toString();

            // Execute the constructed cypher and collect Nodes/Relationships

            Transaction tx = db.beginTx();
            Result result = tx.execute(cypherStr);

            // collect unique nodes/relationships
            LinkedHashSet<Node> nodeSet = new LinkedHashSet<>();
            LinkedHashSet<Relationship> relSet = new LinkedHashSet<>();

            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                for (Object v : row.values()) {
                    collectElements(v, nodeSet, relSet);
                }
            }
            // close transaction
            tx.close();

            List<Node> nodes = new ArrayList<>(nodeSet);
            List<Relationship> rels = new ArrayList<>(relSet);

            return Stream.of(new VizResult(cypherStr, nodes, rels));

        } catch (Exception e) {
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
        return null; // nothing found
    }

    //The following method was written with the support of OpenAI's model ChatGPT 5.1 to handle different less common types of Cypher/Neo4j objects besides Node/Relationship (although they do not seem to be relevant for this concrete database in general)
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

    //useful function: cypher does not seem to accept other symbols in variable names besides letters, numbers, and potentially low lines _
    private static String safeName(String id) {
        return id.replaceAll("[^A-Za-z0-9]", "_");
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

