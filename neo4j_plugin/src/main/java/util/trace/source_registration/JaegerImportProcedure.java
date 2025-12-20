/*
 Example:
 CALL timegraph.jaeger.import_graph( "http://10.1.20.236:30212") YIELD services, operations, dependencies, durationMs, message;
 */
package util.trace.source_registration;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import util.source_registration.prometheus_registration_II.ServicePodLinker;
import util.trace.datatype.datastructs.JaegerServices;
import util.trace.datatype.datastructs.JaegerTraces;
import util.trace.datatype.datastructs.Trace;
import util.trace.datatype.datastructs.Span;
import util.trace.datatype.datastructs.Process;
import util.trace.datatype.datastructs.Reference;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

public class JaegerImportProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private static final Gson gson = new Gson();

    @Procedure(name = "graphobs.jaeger.import_graph", mode = Mode.WRITE)
    @Description("Importiert Graph aus Jaeger. Findet API automatisch (auch unter /jaeger/ui/api).")
    public Stream<ImportResult> importTraces(
            @Name(value = "baseUrl", defaultValue = "http://10.1.20.236:30212") String baseUrl
    ) {
        long start = System.currentTimeMillis();

        if (baseUrl == null || baseUrl.isBlank()) {
            return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start, "baseUrl darf nicht leer sein"));
        }

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5)) // Timeout etwas kürzer für schnellere Probes
                .build();

        try {
            // 1. API Detection starten
            ApiDetectionResult detectionResult = detectApiAndFetchServices(client, baseUrl);

            if (!detectionResult.success) {
                return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start,
                        "Konnte Jaeger API nicht finden. Versuchte Pfade: " + detectionResult.attemptedPaths));
            }

            String apiBase = detectionResult.apiBase;
            List<String> servicesToQuery = detectionResult.services;

            log.info("Jaeger API gefunden: " + apiBase);

            if (servicesToQuery.isEmpty()) {
                return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start, "Keine Services gefunden via " + apiBase));
            }

            // 2. Traces abrufen (Logik bleibt gleich, nutzt jetzt die korrekte apiBase)
            List<Trace> allTraces = new ArrayList<>();
            for (String svc : servicesToQuery) {
                try {
                    //String tracesUrl = apiBase + "/traces?service=" + URLEncoder.encode(svc, StandardCharsets.UTF_8);
                    long lookbackHours = 1;
                    long startMicros = (System.currentTimeMillis() - (lookbackHours * 3600 * 1000)) * 1000;
                    String limit = "250";

                    String tracesUrl = apiBase + "/traces?service=" + URLEncoder.encode(svc, StandardCharsets.UTF_8)
                            + "&limit=" + limit
                            + "&start=" + startMicros;


                    HttpRequest tracesReq = HttpRequest.newBuilder()
                            .uri(URI.create(tracesUrl))
                            .timeout(Duration.ofSeconds(20))
                            .header("Accept", "application/json")
                            .GET()
                            .build();

                    HttpResponse<String> tracesResp = client.send(tracesReq, HttpResponse.BodyHandlers.ofString());
                    int tStatus = tracesResp.statusCode();
                    String tBody = tracesResp.body() == null ? "" : tracesResp.body();

                    if (tStatus < 200 || tStatus >= 300) {
                        log.warn("Jaeger traces HTTP error for service=" + svc + " status=" + tStatus);
                        continue;
                    }

                    try {
                        JaegerTraces jt = gson.fromJson(tBody, JaegerTraces.class);
                        if (jt != null && jt.getData() != null) {
                            allTraces.addAll(jt.getData());
                        }
                    } catch (JsonSyntaxException ex) {
                        log.error("JSON parse error for traces of service " + svc + ": " + ex.getMessage());
                    }
                } catch (IOException | InterruptedException e) {
                    log.error("HTTP error while fetching traces for service " + svc + ": " + e.getMessage());
                }
            }

            if (allTraces.isEmpty()) {
                return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start, "Keine Traces gefunden."));
            }

            // 3. Graph bauen (Services, Operations, Dependencies)
            Set<String> services = new HashSet<>();
            List<Map<String, Object>> operations = new ArrayList<>();
            Set<String> opIds = new HashSet<>();
            List<Map<String, String>> dependencies = new ArrayList<>();
            Set<String> dependencyPairs = new HashSet<>();

            for (Trace trace : allTraces) {
                if (trace == null || trace.getSpans() == null) continue;

                Map<String, String> spanIdToOpId = new HashMap<>();
                Map<String, Process> processes = trace.getProcesses() == null ? Collections.emptyMap() : trace.getProcesses();

                // Pass 1: Spans & Operations
                for (Span sp : trace.getSpans()) {
                    if (sp == null) continue;
                    String procId = sp.getProcessId();
                    String serviceName = "unknown-service";
                    if (procId != null && processes.containsKey(procId)) {
                        Process p = processes.get(procId);
                        if (p != null && p.getServiceName() != null) {
                            serviceName = p.getServiceName();
                        }
                    }

                    String operationName = sp.getOperationName() == null ? "unknown-operation" : sp.getOperationName();
                    String operationId = serviceName + "::" + operationName;

                    services.add(serviceName);

                    if (!opIds.contains(operationId)) {
                        Map<String, Object> op = new HashMap<>();
                        op.put("id", operationId);
                        op.put("name", operationName);
                        op.put("service", serviceName);
                        operations.add(op);
                        opIds.add(operationId);
                    }
                    spanIdToOpId.put(sp.getSpanID(), operationId);
                }

                // Pass 2: Dependencies
                for (Span sp : trace.getSpans()) {
                    if (sp == null || sp.getReferences() == null) continue;
                    String childOpId = spanIdToOpId.get(sp.getSpanID());
                    if (childOpId == null) continue;

                    for (Reference ref : sp.getReferences()) {
                        if (ref == null || !"CHILD_OF".equals(ref.getRefType())) continue;
                        String parentSpanId = ref.getSpanID();
                        if (parentSpanId == null) continue;
                        String parentOpId = spanIdToOpId.get(parentSpanId);
                        if (parentOpId == null) continue;

                        String pairKey = parentOpId + "->" + childOpId;
                        if (!dependencyPairs.contains(pairKey)) {
                            Map<String, String> dep = new HashMap<>();
                            dep.put("from", parentOpId);
                            dep.put("to", childOpId);
                            dependencies.add(dep);
                            dependencyPairs.add(pairKey);
                        }
                    }
                }
            }

            // 4. DB Write
            try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
                if (!services.isEmpty()) {
                    tx.execute("UNWIND $x AS s MERGE (n:Service {id: s}) ON CREATE SET n.name = s",
                            Map.of("x", new ArrayList<>(services)));
                }
                if (!operations.isEmpty()) {
                    tx.execute("UNWIND $x AS o MERGE (n:Operation {id: o.id}) ON CREATE SET n.name = o.name, n.service = o.service",
                            Map.of("x", operations));
                    tx.execute("UNWIND $x AS o MATCH (s:Service {id: o.service}), (n:Operation {id: o.id}) MERGE (s)-[:HAS_OPERATION]->(n)",
                            Map.of("x", operations));
                }
                if (!dependencies.isEmpty()) {
                    tx.execute("UNWIND $x AS d MATCH (a:Operation {id: d.from}), (b:Operation {id: d.to}) MERGE (a)-[:DEPENDS_ON]->(b)",
                            Map.of("x", dependencies));
                }
                tx.execute("MATCH (j:Jaeger) DETACH DELETE j", Collections.emptyMap());
                tx.execute("CREATE (j:Jaeger { baseUrl: $baseUrl })",
                        Map.of("baseUrl", apiBase));

                tx.commit();
            }

            // 5. Linker
            try {
                ServicePodLinker linker = new ServicePodLinker(db, log);
                linker.linkServicesAndPods();
            } catch (Exception e) {
                log.warn("Linker error: " + e.getMessage());
            }

            return Stream.of(new ImportResult(services.size(), operations.size(), dependencies.size(), System.currentTimeMillis() - start, "Success"));

        } catch (Exception e) {
            log.error("Fehler: " + e.getMessage(), e);
            return Stream.of(new ImportResult(0, 0, 0, System.currentTimeMillis() - start, "Fehler: " + e.getMessage()));
        }
    }

    /**
     * Diese Methode probiert ALLE wahrscheinlichen Varianten durch, inklusive des UI-Falls.
     */
    private ApiDetectionResult detectApiAndFetchServices(HttpClient client, String inputUrl) {
        // Entferne Trailing Slash für sauberes String-Bauen
        String rawUrl = inputUrl.replaceAll("/+$", "");

        // Wir nutzen ein LinkedHashSet, um die Reihenfolge der Versuche zu steuern
        // und Duplikate zu vermeiden.
        Set<String> candidates = new LinkedHashSet<>();

        // 1. Priorität: Genau das, was der User eingegeben hat + /api
        // Wenn du http://localhost:16686/jaeger/ui eingibst, wird hier http://localhost:16686/jaeger/ui/api getestet.
        candidates.add(rawUrl + "/api");

        // 2. Priorität: Falls der User nur http://localhost:16686 eingegeben hat,
        // müssen wir raten. Wir fügen explizit deinen UI-Pfad hinzu.
        candidates.add(rawUrl + "/jaeger/ui/api"); // <--- DAS LÖST DEIN PROBLEM
        candidates.add(rawUrl + "/jaeger/api");    // Standard Ingress

        // 3. Fallback: Versuch, den Host zu extrahieren, falls der User einen tiefen Pfad
        // eingegeben hat, der aber falsch war, und wir von "root" neu suchen wollen.
        try {
            URI uri = URI.create(rawUrl);
            String root = uri.getScheme() + "://" + uri.getAuthority(); // http://localhost:16686

            // Auch hier fügen wir wieder die Kandidaten basierend auf Root hinzu
            candidates.add(root + "/api");
            candidates.add(root + "/jaeger/ui/api"); // <--- Auch hier wichtig
            candidates.add(root + "/jaeger/api");
        } catch (IllegalArgumentException e) {
            // Ignorieren, wenn URL nicht parsebar
        }

        List<String> triedPaths = new ArrayList<>();

        for (String candidateApiBase : candidates) {
            triedPaths.add(candidateApiBase);
            try {
                // Der Endpunkt zum Testen ist immer /services
                String testUrl = candidateApiBase + "/services";

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(testUrl))
                        .timeout(Duration.ofSeconds(3)) // Fail fast
                        .header("Accept", "application/json")
                        .GET()
                        .build();

                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

                // Wenn wir JSON zurückbekommen und Status 200 ist, haben wir gewonnen
                if (resp.statusCode() == 200 && resp.body() != null) {
                    // Kurzer Check, ob es wirklich Jaeger JSON ist
                    JaegerServices js = gson.fromJson(resp.body(), JaegerServices.class);
                    if (js != null && js.getData() != null) {
                        return new ApiDetectionResult(true, candidateApiBase, js.getData(), triedPaths.toString());
                    }
                }
            } catch (Exception e) {
                // Fehler beim Verbinden -> Nächsten Kandidaten probieren
            }
        }

        return new ApiDetectionResult(false, null, Collections.emptyList(), triedPaths.toString());
    }

    private static class ApiDetectionResult {
        boolean success;
        String apiBase;
        List<String> services;
        String attemptedPaths;

        ApiDetectionResult(boolean success, String apiBase, List<String> services, String attemptedPaths) {
            this.success = success;
            this.apiBase = apiBase;
            this.services = services;
            this.attemptedPaths = attemptedPaths;
        }
    }

    public static class ImportResult {
        public long services;
        public long operations;
        public long dependencies;
        public long durationMs;
        public String message;

        public ImportResult(long services, long operations, long dependencies, long durationMs, String message) {
            this.services = services;
            this.operations = operations;
            this.dependencies = dependencies;
            this.durationMs = durationMs;
            this.message = message;
        }
    }
}
