package structural_search;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ServiceCallChainByOperation {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    // Ergebnis-Typ
    public static class ServiceResult {
        public Node service;

        public ServiceResult(Node service) {
            this.service = service;
        }
    }

    @Procedure(name = "graphobs.search.get_calling_services_from_operation", mode = Mode.READ)
    @Description("Find services that call a specific operation (by name + service) within maxSteps hops")
    public Stream<ServiceResult> getCallingServicesFromOperation(
            @Name("serviceName") String serviceName,
            @Name("operationName") String operationName,
            @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {

        String query =
                "MATCH (targetService:Service {name: $serviceName})-[:HAS_OPERATION]->(targetOp:Operation {name: $operationName}) " +
                        "MATCH (caller:Service)-[:HAS_OPERATION]->(:Operation)-[:DEPENDS_ON*0.." + maxSteps + "]->(targetOp) " +
                        "RETURN DISTINCT caller";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName, "operationName", operationName));
        return result.stream()
                .map(row -> new ServiceResult((Node) row.get("caller")))
                .onClose(tx::close);
    }


    /*@Procedure(name = "timegraph.search.getCalledServicesFromOperation", mode = Mode.READ)
    @Description("Find services called by a specific operation (by name + service) within maxSteps hops")
    public Stream<ServiceResult> getCalledServicesFromOperation(
            @Name("serviceName") String serviceName,
            @Name("operationName") String operationName,
            @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {

        String query =
                "MATCH (sourceService:Service {name: $serviceName})-[:HAS_OPERATION]->(sourceOp:Operation {name: $operationName}) " +
                        "MATCH (sourceOp)-[:DEPENDS_ON*0.." + maxSteps + "]->(:Operation)<-[:HAS_OPERATION]-(called:Service) " +
                        "WITH DISTINCT called " +
                        "RETURN called";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName, "operationName", operationName));

        // Fügen Sie hier Logging hinzu, um die Einzigartigkeit zu überprüfen
        return result.stream()
                .peek(row -> {
                    Node node = (Node) row.get("called");
                    // Verwenden Sie Ihren bevorzugten Logger oder System.out zur Fehlersuche
                    log.info("Processing Node ID: " + node.getElementId() + ", Properties: " + node.getAllProperties());
                })
                .map(row -> new ServiceResult((Node) row.get("called")))
                .onClose(tx::close);
    }*/



    @Procedure(name = "graphobs.search.get_called_services_from_operation", mode = Mode.READ)
    @Description("Find services called by a specific operation (by name + service) within maxSteps hops")
    public Stream<ServiceResult> getCalledServicesFromOperation(
            @Name("serviceName") String serviceName,
            @Name("operationName") String operationName,
            @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {

        String query =
                "MATCH (sourceService:Service {name: $serviceName})-[:HAS_OPERATION]->(sourceOp:Operation {name: $operationName}) " +
                        "MATCH (sourceOp)-[:DEPENDS_ON*0.." + maxSteps + "]->(:Operation)<-[:HAS_OPERATION]-(called:Service) " +
                        // Anstatt zu versuchen, die Zeilen eindeutig zu machen, sammeln wir alle gefundenen Knoten
                        // und erstellen eine Liste, die nur die eindeutigen Knoten enthält.
                        "RETURN collect(DISTINCT called) AS services";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName, "operationName", operationName));

        // Die Abfrage gibt jetzt nur noch EINE Zeile zurück. Diese Zeile enthält die Liste.
        // Wir müssen diese Liste aus der einen Zeile extrahieren und in einen Stream umwandeln.
        return result.stream()
                .flatMap(row -> ((List<Node>) row.get("services")).stream())
                .map(ServiceResult::new)
                .onClose(tx::close);
    }


    /*@Procedure(name = "timegraph.search.getCalledServicesFromOperation", mode = Mode.READ)
    @Description("Find services called by a specific operation (by name + service) within maxSteps hops")
    public Stream<ServiceResult> getCalledServicesFromOperation(
            @Name("serviceName") String serviceName,
            @Name("operationName") String operationName,
            @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {

        String query =
                // Diese Abfrage ist robust und korrekt. Sie sammelt alle eindeutigen Knoten in einer Liste.
                "MATCH (sourceService:Service {name: $serviceName})-[:HAS_OPERATION]->(sourceOp:Operation {name: $operationName}) " +
                        "MATCH (sourceOp)-[:DEPENDS_ON*0.." + maxSteps + "]->(:Operation)<-[:HAS_OPERATION]-(called:Service) " +
                        "RETURN collect(DISTINCT called) AS services";

        // Materialisieren Sie das Ergebnis innerhalb eines try-with-resources-Blocks,
        // um eine korrekte Transaktionsbehandlung sicherzustellen.
        try (Transaction tx = db.beginTx();
             Result result = tx.execute(query, Map.of("serviceName", serviceName, "operationName", operationName))) {

            // Prüfen, ob überhaupt ein Ergebnis zurückkam
            if (result.hasNext()) {
                // Holen Sie die eine Ergebniszeile
                var row = result.next();
                // Extrahieren Sie die Liste der Knoten
                List<Node> serviceNodes = (List<Node>) row.get("services");

                // Konvertieren Sie die Liste von Knoten in eine Liste von ServiceResult-Objekten.
                // Dies ist der entscheidende "Materialisierungs"-Schritt.
                List<ServiceResult> finalResults = serviceNodes.stream()
                        .map(ServiceResult::new)
                        .collect(Collectors.toList());

                // Schließen Sie die Transaktion explizit, BEVOR der Stream zurückgegeben wird.
                tx.commit();

                // Geben Sie einen neuen Stream von der fertigen In-Memory-Liste zurück.
                // Dieser Stream hat keine Verbindung mehr zur "live" Datenbank-Transaktion.
                return finalResults.stream();
            } else {
                tx.commit();
                return Stream.empty(); // Kein Ergebnis gefunden
            }
        }
    }*/




}
