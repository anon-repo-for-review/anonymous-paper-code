package structural_search;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

public class OwnershipLookup {

    @Context
    public GraphDatabaseService db;

    public static class ServiceResult {
        public Node service;

        public ServiceResult(Node service) {
            this.service = service;
        }
    }

    @Procedure(name = "graphobs.search.get_owning_service_by_name", mode = Mode.READ)
    @Description("Finds the closest owning Service for a given node name, based on outgoing relationships from the Service to the node")
    public Stream<ServiceResult> getOwningService(@Name("nodeName") String nodeName) {

        String query =
                "MATCH (target {name: $nodeName}) " +
                        "MATCH path = (s:Service)-[*]->(target) " +
                        "RETURN s AS service " +
                        "ORDER BY length(path) ASC " +
                        "LIMIT 1";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("nodeName", nodeName));
        return result.stream()
                .map(row -> new ServiceResult((Node) row.get("service")))
                .onClose(tx::close);
    }


    @Procedure(name = "graphobs.search.get_owning_service", mode = Mode.READ)
    @Description("Finds the closest owning Service for a given node")
    public Stream<ServiceResult> getOwningService(@Name("targetNode") Node targetNode) {

        String query =
                "MATCH (target) WHERE elementId(target) = $elementId " +
                        "MATCH path = (s:Service)-[*]->(target) " +
                        "RETURN s AS service " +
                        "ORDER BY length(path) " +
                        "LIMIT 1";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("elementId", targetNode.getElementId()));
        return result.stream()
                    .map(row -> new ServiceResult((Node) row.get("service")))
                    .onClose(tx::close);

    }
}
