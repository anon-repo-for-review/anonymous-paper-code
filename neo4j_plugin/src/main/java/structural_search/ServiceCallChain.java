package structural_search;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import javax.ws.rs.DefaultValue;
import java.util.Map;
import java.util.stream.Stream;

public class ServiceCallChain {

    // Neo4j API wird injiziert
    @Context
    public GraphDatabaseService db;

    // Ergebnis-Typ
    public static class ServiceResult {
        public Node service;

        public ServiceResult(Node service) {
            this.service = service;
        }
    }

    @Procedure(name = "graphobs.search.get_calling_services", mode = Mode.READ)
    @Description("Find all services that call the given service within maxSteps (logical hops)")
    public Stream<ServiceResult> getCallingServices(@Name("serviceName") String serviceName,
                                                    @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {
        String query =
                "MATCH (target:Service {name: $serviceName}) " +
                        "MATCH path = (caller:Service)-[:HAS_OPERATION]->(:Operation)-[:DEPENDS_ON*1.." + (maxSteps) + "]->(:Operation)<-[:HAS_OPERATION]-(target) " +
                        "RETURN DISTINCT caller";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName));
        return result.stream().map(row -> new ServiceResult((Node) row.get("caller"))).onClose(tx::close);
    }



    @Procedure(name = "graphobs.search.get_called_services", mode = Mode.READ)
    @Description("Find all services called by the given service within maxSteps (logical hops)")
    public Stream<ServiceResult> getCalledServices(@Name("serviceName") String serviceName,
                                                   @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {
        String query =
                "MATCH (source:Service {name: $serviceName}) " +
                        "MATCH path = (source)-[:HAS_OPERATION]->(:Operation)-[:DEPENDS_ON*1.." + maxSteps + "]->(:Operation)<-[:HAS_OPERATION]-(called:Service) " +
                        "RETURN DISTINCT called";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName));
        return result.stream()
                .map(row -> new ServiceResult((Node) row.get("called")))
                .onClose(tx::close);
    }
}