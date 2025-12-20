package structural_search;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

public class OperationCallChain {

    @Context
    public GraphDatabaseService db;

    public static class OperationResult {
        public Node operation;

        public OperationResult(Node operation) {
            this.operation = operation;
        }
    }

    // 1) Operationen, die eine bestimmte Operation (service + opName) aufrufen (rückwärts)
    @Procedure(name = "graphobs.search.get_calling_operations_from_operation", mode = Mode.READ)
    @Description("Find operations that call a specific operation within maxSteps")
    public Stream<OperationResult> getCallingOperationsFromOperation(
            @Name("serviceName") String serviceName,
            @Name("operationName") String operationName,
            @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {

        String query =
                "MATCH (targetService:Service {name: $serviceName})-[:HAS_OPERATION]->(targetOp:Operation {name: $operationName}) " +
                        "MATCH (callerOp:Operation)-[:DEPENDS_ON*1.." + maxSteps + "]->(targetOp) " +
                        "RETURN DISTINCT callerOp";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName, "operationName", operationName));
        return result.stream()
                .map(row -> new OperationResult((Node) row.get("callerOp")))
                .onClose(tx::close);
    }

    // 2) Operationen, die von einer bestimmten Operation aufgerufen werden (vorwärts)
    @Procedure(name = "graphobs.search.get_called_operations_from_operation", mode = Mode.READ)
    @Description("Find operations called by a specific operation within maxSteps")
    public Stream<OperationResult> getCalledOperationsFromOperation(
            @Name("serviceName") String serviceName,
            @Name("operationName") String operationName,
            @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {

        String query =
                "MATCH (sourceService:Service {name: $serviceName})-[:HAS_OPERATION]->(sourceOp:Operation {name: $operationName}) " +
                        "MATCH (sourceOp)-[:DEPENDS_ON*1.." + maxSteps + "]->(calledOp:Operation) " +
                        "RETURN DISTINCT calledOp";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName, "operationName", operationName));
        return result.stream()
                .map(row -> new OperationResult((Node) row.get("calledOp")))
                .onClose(tx::close);
    }

    // 3) Operationen, die irgendeine Operation eines Services aufrufen (rückwärts)
    @Procedure(name = "graphobs.search.get_calling_operations_from_service", mode = Mode.READ)
    @Description("Find operations that call any operation of a given service within maxSteps")
    public Stream<OperationResult> getCallingOperationsFromService(
            @Name("serviceName") String serviceName,
            @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {

        String query =
                "MATCH (targetService:Service {name: $serviceName})-[:HAS_OPERATION]->(targetOp:Operation) " +
                        "MATCH (callerOp:Operation)-[:DEPENDS_ON*0.." + maxSteps + "]->(targetOp) " +
                        "RETURN DISTINCT callerOp";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName));
        return result.stream()
                .map(row -> new OperationResult((Node) row.get("callerOp")))
                .onClose(tx::close);
    }

    // 4) Operationen, die von irgendeiner Operation eines Services aufgerufen werden (vorwärts)
    @Procedure(name = "graphobs.search.get_called_operations_from_service", mode = Mode.READ)
    @Description("Find operations called by any operation of a given service within maxSteps")
    public Stream<OperationResult> getCalledOperationsFromService(
            @Name("serviceName") String serviceName,
            @Name(value = "maxSteps", defaultValue = "2147483647") long maxSteps) {

        String query =
                "MATCH (sourceService:Service {name: $serviceName})-[:HAS_OPERATION]->(sourceOp:Operation) " +
                        "MATCH (sourceOp)-[:DEPENDS_ON*0.." + maxSteps + "]->(calledOp:Operation) " +
                        "RETURN DISTINCT calledOp";

        Transaction tx = db.beginTx();
        Result result = tx.execute(query, Map.of("serviceName", serviceName));
        return result.stream()
                .map(row -> new OperationResult((Node) row.get("calledOp")))
                .onClose(tx::close);
    }
}