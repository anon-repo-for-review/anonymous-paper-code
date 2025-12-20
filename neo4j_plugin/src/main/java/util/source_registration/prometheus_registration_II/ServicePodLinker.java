package util.source_registration.prometheus_registration_II;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.Map;


public class ServicePodLinker {

    private final GraphDatabaseService db;
    private final Log log;

    public ServicePodLinker(GraphDatabaseService db, Log log) {
        this.db = db;
        this.log = log;
    }

    public void linkServicesAndPods() {
        // ÄNDERUNG: 'CONTAINS' statt 'STARTS WITH'
        // Da der Pod 'socialnetwork_media-service_1' heißt und der Service 'media-service',
        // müssen wir prüfen, ob der Service-Name im Pod-Namen enthalten ist.
        /*String query =
                "MATCH (s:Service), (p:Pod) " +
                        "WHERE NOT (s)-[:HAS_POD]->(p) " +
                        "AND p.name CONTAINS s.name " +
                        "MERGE (s)-[:HAS_POD]->(p) " +
                        "RETURN count(*) as createdRels";*/
        String query =
                "MATCH (s:Service), (p:Pod) " +
                        "WHERE p.name STARTS WITH (s.name + '-') " +


                        "WITH p, s " +
                        "ORDER BY size(s.name) DESC " +

                        "WITH p, collect(s) as potentialServices " +
                        "WITH p, head(potentialServices) as bestService " +

                        "MERGE (bestService)-[:HAS_POD]->(p) " +
                        "RETURN count(*) as createdRels";


        try (Transaction tx = db.beginTx()) {
            long count = 0;
            org.neo4j.graphdb.Result res = tx.execute(query);
            if (res.hasNext()) {
                Map<String, Object> row = res.next();
                Object val = row.get("createdRels");
                if (val instanceof Number) {
                    count = ((Number) val).longValue();
                }
            }
            tx.commit();

            if (count > 0) {
                log.info("ServicePodLinker: Linked " + count + " Services to Pods using CONTAINS strategy.");
            } else {
                log.info("ServicePodLinker: No new links created.");
            }
        } catch (Exception e) {
            log.error("ServicePodLinker: Error while linking services and pods", e);
        }

        linkServicesAndOperationsToPrometheus();
    }


    public void linkServicesAndOperationsToPrometheus() {
        try (Transaction tx = db.beginTx()) {
            // Verbinde alle Services und Operations mit dem Prometheus Knoten
            // Voraussetzung: Die Namen stimmen überein oder sind über Labels auflösbar
            String q = "MATCH (x), (p:Prometheus) " +
                    "WHERE (x:Service OR x:Operation) " +
                    "MERGE (x)-[:HAS_TIME_SERIES]->(p)";
            tx.execute(q);
            tx.commit();
        }
    }
}