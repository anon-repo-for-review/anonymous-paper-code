package util.source_registration.prometheus_registration_II;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

public class PrometheusRegistrar {
    @Context public GraphDatabaseService db;
    @Context public Log log;

    public static class Output { public String message; public String url; public long podCount; public Output(String m,String u,long c){ message=m;url=u;podCount=c;} }

    @Procedure(name = "graphobs.datasources.register_prometheus", mode = Mode.WRITE)
    public Stream<Output> registerPrometheus(@Name("url") String url,
                                             @Name(value="options", defaultValue="{}") Map<String,Object> options) {
        try {
            PrometheusClient client = new PrometheusHttpClient(url, log);
            PodRepository repo = new Neo4jPodRepository(db);
            PrometheusRegistrationService svc = new PrometheusRegistrationService(client, repo, log);
            long count = svc.register(url);




            boolean doUpdate = (options != null && options.getOrDefault("update", null) instanceof Boolean) ? (Boolean) options.get("update") : false;

            if (doUpdate) {
                long interval = (options != null && options.get("intervalSeconds") instanceof Number) ? ((Number) options.get("intervalSeconds")).longValue() : 30L;
                PrometheusToNeo4jUpdater up = new PrometheusToNeo4jUpdater(client, repo, log, url, interval, db);
                up.startOrEnsureUpdater();
            }


            Thread.sleep(10_000);
            ServicePodLinker linker = new ServicePodLinker(db, log);
            linker.linkServicesAndPods();




            return Stream.of(new Output("registered", url, count));
        } catch (Exception e) {
            return Stream.of(new Output("error: " + e.getMessage(), url, 0));
        }
    }
}
