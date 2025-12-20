package util.source_registration.prometheus_registration_II;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import java.time.ZonedDateTime;
import java.util.*;

public class Neo4jPodRepository implements PodRepository {
    private final GraphDatabaseService db;

    public Neo4jPodRepository(GraphDatabaseService db) { this.db = db; }

    @Override
    public long ensurePrometheusNode(String url, List<String> metricNames) {
        Map<String,Object> params = new HashMap<>();
        params.put("url", url);
        params.put("names", metricNames);
        try (Transaction tx = db.beginTx()) {
            Result r = tx.execute("MERGE (pr:Prometheus {url:$url}) SET pr.names=$names RETURN id(pr) AS id", params);
            long id = r.hasNext() ? (long) r.next().get("id") : -1;
            tx.commit();
            return id;
        }
    }

    @Override
    public void createOrUpdateServers(List<Map<String, Object>> servers) {
        if (servers == null || servers.isEmpty()) return;
        try (Transaction tx = db.beginTx()) {
            Map<String,Object> params = new HashMap<>();
            params.put("rows", servers);
            String q = "UNWIND $rows AS r MERGE (s:Server:time_period {name:r.name}) " +
                    "SET s.start = r.start, s.end = r.end";
            tx.execute(q, params);
            tx.commit();
        }
    }

    /*@Override
    public void upsertPodsBulk(List<Map<String, Object>> rows, String prometheusUrl) {
        if (rows == null || rows.isEmpty()) return;
        try (Transaction tx = db.beginTx()) {
            Map<String,Object> params = new HashMap<>();
            params.put("rows", rows);
            params.put("url", prometheusUrl);
            String q = "UNWIND $rows AS r MERGE (p:Pod:time_period {name:r.name}) " +
                    "SET p.start = r.start, p.end = r.end, p.status = coalesce(r.status,'RUNNING') " +
                    "WITH p MATCH (pr:Prometheus {url:$url}) MERGE (p)-[:HAS_TIME_SERIES]->(pr)";
            tx.execute(q, params);
            tx.commit();
        }
    }*/

    @Override
    public Map<String, ZonedDateTime> findPodEndsForPrometheus(String prometheusUrl) {
        Map<String,ZonedDateTime> out = new HashMap<>();
        String q = "MATCH (p:Pod:time_period)-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) RETURN p.name as name, p.end as end";
        Map<String,Object> params = Collections.singletonMap("url", prometheusUrl);
        try (Transaction tx = db.beginTx()) {
            Result r = tx.execute(q, params);
            while (r.hasNext()) {
                Map<String,Object> row = r.next();
                Object end = row.get("end");
                if (end != null) {
                    try {
                        out.put((String)row.get("name"), ZonedDateTime.parse(end.toString()));
                    } catch (Exception ex) {
                        // ignore malformed
                    }
                } else {
                    out.put((String)row.get("name"), null);
                }
            }
            tx.commit();
        }
        return out;
    }

    @Override
    public void markPodsStopped(List<String> podNames, String prometheusUrl) {
        if (podNames == null || podNames.isEmpty()) return;
        try (Transaction tx = db.beginTx()) {
            Map<String,Object> params = new HashMap<>();
            params.put("names", podNames);
            params.put("url", prometheusUrl);
            String q = "UNWIND $names AS n MATCH (p:Pod:time_period {name:n})-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) SET p.status = 'STOPPED'";
            tx.execute(q, params);
            tx.commit();
        }
    }

    @Override
    public void linkPodsToPrometheus(String prometheusUrl) {
        Map<String,Object> param = Collections.singletonMap("url", prometheusUrl);
        try (Transaction tx = db.beginTx()) {
            String q = "MATCH (p:Pod:time_period), (pr:Prometheus {url:$url}) MERGE (p)-[:HAS_TIME_SERIES]->(pr)";
            tx.execute(q, param);
            tx.commit();
        }
    }


    @Override
    public void upsertPodsBulk(List<Map<String, Object>> rows, String prometheusUrl) {
        if (rows == null || rows.isEmpty()) return;
        try (Transaction tx = db.beginTx()) {
            Map<String,Object> params = new HashMap<>();
            params.put("rows", rows);
            params.put("url", prometheusUrl);

            String q = ""
                    + "UNWIND $rows AS r\n"
                    + "MERGE (p:Pod:time_period {name:r.name})\n"
                    + "SET p.start = coalesce(r.start, p.start, r.end),\n"
                    + "    p.end   = coalesce(r.end, p.end),\n"
                    + "    p.status = coalesce(r.status, p.status, 'RUNNING')\n"
                    + "WITH p\n"
                    + "MATCH (pr:Prometheus {url:$url})\n"
                    + "MERGE (p)-[:HAS_TIME_SERIES]->(pr)";
            tx.execute(q, params);
            tx.commit();
        }
    }


    @Override
    public void upsertServersBulk(List<Map<String, Object>> rows, String prometheusUrl) {
        if (rows == null || rows.isEmpty()) return;
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> params = new HashMap<>();
            params.put("rows", rows);
            params.put("url", prometheusUrl);

            // Mergt Server Knoten mit Label Server und time_period
            // Verknüpft sie auch direkt mit Prometheus, falls gewünscht (analog zu Pods)
            String q = ""
                    + "UNWIND $rows AS r\n"
                    + "MERGE (s:Server:time_period {name:r.name})\n"
                    + "SET s.start = coalesce(r.start, s.start, r.end),\n"
                    + "    s.end   = coalesce(r.end, s.end),\n"
                    + "    s.status = coalesce(r.status, s.status, 'RUNNING')\n"
                    + "WITH s\n"
                    + "MATCH (pr:Prometheus {url:$url})\n"
                    + "MERGE (s)-[:HAS_TIME_SERIES]->(pr)";
            tx.execute(q, params);
            tx.commit();
        }
    }

    @Override
    public void linkPodsToServers(Map<String, String> podToServerMap) {
        if (podToServerMap == null || podToServerMap.isEmpty()) return;

        // Umwandeln in Liste von Maps für UNWIND
        List<Map<String, String>> list = new ArrayList<>();
        podToServerMap.forEach((pod, srv) -> {
            Map<String, String> m = new HashMap<>();
            m.put("pod", pod);
            m.put("server", srv);
            list.add(m);
        });

        try (Transaction tx = db.beginTx()) {
            Map<String, Object> params = Collections.singletonMap("data", list);
            String q = ""
                    + "UNWIND $data AS row\n"
                    + "MATCH (p:Pod {name: row.pod})\n"
                    + "MATCH (s:Server {name: row.server})\n"
                    + "MERGE (p)-[:DEPLOYED_ON]->(s)";
            tx.execute(q, params);
            tx.commit();
        }
    }

    @Override
    public Map<String, ZonedDateTime> findServerEndsForPrometheus(String prometheusUrl) {
        // Identisch zur Pod Logik, nur Label :Server
        Map<String,ZonedDateTime> out = new HashMap<>();
        String q = "MATCH (s:Server:time_period)-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) RETURN s.name as name, s.end as end";
        Map<String,Object> params = Collections.singletonMap("url", prometheusUrl);
        try (Transaction tx = db.beginTx()) {
            Result r = tx.execute(q, params);
            while (r.hasNext()) {
                Map<String,Object> row = r.next();
                Object end = row.get("end");
                if (end != null) {
                    try { out.put((String)row.get("name"), ZonedDateTime.parse(end.toString())); }
                    catch (Exception ex) { /*ignore*/ }
                } else {
                    out.put((String)row.get("name"), null);
                }
            }
            tx.commit();
        }
        return out;
    }

    @Override
    public void markServersStopped(List<String> names, String prometheusUrl) {
        if (names == null || names.isEmpty()) return;
        try (Transaction tx = db.beginTx()) {
            Map<String,Object> params = new HashMap<>();
            params.put("names", names);
            params.put("url", prometheusUrl);
            String q = "UNWIND $names AS n MATCH (s:Server:time_period {name:n})-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) SET s.status = 'STOPPED'";
            tx.execute(q, params);
            tx.commit();
        }
    }
}

