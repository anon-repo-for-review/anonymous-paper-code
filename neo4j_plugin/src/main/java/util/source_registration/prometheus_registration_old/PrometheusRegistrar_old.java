package util.source_registration.prometheus_registration_old;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import util.TimeSeriesUtil;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static util.source_registration.prometheus_registration_old.PrometheusClient_old.*;

/**
 * Neo4j procedure to register a Prometheus data source and create Pod/Server/Prometheus nodes,
 * with a background updater that refreshes Pod end timestamps every 30s.
 */

public class PrometheusRegistrar_old {

    // Neo4j DB access injected by Neo4j runtime
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    // Map to keep a scheduler per Prometheus URL (avoid starting multiple updaters for same URL)
    private static final ConcurrentMap<String, ScheduledExecutorService> SCHEDULERS = new ConcurrentHashMap<>();


    // Procedure output record
    public static class Output {

        public String message;


        public String prometheusUrl;

        public long podCount;

        public Output(String message, String url, long podCount) {
            this.message = message;
            this.prometheusUrl = url;
            this.podCount = podCount;
        }
    }

    @Procedure(name = "timegraph.datasources.register_prometheus_old", mode = Mode.WRITE)
    @Description("CALL timegraph.datasources.register_prometheus('http://localhost:9090/') - registers Pods/Servers from Prometheus and starts a 30s updater")
    public Stream<Output> registerPrometheus(@Name("url") String prometheusUrl,
                                             @Name(value = "options", defaultValue = "{}") Map<String, Object> options) {
        try {
            // 1) Get metric names (__name__ label values)
            boolean doUpdate =  true;
            long intervalSeconds = 30L;


            if (options != null) {
                doUpdate = options.getOrDefault("update", null) instanceof Boolean
                        ? (Boolean) options.get("update")
                        : true;

                Object intervalObj = options.getOrDefault("intervalSeconds", null);
                if (intervalObj instanceof Number) {
                    intervalSeconds = ((Number) intervalObj).longValue();
                }

            }





            List<String> metricNames = fetchMetricNames(prometheusUrl);



            // 2) Get pod names (try candidates)
            LabelValuesResult podLabelResult = fetchFirstAvailableLabelValuesAndLabel(prometheusUrl);
            List<String> podNames = podLabelResult.values;


            // 3) Get instance values (server host:port), then extract host part
            Set<String> servers = new LinkedHashSet<>();

            // Heuristic time_range: start = now - 30 days, end = now (ISO 8601).
            Instant now = Instant.now();
            Instant start = now.minus(30, ChronoUnit.DAYS);
            String isoNow = now.toString();
            String isoStart = start.toString();

            // Persist nodes & relationships
            long podCount = createNodesAndRelationships(prometheusUrl, metricNames, podNames, new ArrayList<>(servers), isoStart, isoNow);

            // Create or reuse a scheduled updater that updates Pod.end every 30s
            //startOrEnsureUpdater(prometheusUrl);
            if (doUpdate) {
                PrometheusToNeo4jUpdater_old Updater = new PrometheusToNeo4jUpdater_old(db, log, prometheusUrl, intervalSeconds);
                Updater.startOrEnsureUpdater();
            }

            return Stream.of(new Output("registered", prometheusUrl, podCount));

        } catch (Exception e) {
            return Stream.of(new Output("error: " + e.getMessage(), prometheusUrl, 0));
        }
    }

    // -----------------------
    // Neo4j persistence helpers
    // -----------------------
    private long createNodesAndRelationships(String prometheusUrl, List<String> metricNames,
                                             List<String> podNames, List<String> serverNames,
                                             String startIso, String endIso) {
        long podCount = 0;
        try (Transaction tx = db.beginTx()) {
            // Create Prometheus node (MERGE by url so re-calls are idempotent)
            Map<String,Object> params = new HashMap<>();
            params.put("url", prometheusUrl);
            params.put("names", metricNames);
            String createProm = "MERGE (pr:Prometheus {url:$url}) SET pr.names = $names RETURN id(pr) AS id";
            Result r = tx.execute(createProm, params);
            // create servers nodes
            for (String sName: serverNames) {
                Map<String,Object> p = new HashMap<>();
                p.put("name", sName);
                p.put("start", startIso);
                p.put("end", endIso);
                String q = "MERGE (s:Server:time_period {name:$name}) " +
                        "SET s.start = $start, s.end = $end";
                tx.execute(q, p);
            }
            // create pod nodes and DEPLOYED_ON relationships
            for (String pName: podNames) {
                TimeRange range = null;
                try {
                    LabelValuesResult labelRes = fetchFirstAvailableLabelValuesAndLabel(prometheusUrl);
                    range = PrometheusClient_old.getPodTimeRange(prometheusUrl, pName, labelRes.label ,log);
                } catch (Exception e) {
                    log.error(e.getMessage());
                    throw new RuntimeException(e);
                }

                //log.info("start: " + range.start + "  end: " + range.end);

                Map<String, Object> p = new HashMap<>();
                p.put("name", pName);
                p.put("start", String.valueOf(TimeSeriesUtil.parseToZonedDateTime(range.start))); // ändern -> echtes data time
                p.put("end", String.valueOf(TimeSeriesUtil.parseToZonedDateTime(range.end))); // ändern -> echtes data time
                // MERGE pod
                String qpod = "MERGE (p:Pod:time_period {name:$name}) " +
                        "SET p.start = $start, p.end = $end";


                //String qpod = "MERGE (p:Pod:time_period {name:$name}) " +
                //       "SET p.start = '2025-11-11T13:53:44Z', p.end = '2025-11-11T13:53:44Z'";
                tx.execute(qpod, p);



                //log.info("start 2: " + range.start + "  end 2: " + range.end);

                boolean linked = false;
                String maybeIp = extractIpFromName(pName); // try to extract ip-like substring
                if (maybeIp != null) {
                    Map<String,Object> map = new HashMap<>();
                    map.put("podName", pName);
                    map.put("serverName", maybeIp);
                    String qrel = "MATCH (p:Pod:time_period {name:$podName}), (s:Server:time_period {name:$serverName}) " +
                            "MERGE (p)-[:DEPLOYED_ON]->(s)";
                    tx.execute(qrel, map);
                    linked = true;
                }
                if (!linked && serverNames.size() == 1) {
                    Map<String,Object> map = new HashMap<>();
                    map.put("podName", pName);
                    map.put("serverName", serverNames.get(0));
                    String qrel = "MATCH (p:Pod:time_period {name:$podName}), (s:Server:time_period {name:$serverName}) " +
                            "MERGE (p)-[:DEPLOYED_ON]->(s)";
                    tx.execute(qrel, map);
                }
                podCount++;


            }

            // Connect all pods to the Prometheus node with HAS_TIME_SERIES
            String linkPodsToProm = "MATCH (p:Pod:time_period), (pr:Prometheus {url:$url}) " +
                    "MERGE (p)-[:HAS_TIME_SERIES]->(pr)";
            Map<String,Object> param = new HashMap<>();
            param.put("url", prometheusUrl);
            tx.execute(linkPodsToProm, param);

            tx.commit();
        }
        return podCount;
    }

    private static String extractIpFromName(String s) {
        if (s == null) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(?:\\d{1,3}\\.){3}\\d{1,3}").matcher(s);
        if (m.find()) return m.group(0);
        return null;
    }

    // -----------------------
    // Updater (runs every 30s) - enhanced to detect new pods, update running pods, and mark stopped ones
    // -----------------------
    /*private void startOrEnsureUpdater(String prometheusUrl) {
        SCHEDULERS.computeIfAbsent(prometheusUrl, url -> {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("prometheus-updater-" + Math.abs(url.hashCode()));
                return t;
            });

            // Configuration
            final long gracePeriodSeconds = 60 * 5; // 5 minutes grace for marking a pod stopped

            Runnable task = () -> {
                String nowIso = Instant.now().toString();
                long nowEpoch = Instant.now().getEpochSecond();
                try {
                    // 1) discover which pod label is available and list pods
                    LabelValuesResult labelRes = fetchFirstAvailableLabelValuesAndLabel(prometheusUrl, POD_LABEL_CANDIDATES);
                    String podLabel = labelRes.label;
                    List<String> promPods = labelRes.values != null ? labelRes.values : Collections.emptyList();

                    if (podLabel == null || promPods.isEmpty()) {
                        // nothing to do
                        return;
                    }

                    // 2) for each pod ask Prometheus for lastSeen (simple per-pod approach)
                    Map<String, String> podToLastSeenIso = new HashMap<>();
                    for (String pod : promPods) {
                        try {
                            Double lastSeenSec = getPodLastSeen(prometheusUrl, podLabel, pod);
                            if (lastSeenSec != null) {
                                String iso = Instant.ofEpochSecond(lastSeenSec.longValue()).toString();
                                podToLastSeenIso.put(pod, iso);
                            } else {
                                // fallback: use now as heartbeat if no metric but pod listed
                                // eventuell ändern
                                podToLastSeenIso.put(pod, nowIso);
                            }
                        } catch (Exception e) {
                            // on failure assume now (keep updater robust)
                            podToLastSeenIso.put(pod, nowIso);
                        }
                    }

                    // 3) Fetch current pods from DB that are linked to this Prometheus
                    Map<String, Map<String, Object>> dbPods = new HashMap<>();
                    try (Transaction tx = db.beginTx()) {
                        Map<String,Object> param = Collections.singletonMap("url", prometheusUrl);
                        String q = "MATCH (p:Pod:time_period)-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) RETURN p.name AS name, p.lastSeen AS lastSeen, p.end AS end";
                        Result res = tx.execute(q, param);
                        while (res.hasNext()) {
                            Map<String,Object> row = res.next();
                            String name = (String) row.get("name");
                            dbPods.put(name, row);
                        }
                        tx.commit();
                    }

                    Set<String> promSet = podToLastSeenIso.keySet();
                    Set<String> dbSet = dbPods.keySet();

                    // 4) Determine new pods and pods to update
                    List<Map<String,Object>> podsToCreate = new ArrayList<>();
                    List<Map<String,Object>> podsToUpdate = new ArrayList<>();

                    for (String pod : promSet) {
                        String lastSeenIso = podToLastSeenIso.get(pod);
                        if (!dbSet.contains(pod)) {
                            // new pod -> create with start=lastSeen, end=lastSeen, lastSeen
                            Map<String,Object> m = new HashMap<>();
                            m.put("name", pod);
                            m.put("start", lastSeenIso);
                            m.put("end", lastSeenIso);
                            m.put("lastSeen", lastSeenIso); // muss weg
                            podsToCreate.add(m);
                        } else {
                            // existing -> update lastSeen/end if newer
                            Map<String,Object> dbRow = dbPods.get(pod);
                            String dbLastSeen = dbRow.get("lastSeen") != null ? dbRow.get("lastSeen").toString() : null;
                            String dbEnd = dbRow.get("end") != null ? dbRow.get("end").toString() : null;

                            boolean shouldUpdate = false;
                            if (dbLastSeen == null) shouldUpdate = true; // weg
                            else {
                                try {
                                    Instant dbInst = Instant.parse(dbLastSeen);
                                    Instant newInst = Instant.parse(lastSeenIso);
                                    if (newInst.isAfter(dbInst)) shouldUpdate = true;
                                } catch (Exception e) {
                                    shouldUpdate = true; // if parsing fails, be conservative and update
                                }
                            }

                            if (shouldUpdate) {
                                Map<String,Object> m = new HashMap<>();
                                m.put("name", pod);
                                m.put("lastSeen", lastSeenIso);
                                podsToUpdate.add(m);
                            }
                        }
                    }

                    // 5) Create new pods in DB (batch)
                    if (!podsToCreate.isEmpty()) {
                        try (Transaction tx = db.beginTx()) {
                            Map<String,Object> p = Collections.singletonMap("rows", podsToCreate);
                            String create = "UNWIND $rows AS r MERGE (p:Pod:time_period {name:r.name}) " +
                                    "SET p.start = r.start, p.end = r.end, p.lastSeen = r.lastSeen, p.status = 'RUNNING' " +
                                    "WITH p MATCH (pr:Prometheus {url:$url}) MERGE (p)-[:HAS_TIME_SERIES]->(pr)";
                            Map<String,Object> params = new HashMap<>();
                            params.put("rows", podsToCreate);
                            params.put("url", prometheusUrl);
                            tx.execute(create, params);
                            tx.commit();
                        } catch (Exception e) {
                            log.error("Error creating pods: " + e.getMessage());
                        }
                    }

                    // 6) Update existing pods lastSeen/end
                    if (!podsToUpdate.isEmpty()) {
                        try (Transaction tx = db.beginTx()) {
                            Map<String,Object> params = new HashMap<>();
                            params.put("rows", podsToUpdate);
                            params.put("url", prometheusUrl);
                            String update = "UNWIND $rows AS r MATCH (p:Pod:time_period {name:r.name})-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) " +
                                    "SET p.lastSeen = r.lastSeen, p.end = r.lastSeen, p.status = 'RUNNING'";
                            tx.execute(update, params);
                            tx.commit();
                        } catch (Exception e) {
                            log.error("Error updating pods: " + e.getMessage());
                        }
                    }

                    // 7) Mark stopped pods (those in DB but not in promSet and not seen within grace period)
                    List<String> podsToStop = new ArrayList<>();
                    for (String dbPod : dbSet) {
                        if (!promSet.contains(dbPod)) {
                            Map<String,Object> row = dbPods.get(dbPod);
                            String lastSeen = row.get("lastSeen") != null ? row.get("lastSeen").toString() : null;
                            if (lastSeen != null) {
                                try {
                                    Instant lastSeenInst = Instant.parse(lastSeen);
                                    if (lastSeenInst.plusSeconds(gracePeriodSeconds).isBefore(Instant.ofEpochSecond(nowEpoch))) {
                                        podsToStop.add(dbPod);
                                    }
                                } catch (Exception e) {
                                    // Parsing problem -> be conservative and not stop
                                }
                            }
                        }
                    }

                    if (!podsToStop.isEmpty()) {
                        try (Transaction tx = db.beginTx()) {
                            Map<String,Object> params = new HashMap<>();
                            params.put("names", podsToStop);
                            params.put("url", prometheusUrl);
                            String stopQ = "UNWIND $names AS n MATCH (p:Pod:time_period {name:n})-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) " +
                                    "SET p.status = 'STOPPED' " +
                                    "FOREACH (ignoreMe IN CASE WHEN p.end IS NULL THEN [1] ELSE [] END | SET p.end = p.lastSeen)";
                            // Neo4j doesn't support SET inside FOREACH like this directly with that syntax in older versions —
                            // to be safe, do two statements: set status and then set end if null.
                            String setStatus = "UNWIND $names AS n MATCH (p:Pod:time_period {name:n})-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) SET p.status = 'STOPPED'";
                            tx.execute(setStatus, params);
                            String setEndIfNull = "UNWIND $names AS n MATCH (p:Pod:time_period {name:n})-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) " +
                                    "WITH p WHERE p.end IS NULL SET p.end = p.lastSeen";
                            tx.execute(setEndIfNull, params);
                            tx.commit();
                        } catch (Exception e) {
                            log.error("Error stopping pods: " + e.getMessage());
                        }
                    }

                } catch (Exception e) {
                    // keep scheduler alive
                    log.error("Prometheus updater for " + prometheusUrl + " failed: " + e.getMessage());
                }
            };
            // start immediately, then every 30 seconds
            scheduler.scheduleAtFixedRate(task, 0L, 30L, TimeUnit.SECONDS);
            return scheduler;
        });
    }*/
}









/*public class PrometheusRegistrar {

    // Neo4j DB access injected by Neo4j runtime
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    // Map to keep a scheduler per Prometheus URL (avoid starting multiple updaters for same URL)
    private static final ConcurrentMap<String, ScheduledExecutorService> SCHEDULERS = new ConcurrentHashMap<>();

    // Labels to try for pod name (fallbacks)
    /*private static final String[] POD_LABEL_CANDIDATES = new String[] {
            "pod", "pod_name", "kubernetes_pod_name", "container_label_io_kubernetes_pod_name"
    };*/

    /*private static final String[]  POD_LABEL_CANDIDATES = new String[] {
            /*
             * == Gängige Labels für Kubernetes Pods (höchste Priorität) ==
             * "pod" ist der De-facto-Standard in den meisten modernen Kubernetes-Setups.
             */
            /*"pod",
            "pod_name",
            "kubernetes_pod_name",

            /*
             * == Gängige Labels für Container-Namen (in K8s und Docker) ==
             * cAdvisor (oft in K8s integriert) und andere Tools verwenden "container".
             * Der Name kann manchmal den Präfix "k8s_" haben.
             */
            /*"container",
            "container_name",
            "name", // Ein generisches Label, das cAdvisor für den reinen Container-Namen nutzt

            /*
             * == Weniger gängige, aber mögliche Labels ==
             */
            /*"container_label_io_kubernetes_pod_name", // Spezifisch für Kubernetes
            "exported_pod",
            "pod_id",
            "com_docker_compose_service" // Wenn Docker Compose Labels exportiert werden
    };


    private static final String[] SERVER_NODE_LABEL_CANDIDATES = new String[] {
            /*
             * == Gängigste Labels für Server/VMs/Nodes ==
             * "instance" wird von Prometheus oft automatisch hinzugefügt und enthält meist "hostname:port".
             * "nodename" ist der Standard vom node_exporter.
             * "hostname" ist ebenfalls sehr verbreitet.
             */
            /*"instance",
            "nodename",
            "hostname",

            /*
             * == Labels in Kubernetes-Umgebungen ==
             * "kubernetes_node" wird oft in K8s-Setups für den Node-Namen verwendet.
             * "node" ist der Standard von kube-state-metrics.
             */
            /*"node",
            "kubernetes_node",

            /*
             * == Weniger gängige, aber mögliche Labels ==
             * "exported_instance" und "exported_job" können durch Relabeling-Regeln entstehen.
             */
            /*"exported_instance",
            "exported_nodename",
            "host"
    };



    // Procedure output record
    public static class Output {

        public String message;


        public String prometheusUrl;

        public long podCount;

        public Output(String message, String url, long podCount) {
            this.message = message;
            this.prometheusUrl = url;
            this.podCount = podCount;
        }
    }

    @Procedure(name = "timegraph.datasources.register_prometheus", mode = Mode.WRITE)
    @Description("CALL timegraph.datasources.register_prometheus(url) - registers Pods/Servers from Prometheus and starts a 30s updater")
    public Stream<Output> registerPrometheus(@Name("url") String prometheusUrl) {
        try {
            // 1) Get metric names (__name__ label values)
            List<String> metricNames = fetchMetricNames(prometheusUrl);

            log.info("URL: " +prometheusUrl);
            log.info("metricNames: " + metricNames);

            // 2) Get pod names (try candidates)
            List<String> podNames = fetchFirstAvailableLabelValues(prometheusUrl, POD_LABEL_CANDIDATES);

            log.info("podNames: " + podNames);

            // 3) Get instance values (server host:port), then extract host part
            // momentan auskommentiert später fixen !!!!!
            //List<String> instances = fetchLabelValues(prometheusUrl, "instance");
            Set<String> servers = new LinkedHashSet<>();
            /*for (String inst : instances) {
                if (inst == null) continue;
                String host = inst.split(":")[0];
                if (host != null && !host.isEmpty()) servers.add(host);
            }

            log.info("instances: " + instances);
            log.info("servers: " + servers);*/

            // Heuristic time_range: start = now - 30 days, end = now (ISO 8601).
            // This is a reasonable default; you can later refine with more accurate queries.
            /*Instant now = Instant.now();
            Instant start = now.minus(30, ChronoUnit.DAYS);
            String isoNow = now.toString();
            String isoStart = start.toString();

            // Persist nodes & relationships
            long podCount = createNodesAndRelationships(prometheusUrl, metricNames, podNames, new ArrayList<>(servers), isoStart, isoNow);

            // Create or reuse a scheduled updater that updates Pod.end every 30s
            //startOrEnsureUpdater(prometheusUrl);

            return Stream.of(new Output("registered", prometheusUrl, podCount));//podCount));

        } catch (Exception e) {
            // Return error message in result (procedures should not throw unchecked except for fatal)
            return Stream.of(new Output("error: " + e.getMessage(), prometheusUrl, 0));
        }
    }

    // -----------------------
    // Neo4j persistence helpers
    // -----------------------
    private long createNodesAndRelationships(String prometheusUrl, List<String> metricNames,
                                             List<String> podNames, List<String> serverNames,
                                             String startIso, String endIso) {
        long podCount = 0;
        try (Transaction tx = db.beginTx()) {
            // Create Prometheus node (MERGE by url so re-calls are idempotent)
            Map<String,Object> params = new HashMap<>();
            params.put("url", prometheusUrl);
            params.put("names", metricNames);
            String createProm = "MERGE (pr:Prometheus {url:$url}) SET pr.names = $names RETURN id(pr) AS id";
            Result r = tx.execute(createProm, params);
            // create servers nodes
            for (String sName: serverNames) {
                Map<String,Object> p = new HashMap<>();
                p.put("name", sName);
                p.put("start", startIso);
                p.put("end", endIso);
                String q = "MERGE (s:Server:time_period {name:$name}) " +
                        "SET s.start = $start, s.end = $end";
                tx.execute(q, p);
            }
            // create pod nodes and DEPLOYED_ON relationships
            for (String pName: podNames) {
                TimeRange range = null;
                try {
                    range = PrometheusClient.getPodTimeRange(prometheusUrl, pName);
                } catch (Exception e) {
                    log.error(e.getMessage());
                    throw new RuntimeException(e);
                }

                log.info("start: " + range.start + "  end: " + range.end);

                Map<String, Object> p = new HashMap<>();
                p.put("name", pName);
                p.put("start", range.start);
                p.put("end", range.end);
                // MERGE pod
                String qpod = "MERGE (p:Pod:time_period {name:$name}) " +
                        "SET p.start = $start, p.end = $end";
                tx.execute(qpod, p);

                // Try to match server by pod label -> server matching is heuristic: if pod contains server ip or known mapping not available
                // We'll simply link all Pods to all servers if no deterministic mapping is available.
                // First, try to find a server node by name if pod contains a recognizable IP
                boolean linked = false;
                String maybeIp = extractIpFromName(pName); // try to extract ip-like substring
                if (maybeIp != null) {
                    Map<String,Object> map = new HashMap<>();
                    map.put("podName", pName);
                    map.put("serverName", maybeIp);
                    String qrel = "MATCH (p:Pod:time_period {name:$podName}), (s:Server:time_period {name:$serverName}) " +
                            "MERGE (p)-[:DEPLOYED_ON]->(s)";
                    tx.execute(qrel, map);
                    linked = true;
                }
                // If not linked, create DEPLOYED_ON to a default server if there is exactly one server, or to none.
                if (!linked && serverNames.size() == 1) {
                    Map<String,Object> map = new HashMap<>();
                    map.put("podName", pName);
                    map.put("serverName", serverNames.get(0));
                    String qrel = "MATCH (p:Pod:time_period {name:$podName}), (s:Server:time_period {name:$serverName}) " +
                            "MERGE (p)-[:DEPLOYED_ON]->(s)";
                    tx.execute(qrel, map);
                }
                podCount++;
            }

            // Connect all pods to the Prometheus node with HAS_TIME_SERIES
            String linkPodsToProm = "MATCH (p:Pod:time_period), (pr:Prometheus {url:$url}) " +
                    "MERGE (p)-[:HAS_TIME_SERIES]->(pr)";
            Map<String,Object> param = new HashMap<>();
            param.put("url", prometheusUrl);
            tx.execute(linkPodsToProm, param);

            tx.commit();
        }
        return podCount;
    }

    private static String extractIpFromName(String s) {
        if (s == null) return null;
        // naive IP regex search
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(?:\\d{1,3}\\.){3}\\d{1,3}").matcher(s);
        if (m.find()) return m.group(0);
        return null;
    }

    // -----------------------
    // Updater (runs every 30s)
    // -----------------------
    private void startOrEnsureUpdater(String prometheusUrl) {
        SCHEDULERS.computeIfAbsent(prometheusUrl, url -> {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("prometheus-updater-" + Math.abs(url.hashCode()));
                return t;
            });
            Runnable task = () -> {
                String now = Instant.now().toString();
                try (Transaction tx = db.beginTx()) {
                    Map<String,Object> p = Collections.singletonMap("now", now);
                    String update = "MATCH (p:Pod:time_period) SET p.end = $now RETURN count(p) AS updated";
                    Result res = tx.execute(update, p);
                    // optionally log number updated:
                    try {
                        if (res.hasNext()) {
                            Map<String,Object> row = res.next();
                            // no logger available, but we could use System.out (avoid noisy logs)
                            // System.out.printf("Prometheus updater for %s updated %s pods%n", prometheusUrl, row.get("updated"));
                        }
                    } catch (Exception ignored) {}
                    tx.commit();
                } catch (Exception e) {
                    // swallow exceptions to keep scheduler alive; in production you should log
                    // System.err.println("Prometheus updater error: " + e.getMessage());
                }
            };
            // start immediately, then every 30 seconds
            scheduler.scheduleAtFixedRate(task, 0L, 30L, TimeUnit.SECONDS);
            return scheduler;
        });
    }
}*/
