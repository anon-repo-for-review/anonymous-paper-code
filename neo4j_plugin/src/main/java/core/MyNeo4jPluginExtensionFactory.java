package core;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.Driver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


@ServiceProvider
public class MyNeo4jPluginExtensionFactory extends ExtensionFactory<MyNeo4jPluginExtensionFactory.Dependencies> {


    private GraphDatabaseService db;
    private DatabaseManagementService managementService;
    private LogService log;
    private Driver driver;

    public MyNeo4jPluginExtensionFactory() {
        super(ExtensionType.DATABASE, "MyExtensionFactory");
    }



    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        db = dependencies.db();
        managementService = dependencies.databaseManagementService();
        log = dependencies.log();


        //dependencies.startupListener().start();

        //String boltUrl = "bolt://localhost:7687";  // Falls Neo4j auf Standard-Port läuft
        //driver = GraphDatabase.driver(boltUrl, AuthTokens.basic("neo4j", "12345678"));

        return new MyAdapter();
    }

    public class MyAdapter extends LifecycleAdapter {

        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        @Override
        public void start() throws Exception {
            if (!db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
                log.getUserLog(MyNeo4jPluginExtensionFactory.class).info("Registering transaction event listener for database " + db.databaseName());
                managementService.registerTransactionEventListener(
                        db.databaseName(),
                        new MyTransactionEventListener(db, log)
                );
            } else {
                log.getUserLog(MyNeo4jPluginExtensionFactory.class).info("System database. Not registering transaction event listener");
            }


            scheduler.schedule(() -> {
                try {
                    //GraphDatabaseService db = dependencies.db();
                    try (Transaction tx = db.beginTx()) {
                        // Du kannst hier denselben Code verwenden wie in der Prozedur:
                        //tx.execute("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)");
                        //tx.execute("CREATE INDEX IF NOT EXISTS FOR (n:Order) ON (n.orderId)");

                        createIndexIfNotExists(tx, "time_series", new String[]{"start", "end"});
                        createIndexIfNotExists(tx, "time_series", new String[]{"start"});
                        createIndexIfNotExists(tx, "time_series", new String[]{"end"});


                        createIndexIfNotExists(tx, "time_period", new String[]{"end", "start"});
                        createIndexIfNotExists(tx, "time_period", new String[]{"start"});
                        createIndexIfNotExists(tx, "time_period", new String[]{"end"});


                        tx.commit();
                    }
                    //dependencies.log().getUserLog(StartupScheduler.class)
                            //.info("Indexes created successfully via StartupScheduler.");
                } catch (Exception e) {
                    log.getUserLog(MyNeo4jPluginExtensionFactory.class).info("Nicht funktioniert");
                }
            }, 30, TimeUnit.SECONDS);





        }

    }


    private void createIndexIfNotExists(Transaction tx, String label, String[] properties) {
        String indexName = label + "_" + String.join("_", properties) + "_index";


        boolean indexExists = false; // Anpassen !!!!!

        if (!indexExists) {
            // Build CREATE INDEX query
            StringBuilder createQuery = new StringBuilder();
            createQuery.append("CREATE INDEX ").append(indexName)
                    .append(" IF NOT EXISTS FOR (n:").append(label).append(") ON (");
            for (int i = 0; i < properties.length; i++) {
                createQuery.append("n.").append(properties[i]);
                if (i < properties.length - 1) {
                    createQuery.append(", ");
                }
            }
            createQuery.append(")");
            log.getUserLog(MyNeo4jPluginExtensionFactory.class).info("Index erstellt: " + createQuery.toString());
            tx.execute(createQuery.toString());
            log.getUserLog(MyNeo4jPluginExtensionFactory.class).info("Index erstellt: " + indexName);
        } else {
            log.getUserLog(MyNeo4jPluginExtensionFactory.class).info("Index existiert bereits: " + indexName);
        }
    }






    interface Dependencies {
        GraphDatabaseService db();
        DatabaseManagementService databaseManagementService();
        LogService log();
        //StartupListener startupListener();

    }
}
