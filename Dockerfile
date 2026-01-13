# TimeGraph Neo4j Plugin Docker Image
# Integrates with Prometheus and Jaeger for observability data

FROM neo4j:2025-community

# Copy the plugin JAR to Neo4j plugins directory
COPY neo4j_plugin/target/neo4j_plugin-1.0-SNAPSHOT.jar /var/lib/neo4j/plugins/

# Neo4j configuration
ENV NEO4J_AUTH=neo4j/graphobs123
ENV NEO4J_PLUGINS='["apoc"]'

# Allow all custom procedures (graphobs.*)
ENV NEO4J_dbms_security_procedures_unrestricted=*
ENV NEO4J_dbms_security_procedures_allowlist=*

# Data source URLs - override these in Kubernetes deployment
# Leave empty to skip auto-registration
ENV PROMETHEUS_URL=""
ENV JAEGER_URL=""

# Expose Neo4j ports
# 7474 - HTTP
# 7687 - Bolt
EXPOSE 7474 7687

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:7474 || exit 1
