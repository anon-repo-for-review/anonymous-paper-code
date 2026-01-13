# GraphObs

GraphObs is a Neo4j plugin that integrates observability data from Prometheus and Jaeger into a unified graph model. This enables topology-aware queries across metrics and traces using Cypher.

This repository demonstrates GraphObs in combination with the [OpenTelemetry Demo](https://opentelemetry.io/docs/demo/), a microservice-based application that generates realistic telemetry data.

## Prerequisites

- Running Kubernetes cluster
- Helm 3.x
- kubectl configured

## Installation

### 1. Add Helm Repository

```bash
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
helm repo update
```

### 2. Deploy OpenTelemetry Demo

```bash
kubectl apply -f kubernetes/grafana-neo4j-datasource.yaml
kubectl apply -f kubernetes/grafana-dashboard-configmap.yaml
helm install otel-demo open-telemetry/opentelemetry-demo -f kubernetes/values-grafana-neo4j.yaml
```

### 3. Deploy GraphObs

```bash
kubectl apply -f kubernetes/graphobs.yaml
```

Wait for all pods to be ready:

```bash
kubectl get pods -w
```

## Access

### Port Forwarding

```bash
# Neo4j Browser
kubectl port-forward svc/graphobs 7474:7474 7687:7687

# Grafana
kubectl port-forward svc/grafana 3000:80
```

### URLs

- Neo4j Browser: http://localhost:7474 (user: `neo4j`, password: `graphobs123`)
- Grafana: http://localhost:3000
- GraphObs Dashboard: Grafana → Dashboards → GraphObs folder

## Notes

- GraphObs automatically imports service topology from Jaeger and pod metrics from Prometheus ~30 seconds after startup.
- Initial data import may take a few minutes depending on cluster size.
- Check GraphObs logs for import status: `kubectl logs deployment/graphobs`

## Example Query

Try this in the Neo4j Browser to verify the setup:

```cypher
MATCH p = (s:Service {name : "frontend"})-[*..2]->(n)
WHERE none(x IN nodes(p) WHERE x:Prometheus OR x:Server OR x:Pod)
UNWIND nodes(p) AS pathNode

OPTIONAL MATCH (op)-[opRel:HAS_OPERATION]->(pathNode)
WHERE NOT (op:Prometheus OR op:Server OR op:Pod) OR op IS NULL
WITH p, op, opRel
UNWIND nodes(p) AS pNode
UNWIND relationships(p) AS pRel

RETURN 
  collect(DISTINCT pNode) + collect(DISTINCT op) AS nodes, 
  collect(DISTINCT pRel) + collect(DISTINCT opRel) AS eges;
```

## Uninstall

```bash
kubectl delete -f kubernetes/graphobs.yaml
kubectl delete -f kubernetes/grafana-dashboard-configmap.yaml
kubectl delete -f kubernetes/grafana-neo4j-datasource.yaml
helm uninstall otel-demo
```

## References

- [OpenTelemetry Demo Documentation](https://opentelemetry.io/docs/demo/)
- [OpenTelemetry Demo GitHub](https://github.com/open-telemetry/opentelemetry-demo)
