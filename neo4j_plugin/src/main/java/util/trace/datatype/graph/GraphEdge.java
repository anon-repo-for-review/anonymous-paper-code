package util.trace.datatype.graph;

public class GraphEdge {
    private String fromNodeId;
    private String toNodeId;
    private String relationshipType; // "HAS_OPERATION" oder "DEPENDS_ON"

    public GraphEdge(String fromNodeId, String toNodeId, String relationshipType) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.relationshipType = relationshipType;
    }

    public String getFromNodeId() {
        return fromNodeId;
    }

    public String getToNodeId() {
        return toNodeId;
    }

    public String getRelationshipType() {
        return relationshipType;
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        GraphEdge graphEdge = (GraphEdge) o;
//        return Objects.equals(fromNodeId, graphEdge.fromNodeId) &&
//                Objects.equals(toNodeId, graphEdge.toNodeId) &&
//                Objects.equals(relationshipType, graphEdge.relationshipType);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(fromNodeId, toNodeId, relationshipType);
//    }


}
