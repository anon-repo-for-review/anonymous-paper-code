package util.trace.datatype.graph;

import java.util.HashMap;
import java.util.Map;

public class GraphNode {
    private String id;
    private String type; //"Service"/"Operation"
    private Map<String, String> properties;

    public GraphNode setType(String type) {
        this.type = type;
        return this;
    }

    public GraphNode setProperties(Map<String, String> properties) {
        this.properties = properties;
        return this;
    }

    public GraphNode setId(String id) {
        this.id = id;
        return this;
    }

    public GraphNode(String id, String type) {
        this.id = id;
        this.type = type;
        this.properties = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void addProperty(String key, String value) {
        this.properties.put(key, value);
    }
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        GraphNode graphNode = (GraphNode) o;
//        return Objects.equals(id, graphNode.id) && Objects.equals(type, graphNode.type);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(id, type);
//    }

}
