package util.trace.datatype.datastructs;

import java.util.List;

public class JaegerTraces {
    private List<Trace> data;
    public List<Trace> getData() {
        return data;
    }
    public void setData(List<Trace> data) {
        this.data = data;
    }
}