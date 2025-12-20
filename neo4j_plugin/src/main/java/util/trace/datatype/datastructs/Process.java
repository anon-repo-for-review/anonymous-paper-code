package util.trace.datatype.datastructs;

import java.util.List;

public class Process {
        private String serviceName;
        private List<Tag> tags;


        public String getServiceName() {
            return serviceName;
        }

        public List<Tag> getTags() {
            return tags;
        }

        public Process setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Process setTags(List<Tag> tags) {
            this.tags = tags;
            return this;
        }

    }
