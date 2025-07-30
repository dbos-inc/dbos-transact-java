package dev.dbos.transact.workflow;

public class ForkOptions {
    private final String forkedWorkflowId;
    private final String applicationVersion;
    private final long timeoutMS;

    private ForkOptions(Builder builder) {
        this.forkedWorkflowId = builder.forkedWorkflowId;
        this.applicationVersion = builder.applicationVersion;
        this.timeoutMS = builder.timeoutMS;
    }

    public String getForkedWorkflowId() {
        return forkedWorkflowId;
    }

    public String getApplicationVersion() {
        return applicationVersion;
    }

    public long getTimeoutMS() {
        return timeoutMS;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String forkedWorkflowId;
        private String applicationVersion;
        private long timeoutMS;

        public Builder() {}

        public Builder forkedWorkflowId(String forkedWorkflowId) {
            this.forkedWorkflowId = forkedWorkflowId;
            return this;
        }

        public Builder applicationVersion(String applicationVersion) {
            this.applicationVersion = applicationVersion;
            return this;
        }

        public Builder timeoutMS(long timeoutMS) {
            this.timeoutMS = timeoutMS;
            return this;
        }

        public ForkOptions build() {
            return new ForkOptions(this);
        }
    }

    @Override
    public String toString() {
        return "ForkOptions{" +
                "forkedWorkflowId='" + forkedWorkflowId + '\'' +
                ", applicationVersion='" + applicationVersion + '\'' +
                ", timeoutMS=" + timeoutMS +
                '}';
    }
}
