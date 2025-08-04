package dev.dbos.transact.workflow;

/**
 * Configuration options for forking workflows. This class is immutable and uses the Builder pattern
 * for construction.
 */
public class ForkOptions {
  private final String forkedWorkflowId;
  private final String applicationVersion;
  private final long timeoutMS;

  private ForkOptions(Builder builder) {
    this.forkedWorkflowId = builder.forkedWorkflowId;
    this.applicationVersion = builder.applicationVersion;
    this.timeoutMS = builder.timeoutMS;
  }

  /**
   * Gets the forked workflow identifier.
   *
   * @return the forked workflow ID, may be null
   */
  public String getForkedWorkflowId() {
    return forkedWorkflowId;
  }

  /**
   * Gets the application version for the forked workflow.
   *
   * @return the application version, may be null
   */
  public String getApplicationVersion() {
    return applicationVersion;
  }

  /**
   * Gets the timeout in milliseconds for the forked workflow.
   *
   * @return the timeout in milliseconds
   */
  public long getTimeoutMS() {
    return timeoutMS;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing ForkOptions instances. Provides a fluent interface for setting
   * configuration values.
   */
  public static class Builder {
    private String forkedWorkflowId;
    private String applicationVersion;
    private long timeoutMS;

    public Builder() {}

    /**
     * Sets the forked workflow identifier.
     *
     * @param forkedWorkflowId the workflow ID to set
     * @return this Builder instance for method chaining
     */
    public Builder forkedWorkflowId(String forkedWorkflowId) {
      this.forkedWorkflowId = forkedWorkflowId;
      return this;
    }

    /**
     * Sets the application version for the forked workflow.
     *
     * @param applicationVersion the application version to set
     * @return this Builder instance for method chaining
     */
    public Builder applicationVersion(String applicationVersion) {
      this.applicationVersion = applicationVersion;
      return this;
    }

    /**
     * Sets the timeout in milliseconds for the forked workflow.
     *
     * @param timeoutMS the timeout in milliseconds
     * @return this Builder instance for method chaining
     */
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
    return "ForkOptions{"
        + "forkedWorkflowId='"
        + forkedWorkflowId
        + '\''
        + ", applicationVersion='"
        + applicationVersion
        + '\''
        + ", timeoutMS="
        + timeoutMS
        + '}';
  }
}
