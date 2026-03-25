package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.VersionInfo;

import java.time.Instant;

public class ApplicationVersionOutput {
  public String versionId;
  public String versionName;
  public Instant versionTimestamp;
  public Instant createdAt;

  public ApplicationVersionOutput() {}

  public ApplicationVersionOutput(VersionInfo v) {
    this.versionId = v.versionId();
    this.versionName = v.versionName();
    this.versionTimestamp = v.versionTimestamp();
    this.createdAt = v.createdAt();
  }
}
