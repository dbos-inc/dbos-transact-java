package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.VersionInfo;

import java.util.Collections;
import java.util.List;

public class ListApplicationVersionsResponse extends BaseResponse {
  public static record AppVersionInfo(
      String version_id, String version_name, long version_timestamp, long created_at) {
    public static AppVersionInfo fromVersionInfo(VersionInfo v) {
      return new AppVersionInfo(
          v.versionId(),
          v.versionName(),
          v.versionTimestamp().toEpochMilli(),
          v.createdAt().toEpochMilli());
    }
  }

  public List<AppVersionInfo> output;

  public ListApplicationVersionsResponse() {}

  public ListApplicationVersionsResponse(BaseMessage message, List<VersionInfo> versions) {
    super(message.type, message.request_id);
    this.output = versions.stream().map(AppVersionInfo::fromVersionInfo).toList();
  }

  public ListApplicationVersionsResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.output = Collections.emptyList();
  }
}
