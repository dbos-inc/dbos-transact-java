package dev.dbos.transact.conductor.protocol;

import java.util.Collections;
import java.util.List;

public class ListApplicationVersionsResponse extends BaseResponse {
  public List<ApplicationVersionOutput> application_versions;

  public ListApplicationVersionsResponse() {}

  public ListApplicationVersionsResponse(
      BaseMessage message, List<ApplicationVersionOutput> versions) {
    super(message.type, message.request_id);
    this.application_versions = versions;
  }

  public ListApplicationVersionsResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.application_versions = Collections.emptyList();
  }
}
