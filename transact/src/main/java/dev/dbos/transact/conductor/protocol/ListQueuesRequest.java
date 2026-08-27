package dev.dbos.transact.conductor.protocol;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import tools.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ListQueuesRequest extends BaseMessage {
  public Body body;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Body(
      @JsonDeserialize(using = StringOrListDeserializer.class) List<String> application_name) {}

  public ListQueuesRequest() {}

  /** A Conductor predating the filter sends no body, leaving the listing scoped to this app. */
  public List<String> applicationName() {
    return body == null ? null : body.application_name();
  }
}
