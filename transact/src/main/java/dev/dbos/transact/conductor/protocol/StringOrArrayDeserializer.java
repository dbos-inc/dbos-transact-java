package dev.dbos.transact.conductor.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public class StringOrArrayDeserializer extends JsonDeserializer<List<String>> {
  @Override
  public List<String> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonNode node = p.getCodec().readTree(p);
    List<String> result = new ArrayList<>();
    if (node.isArray()) {
      for (JsonNode element : node) {
        result.add(element.asText());
      }
    } else if (node.isTextual()) {
      result.add(node.asText());
    }
    return result;
  }
}
