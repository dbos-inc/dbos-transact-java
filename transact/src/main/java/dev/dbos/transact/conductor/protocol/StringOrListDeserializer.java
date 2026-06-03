package dev.dbos.transact.conductor.protocol;

import java.util.ArrayList;
import java.util.List;

import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

public class StringOrListDeserializer extends ValueDeserializer<List<String>> {
  @Override
  public List<String> deserialize(JsonParser p, DeserializationContext ctxt) {
    if (p.currentToken() == JsonToken.START_ARRAY) {
      List<String> result = new ArrayList<>();
      while (p.nextToken() != JsonToken.END_ARRAY) {
        result.add(p.getString());
      }
      return result;
    } else if (p.currentToken() == JsonToken.VALUE_STRING) {
      return List.of(p.getString());
    }
    return ctxt.reportInputMismatch(
        String.class, "Expected a string or an array of strings, got %s", p.currentToken());
  }
}
