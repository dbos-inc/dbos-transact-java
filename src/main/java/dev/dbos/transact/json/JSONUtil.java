package dev.dbos.transact.json;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.Type;

public class JSONUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.activateDefaultTyping(mapper.getPolymorphicTypeValidator(), ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
    }

    public static String serialize(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public static Object deserialize(String json) {
        try {
            return mapper.readValue(json, Object.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}

