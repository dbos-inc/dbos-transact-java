package dev.dbos.transact.json;

import dev.dbos.transact.exceptions.SerializableException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JSONUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // Optional
    }

    public static String serialize(Object obj) {
        return serializeArray(new Object[] { obj });
    }

    public static String serializeArray(Object[] args) {
        try {
            return mapper.writeValueAsString(new Boxed(args));
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public static Object[] deserializeToArray(String json) {
        try {
            Boxed boxed = mapper.readValue(json, Boxed.class);
            return boxed.args;
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }

    public static String serializeError(Throwable error) {

        SerializableException se = new SerializableException(error);
        return JSONUtil.serialize(se);
    }

    public static SerializableException deserializeError(String json) {
        Object[] eArray = JSONUtil.deserializeToArray(json);
        return (SerializableException) eArray[0];
    }
}
