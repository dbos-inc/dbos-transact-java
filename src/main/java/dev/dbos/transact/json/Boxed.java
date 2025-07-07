package dev.dbos.transact.json;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

public class Boxed {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    public Object value;

    public Boxed() {}

    public Boxed(Object value) {
        this.value = value;
    }
}
