package dev.dbos.transact.json;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

public class Boxed {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    public Object[] args;

    public Boxed() {
    }

    public Boxed(Object[] args) {
        this.args = args;
    }
}
