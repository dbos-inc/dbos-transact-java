package dev.dbos.transact.json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PrimitiveObjects {

    public PrimitiveObjects() {

    }

    public PrimitiveObjects(int i, long l, float f, double d, boolean b) {
        intValue = i;
        longValue = l;
        floatValue = f;
        doubleValue = d;
        boolValue = b ;
    }

    @JsonProperty
    Integer intValue ;

    @JsonProperty
    Long longValue ;

    @JsonProperty
    Float floatValue ;

    @JsonProperty
    Double doubleValue;

    @JsonProperty
    Boolean boolValue;
}
