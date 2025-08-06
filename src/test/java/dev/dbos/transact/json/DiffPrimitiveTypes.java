package dev.dbos.transact.json;

public class DiffPrimitiveTypes {

    int intValue;
    long longValue;
    float floatValue;
    double doubleValue;
    boolean boolValue;

    public DiffPrimitiveTypes() {
    }

    public DiffPrimitiveTypes(int i, long l, float f, double d, boolean b) {
        intValue = i;
        longValue = l;
        floatValue = f;
        doubleValue = d;
        boolValue = b;
    }

    public int getIntValue() {
        return intValue;
    }

    public long getLongValue() {
        return longValue;
    }

    public float getFloatValue() {
        return floatValue;
    }

    public double getDoubleValue() {
        return doubleValue;
    }

    public boolean isBoolValue() {
        return boolValue;
    }

    public void setIntValue(int i) {
        intValue = i;
    }

    public void setLongValue(long l) {
        longValue = l;
    }

    public void setFloatValue(float f) {
        floatValue = f;
    }

    public void setDoubleValue(double d) {
        doubleValue = d;
    }

    public void setBoolValue(boolean b) {
        boolValue = b;
    }
}
