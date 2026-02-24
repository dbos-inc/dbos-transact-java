package dev.dbos.transact.json;

/**
 * Native Java serializer using Jackson with type information. This is the default serializer for
 * Java DBOS applications.
 */
public class DBOSJavaSerializer implements DBOSSerializer {

  public static final String NAME = "java_jackson";

  public static final DBOSJavaSerializer INSTANCE = new DBOSJavaSerializer();

  public DBOSJavaSerializer() {}

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String stringify(Object value, boolean noHistoricalWrapper) {
    if (noHistoricalWrapper) return JSONUtil.serializeArray((Object[]) value);
    return JSONUtil.serialize(value);
  }

  @Override
  public Object parse(String text, boolean noHistoricalWrapper) {
    if (text == null) {
      return null;
    }
    var vi = JSONUtil.deserializeToArray(text);
    if (noHistoricalWrapper) return vi;
    return vi[0];
  }

  @Override
  public String stringifyThrowable(Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    return JSONUtil.serializeAppException(throwable);
  }

  @Override
  public Throwable parseThrowable(String text) {
    if (text == null) {
      return null;
    }
    return JSONUtil.deserializeAppException(text);
  }
}
