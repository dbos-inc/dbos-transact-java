package dev.dbos.transact.database;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class StreamIterator implements Iterator<Object> {
  private final String workflowId;
  private final String key;
  private final SystemDatabase systemDatabase;
  private int offset = 0;
  private Object nextValue = null;
  private boolean finished = false;

  public StreamIterator(String workflowId, String key, SystemDatabase systemDatabase) {
    this.workflowId = workflowId;
    this.key = key;
    this.systemDatabase = systemDatabase;
  }

  @Override
  public boolean hasNext() {
    if (!finished && nextValue == null) {
      try {
        Object value = systemDatabase.readStream(workflowId, key, offset);
        if (value == SystemDatabase.END_OF_STREAM) {
          finished = true;
        } else {
          nextValue = value;
          offset++;
        }
      } catch (IllegalStateException e) {
        finished = true;
      }
    }
    return !finished;
  }

  @Override
  public Object next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Object result = nextValue;
    nextValue = null;
    return result;
  }
}
