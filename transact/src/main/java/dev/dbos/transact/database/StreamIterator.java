package dev.dbos.transact.database;

import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;

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
    advance();
  }

  private void advance() {
    while (!finished) {
      try {
        Object value = systemDatabase.readStream(workflowId, key, offset);
        nextValue = value;
        offset++;
        return;
      } catch (IllegalArgumentException e) {
        WorkflowStatus status = systemDatabase.getWorkflowStatus(workflowId);
        if (status == null || !isWorkflowActive(status.status())) {
          finished = true;
          nextValue = null;
          return;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          finished = true;
          nextValue = null;
          return;
        }
      } catch (IllegalStateException e) {
        finished = true;
        nextValue = null;
        return;
      }
    }
  }

  private boolean isWorkflowActive(WorkflowState state) {
    return WorkflowState.PENDING == state || WorkflowState.ENQUEUED == state;
  }

  @Override
  public boolean hasNext() {
    return nextValue != null;
  }

  @Override
  public Object next() {
    if (nextValue == null) {
      throw new NoSuchElementException();
    }
    Object result = nextValue;
    advance();
    return result;
  }
}
