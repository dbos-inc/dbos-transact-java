package dev.dbos.transact.client;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

interface StreamTestService {
  void writeStreamBasic(String key, String value);

  String writeStreamMultiple(String key);

  String writeAndCloseStream(String key);

  void writeStreamInStep(String key, String value);
}

@WorkflowClassName("StreamTestServiceImpl")
class StreamTestServiceImpl implements StreamTestService {

  private final DBOS dbos;

  public StreamTestServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Workflow
  @Override
  public void writeStreamBasic(String key, String value) {
    dbos.writeStream(key, value);
  }

  @Workflow
  @Override
  public String writeStreamMultiple(String key) {
    dbos.writeStream(key, "value1");
    dbos.writeStream(key, "value2");
    dbos.writeStream(key, "value3");
    return "done";
  }

  @Workflow
  @Override
  public String writeAndCloseStream(String key) {
    dbos.writeStream(key, "value1");
    dbos.writeStream(key, "value2");
    dbos.closeStream(key);
    return "done";
  }

  @Workflow
  @Override
  public void writeStreamInStep(String key, String value) {
    dbos.runStep(
        () -> {
          dbos.writeStream(key, value);
        },
        "streamStep");
  }
}
