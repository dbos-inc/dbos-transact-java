package dev.dbos.transact.client;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.time.Duration;

interface StreamTestService {
  void writeStreamBasic(String key, String value);

  String writeStreamMultiple(String key);

  String writeAndCloseStream(String key);

  void writeStreamInStep(String key, String value);

  void writeOneAndSleep(String key);

  void writeTimestampsAndClose(String key, int count);
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

  @Workflow
  @Override
  public void writeOneAndSleep(String key) {
    dbos.writeStream(key, "only_value");
    dbos.sleep(Duration.ofSeconds(2));
  }

  @Workflow
  @Override
  public void writeTimestampsAndClose(String key, int count) {
    for (int i = 0; i < count; i++) {
      dbos.writeStream(key, System.currentTimeMillis());
      dbos.sleep(Duration.ofSeconds(1));
    }
    dbos.closeStream(key);
  }
}
