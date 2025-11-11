package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

public class TestLifecycleService implements DBOSLifecycleListener {
  public int launchCount = 0;
  public int shutdownCount = 0;
  public int nInstances = 0;
  public int nWfs = 0;
  public int annotationCount = 0;

  public ArrayList<RegisteredWorkflow> wfs = new ArrayList<>();

  @Override
  public void dbosLaunched() {
    var expectedParams = new Class<?>[] {int.class, int.class};

    ++launchCount;

    nInstances = DBOS.getRegisteredWorkflowInstances().size();
    var wfs = DBOS.getRegisteredWorkflows();
    for (var wf : wfs) {
      var method = wf.workflowMethod();
      var tag = method.getAnnotation(TestLifecycleAnnotation.class);
      if (tag == null) {
        continue;
      }

      ++nWfs;
      annotationCount += tag.count();

      var paramTypes = method.getParameterTypes();
      if (!Arrays.equals(paramTypes, expectedParams)) {
        continue;
      }

      this.wfs.add(wf);
    }
  }

  @Override
  public void dbosShutDown() {
    ++shutdownCount;
  }

  public int runThemAll() throws Exception {
    int total = 0;
    for (var wf : wfs) {
      Object[] args = {nInstances, nWfs};
      var h = DBOS.startWorkflow(wf, args, new StartWorkflowOptions(UUID.randomUUID().toString()));
      total += (Integer) h.getResult();
    }
    return total;
  }
}
