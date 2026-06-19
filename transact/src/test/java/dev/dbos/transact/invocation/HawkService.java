package dev.dbos.transact.invocation;

import java.time.Instant;

public interface HawkService {

  String simpleWorkflow();

  String sleepWorkflow(long sleepSec);

  String parentWorkflow();

  String parentStartWorkflow();

  String parentSleepWorkflow(Long timeoutSec, long sleepSec);

  Instant nowStep();

  Instant stepWorkflow();

  String illegalStep();

  String illegalWorkflow();

  // returns "user|assumedRole|role0,role1,..." (or "null" for absent fields) for auth context tests
  String authContextWorkflow();

  String parentWorkflowWithAuthOverride();

  String parentWorkflowWithAuthCleared();

  // The following start `authContextWorkflow` as a child via startWorkflow, exercising how
  // StartWorkflowOptions auth fields are resolved against the parent's identity:
  // absent inherits, present overrides, present-null clears.
  String startChildInheritAuth(String childId);

  String startChildOverrideAuth(String childId);

  String startChildClearAllAuth(String childId);

  String startChildClearUserAndRoles(String childId);

  String startChildClearAssumedRoleOnly(String childId);
}
