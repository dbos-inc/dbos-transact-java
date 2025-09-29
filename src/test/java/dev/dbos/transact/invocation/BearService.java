package dev.dbos.transact.invocation;

import java.time.Instant;

public interface BearService {

  String getName();

  Instant nowStep();

  Instant stepWorkflow();
}
