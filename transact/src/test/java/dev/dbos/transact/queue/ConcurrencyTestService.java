package dev.dbos.transact.queue;

public interface ConcurrencyTestService {
  int noopWorkflow(int i);

  int blockedWorkflow(int i) throws InterruptedException;
}
