package dev.dbos.transact.queue;

public interface ConcurrencyTestService {
    public int noopWorkflow(int i);

    public int blockedWorkflow(int i) throws InterruptedException;;
}
