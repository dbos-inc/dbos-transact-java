package dev.dbos.transact.scheduled;

import java.time.Instant;

public interface SkedService {
    void everyMinute(Instant schedule, Instant actual);
    void everySecond(Instant schedule, Instant actual);
    void everyThird(Instant schedule, Instant actual);
    void timed(Instant schedule, Instant actual);
    void withSteps(Instant schedule, Instant actual);
}
