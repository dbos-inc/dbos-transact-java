package dev.dbos.transact.queue;

public class RateLimit {

    private final int limit ;
    private final double period;

    public RateLimit(int limit, double period) {
        this.limit = limit ;
        this.period = period;
    }

    public int getLimit() {
        return limit;
    }

    public double getPeriod() {
        return period;
    }
}
