package dev.dbos.transact.workflow;

public record ErrorResult(
    String className, String message, String serializedError, Throwable throwable) {}
