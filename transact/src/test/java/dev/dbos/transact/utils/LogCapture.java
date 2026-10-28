package dev.dbos.transact.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

/** Records what the given classes' loggers emit while it is open. */
public final class LogCapture implements AutoCloseable {

  private final List<Logger> loggers;
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  public LogCapture(Class<?>... sources) {
    appender.start();
    loggers =
        Arrays.stream(sources).map(source -> (Logger) LoggerFactory.getLogger(source)).toList();
    loggers.forEach(logger -> logger.addAppender(appender));
  }

  /** The formatted messages logged at {@code level} or above that mention {@code text}. */
  public List<String> messages(Level level, String text) {
    List<ILoggingEvent> events;
    // Appends are synchronized on the appender, and arrive from the threads doing the logging.
    synchronized (appender) {
      events = new ArrayList<>(appender.list);
    }
    return events.stream()
        .filter(event -> event.getLevel().isGreaterOrEqual(level))
        .map(ILoggingEvent::getFormattedMessage)
        .filter(message -> message.contains(text))
        .toList();
  }

  @Override
  public void close() {
    loggers.forEach(logger -> logger.detachAppender(appender));
    appender.stop();
  }
}
