package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

interface MockTestService {
  String testWorkflow();
}

class MockTestServiceImpl implements MockTestService {
  private final DBOS dbos;

  public MockTestServiceImpl(DBOS instance) {
    this.dbos = instance;
  }

  @Workflow
  @Override
  public String testWorkflow() {
    var today = dbos.runStep(() -> LocalDate.now(), "todaysDate");
    dbos.setEvent("greetEvent", today);
    var name = dbos.<String>recv("greetTopic", Duration.ofSeconds(30)).orElseThrow();
    return String.format("Hello %s, today is %s", name, today.format(DateTimeFormatter.ISO_DATE));
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class MockDbosInstanceTest {
  @Test
  public void testMockInstance() throws Exception {
    var mockDBOS = mock(DBOS.class);
    var impl = new MockTestServiceImpl(mockDBOS);

    var date = LocalDate.of(2024, 1, 1);

    when(mockDBOS.runStep(
            ArgumentMatchers.<ThrowingSupplier<LocalDate, Exception>>any(),
            ArgumentMatchers.eq("todaysDate")))
        .thenReturn(date);
    when(mockDBOS.recv(
            ArgumentMatchers.eq("greetTopic"), ArgumentMatchers.eq(Duration.ofSeconds(30))))
        .thenReturn(Optional.of("Alice"));

    // Call the workflow
    String result = impl.testWorkflow();

    // Verify output
    assertEquals("Hello Alice, today is 2024-01-01", result);

    verify(mockDBOS)
        .runStep(
            ArgumentMatchers.<ThrowingSupplier<LocalDate, Exception>>any(),
            ArgumentMatchers.eq("todaysDate"));
    verify(mockDBOS).setEvent(ArgumentMatchers.eq("greetEvent"), ArgumentMatchers.eq(date));
    verify(mockDBOS)
        .recv(ArgumentMatchers.eq("greetTopic"), ArgumentMatchers.eq(Duration.ofSeconds(30)));
  }
}
