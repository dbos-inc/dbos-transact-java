# Test Refactoring Plan: Migrating to `DBOS.Instance` and `PgContainer`

## Goal
Migrate all tests in the `java-transact` repository to use `DBOS.Instance` instead of static `DBOS` methods. This ensures test isolation, better resource management, and compatibility with concurrent test execution.

## Key Components

### 1. `PgContainer`
- Each test class should use `PgContainer` to provide a unique, isolated PostgreSQL instance via Testcontainers.
- It is initialized as an `@AutoClose` field.
- Provides `dbosConfig()`, `dataSource()`, and `dbosClient()`.

### 2. `DBOS.Instance`
- Replace `DBOS.configure()` and `DBOSTestAccess.reinitialize()` with `new DBOS.Instance(dbosConfig)`.
- The instance should be an `@AutoClose` field in the test class.
- All static calls (e.g., `DBOS.launch()`, `DBOS.registerWorkflows()`) must be replaced by instance calls (e.g., `dbos.launch()`).

### 3. Service Injection & Implementation
- Services that use DBOS methods (like `recv`, `send`, `runStep`, `startWorkflow`) must be updated to accept a `DBOS.Instance` in their constructor.
- Inside the service, replace `DBOS.method()` with `this.dbos.method()`.
- **Logging**: Replace all `System.out.printf`, `System.out.println`, and `System.err` calls with `slf4j.Logger`. Use the `logger.info("message {}", arg)` pattern.
- **Self-Proxying**: If a service needs a proxy of itself for internal workflow/step calls:
  - Add a `setSelfProxy(ServiceProxy proxy)` (or similar) method to the **implementation class only**, not the interface.
  - The test setup should call this method after `dbos.registerWorkflows()`.
- **Annotations**: Always add `@Override` annotations to methods implemented from a service interface.

### 4. JUnit 5 Annotations & Lifecycle
- Use `@AutoClose` for `PgContainer` and `HikariDataSource`.
- **DBOS.Instance Management**:
  - For standard tests, you can use an `@AutoClose DBOS.Instance dbos;` field if the instance is used throughout the test class and doesn't need restarting.
  - **IMPORTANT**: If a `DBOS.Instance` is created locally within a test method (including setup for individual tests), it **MUST** be managed using `try-with-resources` to ensure `dbos.shutdown()` is called and resources are cleaned up.
  - For tests simulating restarts (like `Issue218`), use multiple `try-with-resources` blocks locally: `try (var dbos = new DBOS.Instance(dbosConfig)) { ... }`.
- Add `@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)` to enable parallel test execution where appropriate.
- Remove `@AfterEach` methods that manually close data sources or shutdown DBOS.

### 5. Code Quality & Naming
- Rename generic interfaces/classes (e.g., `Example`, `Service`, `Impl`) to specific names related to the test (e.g., `Issue218Service`, `Issue218ServiceImpl`).
- **Consolidation**:
  - If a service interface and implementation are used **only** in one test class, move them into that test file (as package-private classes/interfaces).
  - If they are shared across multiple test classes, they should remain in their own files.
- Extract common setup logic (like workflow/queue registration) into helper methods (e.g., `private MyService register(DBOS.Instance dbos)`) to keep tests clean.

---

## Refactoring Pattern

### Test Class Template
```java
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class SomeTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS.Instance dbos;
  @AutoClose HikariDataSource dataSource;
  
  SomeService service;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS.Instance(dbosConfig);
    dataSource = pgContainer.dataSource();

    // Register queues, workflows, etc.
    service = dbos.registerWorkflows(SomeService.class, new SomeServiceImpl(dbos));

    dbos.launch();
  }
  
  // Tests use 'dbos' and 'service'
}
```

### Service Implementation Template
```java
public class SomeServiceImpl implements SomeService {
  private final DBOS.Instance dbos;

  public SomeServiceImpl(DBOS.Instance dbos) {
    this.dbos = dbos;
  }

  @Workflow
  public String someWorkflow() {
    return dbos.runStep(() -> "result");
  }
}
```

---

## Special Case: Restart & Recovery Tests

Some tests (like `Issue218`) verify that DBOS can recover state after a shutdown. For these tests:

1. **Avoid `@AutoClose` for `dbos`**: Do not use a class-level `@AutoClose DBOS.Instance` field if the test requires closing and reopening the instance.
2. **Use `try-with-resources`**: Manage the `DBOS.Instance` lifecycle within the test method using local `try-with-resources` blocks.
3. **Helper Registration**: Extract registration logic into a private helper method that takes `DBOS.Instance` as an argument. This ensures both the initial and the "restarted" instances are configured identically.
4. **Persistent Config**: Reuse the same `dbosConfig` (provided by `pgContainer`) across restarts to ensure they connect to the same database.

### Restart Test Example
```java
@Test
void testRestart() throws Exception {
  String wfid;
  // First run
  try (var dbos = new DBOS.Instance(dbosConfig)) {
    var service = register(dbos);
    dbos.launch();
    var handle = dbos.startWorkflow(() -> service.myWorkflow());
    wfid = handle.workflowId();
  } // dbos.shutdown() called here automatically

  // Second run (Restart)
  try (var dbos = new DBOS.Instance(dbosConfig)) {
    register(dbos);
    dbos.launch();
    assertEquals("result", dbos.getResult(wfid));
  }
}

private MyService register(DBOS.Instance dbos) {
  dbos.registerQueue(new Queue("test-queue"));
  var impl = new MyServiceImpl(dbos);
  return dbos.registerWorkflows(MyService.class, impl);
}
```

---

## Specific Tooling Updates

- **`DBOSTestAccess`**: Use methods that accept `DBOS.Instance` (e.g., `DBOSTestAccess.getQueueService(dbos)`).
- **`DBUtils`**: No longer need `DBUtils.recreateDB(dbosConfig)` as `PgContainer` provides a fresh DB.
- **`DBOSClient`**: Use `pgContainer.dbosClient()` to get a client connected to the test container.

---

## Target Files for Refactoring

- transact/src/test/java/dev/dbos/transact/json/InteropTest.java                                                                                               │
- transact/src/test/java/dev/dbos/transact/json/PortableSerializationTest.java                                                                                 │
- transact/src/test/java/dev/dbos/transact/notifications/EventsTest.java                                                                                       │
- transact/src/test/java/dev/dbos/transact/notifications/NotificationServiceTest.java                                                                          │
- transact/src/test/java/dev/dbos/transact/queue/PartitionedQueuesTest.java                                                                                    │
- transact/src/test/java/dev/dbos/transact/queue/QueuesTest.java                                                                                               │
- transact/src/test/java/dev/dbos/transact/scheduled/SchedulerServiceTest.java                                                                                 │
- transact/src/test/java/dev/dbos/transact/scheduled/SkedService.java     

---

## Execution Steps for the Next Agent

1. **Pick a directory**: Start with a small set of related tests (e.g., `workflow/`).
2. **Update Implementations**: Identify any service implementations used in those tests and update them to accept `DBOS.Instance`.
3. **Refactor Test Classes**: Apply the pattern described above to each test class.
4. **Verify**: Run the refactored tests using `./gradlew test --tests <ClassName>`.
5. **Iterate**: Move to the next directory until all tests are migrated.
6. **Final Cleanup**: Ensure no static `DBOS` methods are called in `src/test`.


## Test files that still need to be updated
