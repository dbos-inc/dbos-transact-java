package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.UUID;

import org.junit.jupiter.api.*;

public class ClientTest {
    private static DBOSConfig dbosConfig;
    private static final String dbUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys";
    private static final String dbUser = "postgres";
    private static final String dbPassword = System.getenv("PGPASSWORD");

    @BeforeAll
    static void onetimeSetup() throws Exception {
        dbosConfig = new DBOSConfig.Builder().name("systemdbtest").dbHost("localhost")
                .dbPort(5432).dbUser(dbUser).sysDbName("dbos_java_sys").maximumPoolSize(2)
                .build();
    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        DBUtils.recreateDB(dbosConfig);
    }

    @Test
    public void clientEnqueue() throws Throwable {

        var dbos = DBOS.initialize(dbosConfig);

        dbos.Queue("testQueue").build();

        dbos.<ClientService>Workflow()
                .interfaceClass(ClientService.class)
                .implementation(new ClientServiceImpl()).build();

        try {
            dbos.launch();

            try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
                var options = new DBOSClient.EnqueueOptionsBuilder("enqueueTest", "testQueue")
                        .targetClassName("dev.dbos.transact.client.ClientServiceImpl").build();
                var handle = client.enqueueWorkflow(options, new Object[]{42, "spam"});
                var result = handle.getResult();
                assertTrue(result instanceof String);
                assertEquals("42-spam", result);
            }
        } finally {
            dbos.shutdown();
        }
    }

    @Test
    public void clientSend() throws Throwable {

        var dbos = DBOS.initialize(dbosConfig);

        ClientService service = dbos.<ClientService>Workflow()
                .interfaceClass(ClientService.class)
                .implementation(new ClientServiceImpl()).build();

        try {
            dbos.launch();

            var handle = dbos.startWorkflow(() -> service.sendTest(42));

            var idempotencyKey = UUID.randomUUID().toString();

            try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
                client.send(handle.getWorkflowId(), "test.message", "test-topic", idempotencyKey);
            }

            var workflowId = "%s-%s".formatted(handle.getWorkflowId(), idempotencyKey);
            var sendHandle = dbos.retrieveWorkflow(workflowId);
            assertEquals("SUCCESS", sendHandle.getStatus().getStatus());

            assertEquals("42-test.message", handle.getResult());
        } finally {
            dbos.shutdown();
        }
    }

}
