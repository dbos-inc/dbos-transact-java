package dev.dbos.transact.devhawk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CancellationException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;

public class HawkTest {
    private static DBOSConfig dbosConfig;
    private DBOS dbos;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        dbosConfig = new DBOSConfig.Builder().name("systemdbtest")
                .dbHost("localhost").dbPort(5432).dbUser("postgres").sysDbName("dbos_java_sys")
                .maximumPoolSize(2).build();
    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        DBUtils.recreateDB(dbosConfig);
        dbos = DBOS.initialize(dbosConfig);
    }

    @AfterEach
    void afterEachTest() throws Exception {
        dbos.shutdown();
    }

    @Test
    void testOne() {

        var impl = new HawkServiceImpl();
        var proxy = dbos.<HawkService>Workflow().interfaceClass(HawkService.class).implementation(impl).build();
        impl.setProxy(proxy);

        dbos.launch();

        var result = proxy.simpleWorkflow(null);
        assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    @Test 
    void testTwo() {

        var impl = new HawkServiceImpl();
        var proxy = dbos.<HawkService>Workflow().interfaceClass(HawkService.class).implementation(impl).build();
        impl.setProxy(proxy);

        dbos.launch();

        String result = null;
        String workflowId = "wf1234";
        try (var _o = new WorkflowOptions(workflowId).setContext()) {
            result = proxy.simpleWorkflow(workflowId);
        }

        assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);

    }

    @Test 
    void testThree() {

        var impl = new HawkServiceImpl();
        var proxy = dbos.<HawkService>Workflow().interfaceClass(HawkService.class).implementation(impl).build();
        impl.setProxy(proxy);

        dbos.launch();

        var options = new WorkflowOptions(Duration.ofSeconds(1));
        try (var _o = options.setContext()) {
            assertThrows(CancellationException.class, () -> proxy.recvWorkflow());
        }
    }

}
