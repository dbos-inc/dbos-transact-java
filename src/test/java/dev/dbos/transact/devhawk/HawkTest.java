package dev.dbos.transact.devhawk;

import java.sql.SQLException;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;

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

        var result = proxy.simpleWorkflow();
    }

    @Test 
    void testTwo() {

        var impl = new HawkServiceImpl();
        var proxy = dbos.<HawkService>Workflow().interfaceClass(HawkService.class).implementation(impl).build();
        impl.setProxy(proxy);

        dbos.launch();

        try (var _o = WorkflowOptions.setWorkflowId("wf1234")) {
            var result = proxy.simpleWorkflow();
        }
    }

    @Test 
    void testThree() {

        var impl = new HawkServiceImpl();
        var proxy = dbos.<HawkService>Workflow().interfaceClass(HawkService.class).implementation(impl).build();
        impl.setProxy(proxy);

        dbos.launch();

        var options = WorkflowOptions.builder().timeout(Duration.ofSeconds(1)).build();
        try (var _o = options.set()) {
            var result = proxy.recvWorkflow();
        }
    }

}
