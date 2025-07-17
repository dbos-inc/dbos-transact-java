package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSOptions;
import dev.dbos.transact.context.SetDBOSOptions;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class UnifiedProxyTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource ;
    private DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        UnifiedProxyTest.dbosConfig = new DBOSConfig
                .Builder()
                .name("systemdbtest")
                .dbHost("localhost")
                .dbPort(5432)
                .dbUser("postgres")
                .sysDbName("dbos_java_sys")
                .maximumPoolSize(2)
                .build();

        String dbUrl = String.format("jdbc:postgresql://%s:%d/%s", dbosConfig.getDbHost(), dbosConfig.getDbPort(), "postgres");

        String sysDb = dbosConfig.getSysDbName();
        try (Connection conn = DriverManager.getConnection(dbUrl, dbosConfig.getDbUser(), dbosConfig.getDbPassword());
             Statement stmt = conn.createStatement()) {


            String dropDbSql = String.format("DROP DATABASE IF EXISTS %s", sysDb);
            String createDbSql = String.format("CREATE DATABASE %s", sysDb);
            stmt.execute(dropDbSql);
            stmt.execute(createDbSql);
        }

    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        UnifiedProxyTest.dataSource = DBUtils.createDataSource(dbosConfig) ;
        DBOS.initialize(dbosConfig);
        dbos = DBOS.getInstance();
        SystemDatabase.initialize(dataSource);
        systemDatabase = SystemDatabase.getInstance();
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos.setDbosExecutor(dbosExecutor);
        dbos.launch();
        DBUtils.clearTables(dataSource);
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
    }


    @Test
    public void optionsWithCall() throws Exception  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();


        // synchronous
        String wfid1 = "wf-123";
        DBOSOptions options = new DBOSOptions.Builder(wfid1).build();
        String result;
        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.workWithString("test-item");
        }
        assertEquals("Processed: test-item", result);

        // asynchronous

        String wfid2 = "wf-124";
        options = new DBOSOptions.Builder(wfid2).async().build();
        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.workWithString("test-item-async");
        }
        assertNull(result);

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(wfid2); ;
        result = handle.getResult();
        assertEquals("Processed: test-item-async", result);
        assertEquals(wfid2, handle.getWorkflowId());
        assertEquals("SUCCESS", handle.getStatus().getStatus()) ;

        // Queued
        String wfid3 = "wf-125";
        Queue q= new DBOS.QueueBuilder("simpleQ").build();
        options = new DBOSOptions.Builder(wfid3).queue(q).build();
        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.workWithString("test-item-q");
        }
        assertNull(result);

        handle = dbosExecutor.retrieveWorkflow(wfid3); ;
        result = handle.getResult();
        assertEquals("Processed: test-item-q", result);
        assertEquals(wfid3, handle.getWorkflowId());
        assertEquals("SUCCESS", handle.getStatus().getStatus()) ;

        ListWorkflowsInput input = new ListWorkflowsInput();
        input.setWorkflowIDs(Arrays.asList(wfid3));
        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(input) ;
        assertEquals("simpleQ", wfs.get(0).getQueueName());

    }

}
