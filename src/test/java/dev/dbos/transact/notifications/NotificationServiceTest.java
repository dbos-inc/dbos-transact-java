package dev.dbos.transact.notifications;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

class NotificationServiceTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource ;
    private DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;
    private NotificationService notificationService;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        NotificationServiceTest.dbosConfig = new DBOSConfig
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
        NotificationServiceTest.dataSource = DBUtils.createDataSource(dbosConfig) ;
        DBOS.initialize(dbosConfig);
        dbos = DBOS.getInstance();
        SystemDatabase.initialize(dataSource);
        systemDatabase = SystemDatabase.getInstance();
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos.setDbosExecutor(dbosExecutor);
        // notificationService = new NotificationService(dataSource, systemDatabase);
        // dbos.setNotificationService(notificationService);
        dbos.launch();
        DBUtils.clearTables(dataSource);
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
    }


    @Test
    public void basic_send_recv() throws Exception  {

        NotService notService = dbos.<NotService>Workflow()
                .interfaceClass(NotService.class)
                .implementation(new NotServiceImpl(dbos))
                .async()
                .build();

        String wfid1 = "recvwf1";

        try(SetWorkflowID id = new SetWorkflowID(wfid1)) {
            notService.recvWorkflow("topic1") ;
        }

        String wfid2 = "sendf1";

        try(SetWorkflowID id = new SetWorkflowID(wfid2)) {
            notService.sendWorkflow(wfid1, "topic1", "HelloDBOS") ;
        }

        WorkflowHandle<String> handle1 = DBOS.retrieveWorkflow(wfid1);
        WorkflowHandle<String> handle2 = DBOS.retrieveWorkflow(wfid2);

        String result = handle1.getResult();
        assertEquals("HelloDBOS", result) ;

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
        assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());
    }

    @Test
    public void multiple_send_recv() throws Exception  {

        NotService notService = dbos.<NotService>Workflow()
                .interfaceClass(NotService.class)
                .implementation(new NotServiceImpl(dbos))
                .async()
                .build();

        String wfid1 = "recvwf1";

        try(SetWorkflowID id = new SetWorkflowID(wfid1)) {
            notService.recvMultiple("topic1") ;
        }


        try(SetWorkflowID id = new SetWorkflowID("send1")) {
            notService.sendWorkflow(wfid1, "topic1", "Hello1") ;
        }
        DBOS.retrieveWorkflow("send1").getResult();

        try(SetWorkflowID id = new SetWorkflowID("send2")) {
            notService.sendWorkflow(wfid1, "topic1", "Hello2") ;
        }
        DBOS.retrieveWorkflow("send2").getResult();

        try(SetWorkflowID id = new SetWorkflowID("send3")) {
            notService.sendWorkflow(wfid1, "topic1", "Hello3") ;
        }
        DBOS.retrieveWorkflow("send3").getResult();

        WorkflowHandle<String> handle1 = DBOS.retrieveWorkflow(wfid1);

        String result = handle1.getResult();
        assertEquals("Hello1Hello2Hello3", result) ;

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());

    }

    @Test
    public void notopic() throws Exception  {

        NotService notService = dbos.<NotService>Workflow()
                .interfaceClass(NotService.class)
                .implementation(new NotServiceImpl(dbos))
                .async()
                .build();

        String wfid1 = "recvwf1";

        try(SetWorkflowID id = new SetWorkflowID(wfid1)) {
            notService.recvWorkflow(null) ;
        }

        String wfid2 = "sendf1";

        try(SetWorkflowID id = new SetWorkflowID(wfid2)) {
            notService.sendWorkflow(wfid1, null, "HelloDBOS") ;
        }

        WorkflowHandle<String> handle1 = DBOS.retrieveWorkflow(wfid1);
        WorkflowHandle<String> handle2 = DBOS.retrieveWorkflow(wfid2);

        String result = handle1.getResult();
        assertEquals("HelloDBOS", result) ;

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
        assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());
    }

    @Test
    public void noWorkflowRecv() {
        try {
            dbos.recv("someTopic", 5);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertEquals("recv() must be called from a workflow.", e.getMessage());
        }
    }

    @Test
    public void sendNotexistingID() throws Exception  {

        NotService notService = dbos.<NotService>Workflow()
                .interfaceClass(NotService.class)
                .implementation(new NotServiceImpl(dbos))
                .build();

        try {
            try (SetWorkflowID id = new SetWorkflowID("send1")) {
                notService.sendWorkflow("fakeid", "topic1", "HelloDBOS");
            }
            assertTrue(false);
        } catch (NonExistentWorkflowException e) {
            assertEquals("fakeid", e.getWorkflowId()) ;
        }

    }

    @Test
    public void sendNull() throws Exception  {

        NotService notService = dbos.<NotService>Workflow()
                .interfaceClass(NotService.class)
                .implementation(new NotServiceImpl(dbos))
                .async()
                .build();

        String wfid1 = "recvwf1";

        try(SetWorkflowID id = new SetWorkflowID(wfid1)) {
            notService.recvWorkflow("topic1") ;
        }

        String wfid2 = "sendf1";

        try(SetWorkflowID id = new SetWorkflowID(wfid2)) {
            notService.sendWorkflow(wfid1, "topic1", null) ;
        }

        WorkflowHandle<String> handle1 = DBOS.retrieveWorkflow(wfid1);
        WorkflowHandle<String> handle2 = DBOS.retrieveWorkflow(wfid2);

        String result = handle1.getResult();
        assertNull(result) ;

        assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().getStatus());
        assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());
    }
}