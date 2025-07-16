package dev.dbos.transact.notifications;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EventsTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource ;
    private DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;
    private NotificationService notificationService;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        EventsTest.dbosConfig = new DBOSConfig
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
        EventsTest.dataSource = DBUtils.createDataSource(dbosConfig) ;
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
    public void basic_set_get() throws Exception {

        EventsService eventService = dbos.<EventsService>Workflow()
                .interfaceClass(EventsService.class)
                .implementation(new EventsServiceImpl(dbos))
                .build();

        try (SetWorkflowID id = new SetWorkflowID("id1")) {
            eventService.setEventWorkflow("key1", "value1");
        }

        try (SetWorkflowID id = new SetWorkflowID("id2")) {
            Object event = eventService.getEventWorkflow("id1", "key1", 3);
            assertEquals("value1", (String)event);
        }

        // outside workflow
        String val = (String)dbos.getEvent("id1", "key1", 3);
        assertEquals("value1", val);

    }

    @Test
    public void multipleEvents() throws Exception {

        EventsService eventService = dbos.<EventsService>Workflow()
                .interfaceClass(EventsService.class)
                .implementation(new EventsServiceImpl(dbos))
                .build();

        try (SetWorkflowID id = new SetWorkflowID("id1")) {
            eventService.setMultipleEvents();
        }

        try (SetWorkflowID id = new SetWorkflowID("id2")) {
            Object event = eventService.getEventWorkflow("id1", "key1", 3);
            assertEquals("value1", (String)event);
        }

        // outside workflow
        Double val = (Double)dbos.getEvent("id1", "key2", 3);
        assertEquals(241.5, val);

    }

    @Test
    public void async_set_get() throws Exception {

        EventsService eventService = dbos.<EventsService>Workflow()
                .interfaceClass(EventsService.class)
                .implementation(new EventsServiceImpl(dbos))
                .async()
                .build();

        try (SetWorkflowID id = new SetWorkflowID("id1")) {
            eventService.setEventWorkflow("key1", "value1");
        }

        DBOS.retrieveWorkflow("id1").getResult();

        try (SetWorkflowID id = new SetWorkflowID("id2")) {
            eventService.getEventWorkflow("id1", "key1", 3);
        }

        String event = (String) DBOS.retrieveWorkflow("id2").getResult();
        assertEquals("value1", event);
    }


}
