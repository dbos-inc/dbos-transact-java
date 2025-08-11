package dev.dbos.transact.conductor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import dev.dbos.transact.conductor.TestWebSocketServer.WebSocketTestListener;
import dev.dbos.transact.conductor.protocol.BaseMessage;
import dev.dbos.transact.conductor.protocol.CancelRequest;
import dev.dbos.transact.conductor.protocol.RestartRequest;
import dev.dbos.transact.conductor.protocol.ResumeRequest;
import dev.dbos.transact.conductor.protocol.SuccessResponse;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.json.JSONUtil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.WebSocket;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ClientHandshake;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConductorTests {

    static Logger logger = LoggerFactory.getLogger(ConductorTests.class);

    SystemDatabase mockDB;
    DBOSExecutor mockExec;
    Conductor.Builder builder;
    TestWebSocketServer testServer;

    final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void beforeEach() throws Exception {
        testServer = new TestWebSocketServer(0);
        testServer.start();
        testServer.waitStart(1000);

        int port = testServer.getPort();
        assertTrue(port != 0, "Invalid Web Socket Server port");
        String domain = String.format("ws://localhost:%d", port);

        mockDB = mock(SystemDatabase.class);
        mockExec = mock(DBOSExecutor.class);
        when(mockExec.getAppName()).thenReturn("test-app-name");
        builder = new Conductor.Builder(mockDB, mockExec, "conductor-key")
                .domain(domain);
    }

    @AfterEach
    void afterEach() throws Exception {
        testServer.stop();
    }

    @Test
    public void connectsToCorrectUrl() throws Exception {

        class Listener implements WebSocketTestListener {
            String resourceDescriptor;
            CountDownLatch latch = new CountDownLatch(1);

            @Override
            public void onOpen(WebSocket conn, ClientHandshake handshake) {
                resourceDescriptor = handshake.getResourceDescriptor();
                latch.countDown();
            }
        }

        Listener listener = new Listener();
        testServer.setListener(listener);

        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.latch.await(10, TimeUnit.SECONDS), "latch timed out");
            assertEquals("/conductor/v1alpha1/websocket/test-app-name/conductor-key", listener.resourceDescriptor);
        }
    }

    @Test
    public void sendsPing() throws Exception {
        logger.info("sendsPing Starting");
        class Listener implements WebSocketTestListener {
            CountDownLatch latch = new CountDownLatch(3);
            boolean onCloseCalled = false;

            @Override
            public void onPing(WebSocket conn, Framedata frame) {
                WebSocketTestListener.super.onPing(conn, frame);
                latch.countDown();
            }

            @Override
            public void onClose(WebSocket conn, int code, String reason, boolean remote) {
                this.onCloseCalled = true;
            }
        }

        Listener listener = new Listener();
        testServer.setListener(listener);

        builder.pingPeriodMs(2000).pingTimeoutMs(1000);
        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.latch.await(10, TimeUnit.SECONDS), "latch timed out");
            assertFalse(listener.onCloseCalled);
        } finally {
            logger.info("sendsPing ending");
        }
    }

    @Test
    public void reconnectsOnFailedPing() throws Exception {
        logger.info("reconnectsOnFailedPing Starting");
        class Listener implements WebSocketTestListener {
            int openCount = 0;
            CountDownLatch latch = new CountDownLatch(2);

            @Override
            public void onPing(WebSocket conn, Framedata frame) {
                // don't respond to pings
            }

            @Override
            public void onOpen(WebSocket conn, ClientHandshake handshake) {
                openCount++;
            }

            @Override
            public void onClose(WebSocket conn, int code, String reason, boolean remote) {
                latch.countDown();
            }
        }

        Listener listener = new Listener();
        testServer.setListener(listener);

        builder.pingPeriodMs(2000).pingTimeoutMs(1000);
        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.latch.await(15, TimeUnit.SECONDS), "latch timed out");
            assertTrue(listener.openCount >= 2);
        } finally {
            logger.info("reconnectsOnFailedPing ending");
        }
    }

    @Test
    public void reconnectsOnRemoteClose() throws Exception {
        class Listener implements WebSocketTestListener {
            int closeCount = 0;
            CountDownLatch latch = new CountDownLatch(3);
            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

            @Override
            public void onOpen(WebSocket conn, ClientHandshake handshake) {
                latch.countDown();
                if (latch.getCount() > 0) {
                    scheduler.schedule(() -> {
                        conn.close();
                    }, 1, TimeUnit.SECONDS);
                }
            }

            @Override
            public void onClose(WebSocket conn, int code, String reason, boolean remote) {
                closeCount++;
            }
        }

        Listener listener = new Listener();
        testServer.setListener(listener);

        builder.pingPeriodMs(2000).pingTimeoutMs(1000);
        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.latch.await(15, TimeUnit.SECONDS), "latch timed out");
            assertTrue(listener.closeCount >= 2);
        }
    }

    class MessageListener implements WebSocketTestListener {
        WebSocket webSocket;
        CountDownLatch openLatch = new CountDownLatch(1);
        String message;
        CountDownLatch messageLatch = new CountDownLatch(1);

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {
            this.webSocket = conn;
            openLatch.countDown();
        }

        @Override
        public void onMessage(WebSocket conn, String message) {
            this.message = message;
            messageLatch.countDown();
        }

        public void send(BaseMessage message) {
            this.webSocket.send(JSONUtil.toJson(message));
        }
    }

    @Test
    public void canCancel() throws Exception {
        MessageListener listener = new MessageListener();
        testServer.setListener(listener);
        String workflowId = "sample-wf-id";

        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

            CancelRequest req = new CancelRequest("12345", workflowId);
            listener.send(req);
            assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");

            // Verify that resumeWorkflow was called with the correct argument
            verify(mockExec).cancelWorkflow(workflowId);

            SuccessResponse resp = mapper.readValue(listener.message, SuccessResponse.class);
            assertEquals("cancel", resp.type);
            assertEquals("12345", resp.request_id);
            assertTrue(resp.success);
            assertNull(resp.error_message);
        }
    }

    @Test
    public void canCancelThrows() throws Exception {
        MessageListener listener = new MessageListener();
        testServer.setListener(listener);

        String errorMessage = "canCancelThrows error";
        String workflowId = "sample-wf-id";

        doThrow(new RuntimeException(errorMessage)).when(mockExec).cancelWorkflow(anyString());

        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

            CancelRequest req = new CancelRequest("12345", workflowId);
            listener.send(req);
            assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
            verify(mockExec).cancelWorkflow(workflowId);

            SuccessResponse resp = mapper.readValue(listener.message, SuccessResponse.class);
            assertEquals("cancel", resp.type);
            assertEquals("12345", resp.request_id);
            assertEquals(errorMessage, resp.error_message);
            assertFalse(resp.success);
        }
    }

    @Test
    public void canResume() throws Exception {
        MessageListener listener = new MessageListener();
        testServer.setListener(listener);
        String workflowId = "sample-wf-id";

        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

            ResumeRequest req = new ResumeRequest("12345", workflowId);
            listener.send(req);
            assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
            verify(mockExec).resumeWorkflow(workflowId);

            SuccessResponse resp = mapper.readValue(listener.message, SuccessResponse.class);
            assertEquals("resume", resp.type);
            assertEquals("12345", resp.request_id);
            assertTrue(resp.success);
            assertNull(resp.error_message);
        }
    }

    @Test
    public void canResumeThrows() throws Exception {
        MessageListener listener = new MessageListener();
        testServer.setListener(listener);

        String errorMessage = "canResumeThrows error";
        String workflowId = "sample-wf-id";

        doThrow(new RuntimeException(errorMessage)).when(mockExec).resumeWorkflow(workflowId);

        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

            ResumeRequest req = new ResumeRequest("12345", workflowId);
            listener.send(req);
            assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
            verify(mockExec).resumeWorkflow(workflowId);

            SuccessResponse resp = mapper.readValue(listener.message, SuccessResponse.class);
            assertEquals("resume", resp.type);
            assertEquals("12345", resp.request_id);
            assertEquals(errorMessage, resp.error_message);
            assertFalse(resp.success);
        }
    }

    @Test
    public void canRestart() throws Exception {
        MessageListener listener = new MessageListener();
        testServer.setListener(listener);
        String workflowId = "sample-wf-id";

        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

            RestartRequest req = new RestartRequest("12345", workflowId);
            listener.send(req);
            assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
            verify(mockExec).forkWorkflow(eq(workflowId), eq(0), any());

            SuccessResponse resp = mapper.readValue(listener.message, SuccessResponse.class);
            assertEquals("restart", resp.type);
            assertEquals("12345", resp.request_id);
            assertTrue(resp.success);
            assertNull(resp.error_message);
        }
    }

    @Test
    public void canRestartThrows() throws Exception {
        MessageListener listener = new MessageListener();
        testServer.setListener(listener);

        String errorMessage = "canRestartThrows error";
        String workflowId = "sample-wf-id";

        doThrow(new RuntimeException(errorMessage)).when(mockExec).forkWorkflow(anyString(), anyInt(), any());

        try (Conductor conductor = builder.build()) {
            conductor.start();

            assertTrue(listener.openLatch.await(5, TimeUnit.SECONDS), "open latch timed out");

            RestartRequest req = new RestartRequest("12345", workflowId);
            listener.send(req);
            assertTrue(listener.messageLatch.await(1, TimeUnit.SECONDS), "message latch timed out");
            verify(mockExec).forkWorkflow(eq(workflowId), eq(0), any());

            SuccessResponse resp = mapper.readValue(listener.message, SuccessResponse.class);
            assertEquals("restart", resp.type);
            assertEquals("12345", resp.request_id);
            assertEquals(errorMessage, resp.error_message);
            assertFalse(resp.success);
        }
    }

}
