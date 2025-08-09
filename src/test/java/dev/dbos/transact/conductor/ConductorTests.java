package dev.dbos.transact.conductor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import dev.dbos.transact.conductor.TestWebSocketServer.WebSocketTestListener;
import dev.dbos.transact.conductor.protocol.CancelRequest;
import dev.dbos.transact.conductor.protocol.SuccessResponse;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.json.JSONUtil;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.java_websocket.WebSocket;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ClientHandshake;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConductorTests {

    SystemDatabase mockDB;
    DBOSExecutor mockExec;
    Conductor.Builder builder;
    static TestWebSocketServer testServer;
    static String domain;

    private static int findFreePort() {
        int port = 0;
        // For ServerSocket port number 0 means that the port number is automatically
        // allocated.
        try (ServerSocket socket = new ServerSocket(0)) {
            // Disable timeout and reuse address after closing the socket.
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
        } catch (IOException ignored) {
        }
        if (port > 0) {
            return port;
        }
        throw new RuntimeException("Could not find a free port");
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        int port = findFreePort();
        domain = String.format("ws://localhost:%d", port);

        System.out.println(String.format("ConductorTest %s", domain));

        testServer = new TestWebSocketServer(port);
        testServer.start();
    }

    @AfterAll
    static void afterAll() throws Exception {
        testServer.stop();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        mockDB = mock(SystemDatabase.class);
        mockExec = mock(DBOSExecutor.class);
        when(mockExec.getAppName()).thenReturn("test-app-name");
        builder = new Conductor.Builder(mockDB, mockExec, "conductor-key")
                .domain(domain);
    }

    @AfterEach
    void afterEach() throws Exception {
        testServer.setListener(null);
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

            listener.latch.await(10, TimeUnit.SECONDS);

            assertEquals("/conductor/v1alpha1/websocket/test-app-name/conductor-key", listener.resourceDescriptor);
        }
    }

    @Test
    public void sendsPing() throws Exception {
        class Listener implements WebSocketTestListener {
            CountDownLatch latch = new CountDownLatch(3);
            boolean onCloseCalled = false;
            int pingCount = 0;

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

            listener.latch.await(10, TimeUnit.SECONDS);
            assertTrue(listener.pingCount >= 3);
            assertFalse(listener.onCloseCalled);
        }
    }

    @Test
    public void reconnectsOnFailedPing() throws Exception {
        class Listener implements WebSocketTestListener {
            int openCount = 0;
            int closeCount = 0;
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
                closeCount++;
                latch.countDown();
            }
        }

        Listener listener = new Listener();
        testServer.setListener(listener);

        builder.pingPeriodMs(2000).pingTimeoutMs(1000);
        try (Conductor conductor = builder.build()) {
            conductor.start();

            listener.latch.await(10, TimeUnit.SECONDS);
            assertTrue(listener.openCount >= 2);
            assertTrue(listener.closeCount >= 2);
        }
    }

    @Test
    public void reconnectsOnRemoteClose() throws Exception {
        class Listener implements WebSocketTestListener {
            int openCount = 0;
            int closeCount = 0;
            CountDownLatch latch = new CountDownLatch(3);
            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

            @Override
            public void onOpen(WebSocket conn, ClientHandshake handshake) {
                openCount++;
                latch.countDown();
                scheduler.schedule(() -> {
                    conn.close();
                }, 1, TimeUnit.SECONDS);
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

            listener.latch.await(10, TimeUnit.SECONDS);
            assertTrue(listener.openCount >= 3);
            assertTrue(listener.closeCount >= 2);
        }
    }

    @Test
    @Disabled("skip for now")
    public void canCancel() throws Exception {
        class Listener implements WebSocketTestListener {
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
        }

        Listener listener = new Listener();
        testServer.setListener(listener);

        try (Conductor conductor = builder.build()) {
            conductor.start();

            listener.openLatch.await(1, TimeUnit.SECONDS);

            CancelRequest req = new CancelRequest("12345", "sample-wf-id");
            String json = JSONUtil.toJson(req);
            listener.webSocket.send(json);
            listener.messageLatch.await(1, TimeUnit.SECONDS);

            SuccessResponse resp = JSONUtil.fromJson(listener.message, SuccessResponse.class);
            assertEquals(new SuccessResponse(req, true), resp);
        }
    }

}
