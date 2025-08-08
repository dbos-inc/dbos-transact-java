package dev.dbos.transact.conductor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import org.java_websocket.WebSocket;
import org.java_websocket.framing.Framedata;
import org.java_websocket.framing.PingFrame;
import org.java_websocket.framing.PongFrame;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConductorTests {

    SystemDatabase mockDB;
    DBOSExecutor mockExec;
    Conductor.Builder builder;


    @BeforeEach
    void beforeEach() throws Exception {
        mockDB = mock(SystemDatabase.class);
        mockExec = mock(DBOSExecutor.class);
        when(mockExec.getAppName()).thenReturn("test-app-name");
        builder = new Conductor.Builder(mockDB, mockExec, "conductor-key")
            .domain("ws://localhost:8080");
    }

    @Test
    public void connectsToCorrectUrl() throws Exception {
        try (TestWebSocketServer mockWebSocketServer = new TestWebSocketServer(8080);
                Conductor conductor = builder.build()) {
            mockWebSocketServer.start();
            conductor.start();

            assertEquals("/conductor/v1alpha1/websocket/test-app-name/conductor-key",
                    mockWebSocketServer.getResourceDescriptor());
        }
    }

    // @Test
    // public void sendsPing() throws Exception {
    //     try (TestWebSocketServer mockWebSocketServer = new TestWebSocketServer(8080);
    //             Conductor conductor = builder.build()) {
    //         mockWebSocketServer.start();
    //         conductor.start();

    //         Thread.sleep(30000);
    //     }
    // }

    class TestWebSocketServer extends WebSocketServer implements AutoCloseable {

        private Logger logger = LoggerFactory.getLogger(ConductorTests.class);


        public TestWebSocketServer(int port) {
            super(new InetSocketAddress(port));
        }

        private int pingCount;
        private String resourceDescriptor;

        public String getResourceDescriptor() {
            return resourceDescriptor;
        }

        public java.util.function.BiConsumer<WebSocket, ClientHandshake> onOpenLambda;
        public java.util.function.Consumer<WebSocket> onCloseLambda;
        public java.util.function.BiConsumer<WebSocket, String> onMessageLambda;
        public java.util.function.BiConsumer<WebSocket, Exception> onErrorLambda;
        public Runnable onStartLambda;

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {
            resourceDescriptor = handshake.getResourceDescriptor();
            logger.info("onOpen {}", resourceDescriptor);
            pingCount = 0;
        }

        @Override
        public void onClose(WebSocket conn, int code, String reason, boolean remote) {
            logger.info("onClose");
        }

        @Override
        public void onMessage(WebSocket conn, String message) {
            logger.info("onMessage, message {} ", message);

        }
        @Override
        public void onWebsocketPing(WebSocket conn, Framedata f) {
            logger.info("onWebsocketPing {}", pingCount++);
            conn.sendFrame(new PongFrame((PingFrame) f));
        }

        @Override
        public void onError(WebSocket conn, Exception ex) {
            logger.error("onError", ex);
        }

        @Override
        public void onStart() {
            logger.info("onStart");
        }

        @Override
        public void close() throws Exception {
            this.stop();
        }
    }
}
