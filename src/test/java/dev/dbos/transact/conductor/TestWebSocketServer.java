package dev.dbos.transact.conductor;

import java.net.InetSocketAddress;

import org.java_websocket.WebSocket;
import org.java_websocket.framing.Framedata;
import org.java_websocket.framing.PingFrame;
import org.java_websocket.framing.PongFrame;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestWebSocketServer extends WebSocketServer {
    public interface WebSocketTestListener {
        default void onStart() {
        }

        default void onPing(WebSocket conn, Framedata frame) {
            conn.sendFrame(new PongFrame((PingFrame) frame));
        }

        default void onError(WebSocket conn, Exception ex) {
        }

        default void onOpen(WebSocket conn, ClientHandshake handshake) {
        }

        default void onMessage(WebSocket conn, String message) {
        }

        default void onClose(WebSocket conn, int code, String reason, boolean remote) {
        }
    }

    private Logger logger = LoggerFactory.getLogger(ConductorTests.class);
    private WebSocketTestListener listener;

    public TestWebSocketServer(int port) {
        super(new InetSocketAddress(port));
    }

    public void setListener(WebSocketTestListener listener) {
        this.listener = listener;
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        logger.info("onOpen");
        if (listener != null) {
            listener.onOpen(conn, handshake);
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        logger.info("onClose");
        if (listener != null) {
            listener.onClose(conn, code, reason, remote);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        logger.info("onMessage, message {} ", message);
        if (listener != null) {
            listener.onMessage(conn, message);
        }
    }

    @Override
    public void onWebsocketPing(WebSocket conn, Framedata f) {
        logger.info("onWebsocketPing");
        if (listener != null) {
            listener.onPing(conn, f);
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        logger.error("onError", ex);
        if (listener != null) {
            listener.onError(conn, ex);
        }
    }

    @Override
    public void onStart() {
        logger.info("onStart");
        if (listener != null) {
            listener.onStart();
        }
    }
}