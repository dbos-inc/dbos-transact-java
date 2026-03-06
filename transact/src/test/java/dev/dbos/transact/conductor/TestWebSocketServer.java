package dev.dbos.transact.conductor;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.java_websocket.WebSocket;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.exceptions.InvalidDataException;
import org.java_websocket.framing.Framedata;
import org.java_websocket.framing.PingFrame;
import org.java_websocket.framing.PongFrame;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestWebSocketServer extends WebSocketServer {
  public interface WebSocketTestListener {
    default void onStart() {}

    default void onPing(WebSocket conn, Framedata frame) {
      TestWebSocketServer.logger.info("sending PongFrame");
      conn.sendFrame(new PongFrame((PingFrame) frame));
    }

    default void onError(WebSocket conn, Exception ex) {}

    default void onOpen(WebSocket conn, ClientHandshake handshake) {}

    default void onMessage(WebSocket conn, String message) {}

    default void onWebsocketMessage(WebSocket conn, Framedata frame) {}

    default void onClose(WebSocket conn, int code, String reason, boolean remote) {}
  }

  private static final Logger logger = LoggerFactory.getLogger(TestWebSocketServer.class);
  private WebSocketTestListener listener;
  private Semaphore startEvent = new Semaphore(0);

  private static class InterceptingDraft extends Draft_6455 {
    TestWebSocketServer server;

    @Override
    public void processFrame(WebSocketImpl webSocketImpl, Framedata frame)
        throws InvalidDataException {
      if (server != null && server.listener != null) {
        server.listener.onWebsocketMessage(webSocketImpl, frame);
      }
      super.processFrame(webSocketImpl, frame);
    }

    @Override
    public Draft_6455 copyInstance() {
      InterceptingDraft copy = new InterceptingDraft();
      copy.server = this.server;
      return copy;
    }
  }

  public TestWebSocketServer(int port) {
    this(port, new InterceptingDraft());
  }

  private TestWebSocketServer(int port, InterceptingDraft draft) {
    super(new InetSocketAddress(port), Collections.singletonList(draft));
    draft.server = this;
  }

  public void setListener(WebSocketTestListener listener) {
    this.listener = listener;
  }

  public boolean waitStart(long millis) throws InterruptedException {
    return startEvent.tryAcquire(millis, TimeUnit.MILLISECONDS);
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
    startEvent.drainPermits();
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
    startEvent.drainPermits();
    logger.error("onError", ex);
    if (listener != null) {
      listener.onError(conn, ex);
    }
  }

  @Override
  public void onStart() {
    startEvent.release();
    logger.info("onStart {}", getPort());
    if (listener != null) {
      listener.onStart();
    }
  }
}
