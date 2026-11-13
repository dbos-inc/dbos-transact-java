package dev.dbos.transact.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.internal.Entitlements.Answer;
import dev.dbos.transact.internal.Entitlements.Feature;
import dev.dbos.transact.utils.LogCapture;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import ch.qos.logback.classic.Level;
import com.sun.net.httpserver.HttpServer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class EntitlementsTest {

  private static final Feature SOME_FEATURE = new Feature("someFeature", "Some feature");
  private static final Feature OTHER_FEATURE = new Feature("otherFeature", "Doing another thing");
  private static final String HOSTED_ROUTE = "https://cloud.dbos.dev/conductor/v2/entitlements";

  /** A local stand-in for Conductor's entitlements route. */
  static final class FakeConductor implements AutoCloseable {
    final HttpServer server;
    final String url;
    final List<Map.Entry<String, @Nullable String>> requests =
        Collections.synchronizedList(new ArrayList<>());
    volatile int status = 200;
    volatile String body = "{\"entitlements\": {\"someFeature\": true}}";
    volatile Map<String, String> headers = Map.of();
    // When set, the answer's headers go out, then its body stalls until the server closes.
    volatile boolean stall = false;
    final CountDownLatch closed = new CountDownLatch(1);

    FakeConductor() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/",
          exchange -> {
            requests.add(
                new java.util.AbstractMap.SimpleEntry<>(
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders().getFirst("Authorization")));
            var payload = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            headers.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
            exchange.sendResponseHeaders(status, payload.length);
            try (var out = exchange.getResponseBody()) {
              if (stall) {
                out.write(payload, 0, 1);
                out.flush();
                try {
                  closed.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                return;
              }
              out.write(payload);
            }
          });
      server.start();
      url = "ws://127.0.0.1:" + server.getAddress().getPort();
    }

    void reject(int status, String detail) {
      reject(status, detail, Map.of());
    }

    void reject(int status, String detail, Map<String, String> headers) {
      this.status = status;
      this.body =
          "{\"title\": \"Rejected\", \"status\": " + status + ", \"detail\": \"" + detail + "\"}";
      this.headers = headers;
    }

    void entitle(String json) {
      this.status = 200;
      this.body = "{\"entitlements\": " + json + "}";
    }

    @Override
    public void close() {
      closed.countDown();
      server.stop(0);
    }
  }

  private FakeConductor conductor;

  @BeforeEach
  void startConductor() throws IOException {
    conductor = new FakeConductor();
  }

  @AfterEach
  void stopConductor() {
    conductor.close();
  }

  /** Checks against the fake Conductor, over HTTP. */
  private void check(@Nullable String enterpriseKey, @Nullable String conductorKey) {
    check(List.of(SOME_FEATURE), enterpriseKey, conductorKey);
  }

  private void check(
      List<Feature> features, @Nullable String enterpriseKey, @Nullable String conductorKey) {
    Entitlements.require(features, enterpriseKey, conductorKey, conductor.url, false);
  }

  private IllegalStateException refused(@Nullable String enterpriseKey) {
    return assertThrows(IllegalStateException.class, () -> check(enterpriseKey, null));
  }

  /** Checks against DBOS's hosted Conductor, answered by {@code http}. */
  private static void checkHosted(@Nullable String dbosDomain, Entitlements.Http http) {
    Entitlements.check(List.of(SOME_FEATURE), "enterprise-key", null, null, dbosDomain, http);
  }

  private static Entitlements.Http answering(int status, @Nullable String errorCode) {
    return (uri, headers) -> new Answer(status, errorCode, "");
  }

  @Test
  public void entitlementUrlSitsBesideTheConductorUrl() throws Exception {
    assertEquals(HOSTED_ROUTE, Entitlements.entitlementUrl(Entitlements.HOSTED_CONDUCTOR_URL));
    assertEquals(
        HOSTED_ROUTE, Entitlements.entitlementUrl("wss://cloud.dbos.dev/conductor/v1alpha1/"));
    // Credentials in the URL are dropped, so they are never sent or quoted in errors.
    assertEquals(
        "https://conductor.example.com:8443/conductor/v2/entitlements",
        Entitlements.entitlementUrl(
            "wss://user:secret@conductor.example.com:8443/conductor/v1alpha1"));
    assertEquals(
        "http://[::1]:8090/v2/entitlements",
        Entitlements.entitlementUrl("ws://user:secret@[::1]:8090"));
    // A self-hosted Conductor serves its routes from the root.
    assertEquals(
        "http://localhost:8090/v2/entitlements",
        Entitlements.entitlementUrl("ws://localhost:8090"));
    assertEquals(
        "https://conductor.example.com/v2/entitlements",
        Entitlements.entitlementUrl("https://conductor.example.com/"));
  }

  @Test
  public void anInvalidConductorUrlIsRefused() {
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                Entitlements.require(
                    List.of(SOME_FEATURE), "enterprise-key", null, "localhost:8090", false));
    assertTrue(e.getMessage().contains("the Conductor URL is invalid"), e.getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // An unencoded % in the password, which java.net.URI rejects quoting the whole URL.
        "wss://user:p%zzsecret@host/conductor/v1alpha1",
        // A space in the password.
        "wss://user:pa secret@host/conductor/v1alpha1",
        // No scheme, so the user name reads as one.
        "user:secret@host",
        "htp://user:secret@host"
      })
  public void anInvalidConductorUrlIsNeverQuoted(String url) {
    var e =
        assertThrows(
            IllegalStateException.class,
            () -> Entitlements.require(List.of(SOME_FEATURE), "enterprise-key", null, url, false));
    assertTrue(e.getMessage().contains("the Conductor URL is invalid"), e.getMessage());
    assertFalse(e.getMessage().contains("secret"), e.getMessage());
    assertFalse(e.getMessage().contains("user"), e.getMessage());
    // Nor in the stack trace.
    assertEquals(null, e.getCause());
  }

  @Test
  public void theDefaultConductorFollowsTheDbosDomain() {
    assertEquals(Entitlements.HOSTED_CONDUCTOR_URL, Entitlements.defaultConductorUrl(null));
    assertEquals(Entitlements.HOSTED_CONDUCTOR_URL, Entitlements.defaultConductorUrl(" "));
    assertEquals(
        "wss://staging.dbos.dev/conductor/v1alpha1",
        Entitlements.defaultConductorUrl("staging.dbos.dev"));
  }

  @Test
  public void anEntitledKeyUnlocksEnterpriseFeatures() {
    check("enterprise-key", null);
    check(null, "conductor-key");
    // The enterprise key is checked when both are set.
    check("enterprise-key", "conductor-key");
    assertEquals(
        List.of(
            Map.entry("/v2/entitlements", "Bearer enterprise-key"),
            Map.entry("/v2/entitlements", "Bearer conductor-key"),
            Map.entry("/v2/entitlements", "Bearer enterprise-key")),
        conductor.requests.stream().map(e -> Map.entry(e.getKey(), e.getValue())).toList());
  }

  @Test
  public void aBlankEnterpriseKeyFallsBackToTheConductorKey() {
    check(" ", "conductor-key");
    check("", "conductor-key");
    assertEquals(2, conductor.requests.size());
    conductor.requests.forEach(r -> assertEquals("Bearer conductor-key", r.getValue()));
  }

  @Test
  public void surroundingWhitespaceIsNotPartOfTheKey() {
    // As read from a secret file that ends in a newline.
    check("enterprise-key\n", null);
    check(null, " conductor-key\r\n");
    assertEquals("Bearer enterprise-key", conductor.requests.get(0).getValue());
    assertEquals("Bearer conductor-key", conductor.requests.get(1).getValue());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"enterprise\nkey", "enterprise\u20ackey", "enterprise\u0100key", "enterprise key"})
  public void aKeyThatIsNotPrintableAsciiIsRefusedWithoutQuotingIt(String key) {
    var e = refused(key);
    assertTrue(e.getMessage().contains("other than printable ASCII"), e.getMessage());
    assertFalse(e.getMessage().contains("enterprise"), e.getMessage());
    assertEquals(List.of(), conductor.requests);
    // Against the hosted Conductor too, where an outage would start anyway.
    var asked = new ArrayList<URI>();
    var hosted =
        assertThrows(
            IllegalStateException.class,
            () ->
                Entitlements.check(
                    List.of(SOME_FEATURE),
                    key,
                    null,
                    null,
                    null,
                    (uri, headers) -> {
                      asked.add(uri);
                      return new Answer(200, null, "{\"entitlements\": {\"someFeature\": true}}");
                    }));
    assertFalse(hosted.getMessage().contains("enterprise"), hosted.getMessage());
    assertEquals(List.of(), asked);
  }

  @Test
  public void aRequestTheHttpClientRefusesFailsClosedEvenWhenHosted() {
    // Not an outage, so the hosted Conductor's fail-open does not apply. The client's message,
    // which can quote the Authorization header, is not passed on.
    Entitlements.Http refusing =
        (uri, headers) -> {
          throw new IllegalArgumentException("invalid header value: \"Bearer secret\"");
        };
    try (var logs = new LogCapture(Entitlements.class)) {
      var e = assertThrows(IllegalStateException.class, () -> checkHosted(null, refusing));
      assertTrue(e.getMessage().contains("could not be sent"), e.getMessage());
      assertFalse(e.getMessage().contains("secret"), e.getMessage());
      assertEquals(null, e.getCause());
      assertEquals(List.of(), logs.messages(Level.WARN, ""));
    }
  }

  @Test
  public void anOversizedAnswerCountsAsAnOutage() {
    conductor.body =
        "{\"entitlements\": {\"someFeature\": true}, \"padding\": \""
            + "x".repeat(Entitlements.MAX_ANSWER_BYTES)
            + "\"}";
    var message = refused("enterprise-key").getMessage();
    assertTrue(
        message.contains("the answer is larger than " + Entitlements.MAX_ANSWER_BYTES + " bytes"),
        message);
    assertTrue(message.contains("Retry once Conductor is reachable"), message);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\n"})
  public void aBlankConductorUrlMeansTheHostedOne(String url) {
    var asked = new ArrayList<URI>();
    Entitlements.check(
        List.of(SOME_FEATURE),
        "enterprise-key",
        null,
        url,
        null,
        (uri, headers) -> {
          asked.add(uri);
          return new Answer(200, null, "{\"entitlements\": {\"someFeature\": true}}");
        });
    assertEquals(List.of(URI.create(HOSTED_ROUTE)), asked);
  }

  @Test
  public void anAnswerThatStallsMidBodyTimesOut() {
    // Headers arrive, then the body never finishes: the deadline covers the whole exchange.
    conductor.stall = true;
    var e =
        assertTimeoutPreemptively(
            Duration.ofSeconds(10),
            () ->
                assertThrows(
                    IllegalStateException.class,
                    () ->
                        Entitlements.check(
                            List.of(SOME_FEATURE),
                            "enterprise-key",
                            null,
                            conductor.url,
                            null,
                            Entitlements.http(Duration.ofMillis(500)))));
    assertTrue(e.getMessage().contains("no answer within 500 ms"), e.getMessage());
    assertTrue(e.getMessage().contains("Retry once Conductor is reachable"), e.getMessage());
  }

  @Test
  public void eachFeatureNeedsItsOwnEntitlement() {
    conductor.entitle("{\"someFeature\": true, \"otherFeature\": false, \"sharding\": true}");
    check("enterprise-key", null);
    var e =
        assertThrows(
            IllegalStateException.class,
            () -> check(List.of(SOME_FEATURE, OTHER_FEATURE), "enterprise-key", null));
    var message = e.getMessage();
    // Names only what the org lacks, by entitlement and by what it does.
    assertTrue(message.contains("not entitled to otherFeature (Doing another thing)"), message);
    assertTrue(message.contains("plan does not include it"), message);
    assertTrue(message.contains("DBOS Conductor Software License"), message);
    assertFalse(message.contains("someFeature"), message);
    assertFalse(message.contains("enterprise-key"), message);
    // One request answers for every feature.
    assertEquals(2, conductor.requests.size());
  }

  @Test
  public void aPastLimitOrgIsToldToUpgrade() {
    // Conductor refuses a past-limit org outright, tagging the 403 with a code.
    conductor.reject(
        403,
        "your DBOS account is past the free trial limit",
        Map.of("X-DBOS-Error", "past_limit"));
    var message = refused("enterprise-key").getMessage();
    assertTrue(
        message.contains("past its plan limit, so it is not entitled to someFeature"), message);
    assertTrue(message.contains("Upgrade its plan"), message);
    // The code, not the status or the prose, marks it: a bare 403 is another refusal.
    conductor.reject(403, "your DBOS account is past the free trial limit");
    message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("refused the check (HTTP 403)"), message);
    assertFalse(message.contains("Upgrade its plan"), message);
  }

  @Test
  public void anEntitlementConductorDoesNotReportIsNotGranted() {
    // A Conductor older than the feature doesn't list it, which grants nothing.
    conductor.entitle("{\"autoUpgrade\": true}");
    var message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("not entitled to someFeature"), message);
    // Anything but true grants nothing either.
    conductor.entitle("{\"someFeature\": \"yes\"}");
    message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("not entitled to someFeature"), message);
  }

  @Test
  public void enterpriseFeaturesRequireAKey() {
    // DBOS's hosted Conductor, implicit or named, is never asked without a key.
    var asked = new ArrayList<URI>();
    Entitlements.Http http =
        (uri, headers) -> {
          asked.add(uri);
          return new Answer(200, null, "{\"entitlements\": {\"someFeature\": true}}");
        };
    for (var url : new String[] {null, Entitlements.HOSTED_CONDUCTOR_URL}) {
      var e =
          assertThrows(
              IllegalStateException.class,
              () -> Entitlements.check(List.of(SOME_FEATURE), null, null, url, null, http));
      assertTrue(
          e.getMessage().startsWith("Some feature requires a DBOS key. Set transactEnterpriseKey"),
          e.getMessage());
    }
    assertEquals(List.of(), asked);
  }

  @Test
  public void aSelfHostedConductorIsAskedWithoutAKey() {
    // Without OAuth, a self-hosted Conductor grants every feature to any caller.
    check(null, null);
    assertEquals(1, conductor.requests.size());
    assertEquals("/v2/entitlements", conductor.requests.get(0).getKey());
    assertEquals(null, conductor.requests.get(0).getValue());
    // With OAuth, it wants a key.
    conductor.reject(401, "this endpoint requires a DBOS API key");
    var message = refused(null).getMessage();
    assertTrue(message.contains("requires a DBOS key: Conductor at"), message);
    assertTrue(message.contains("(HTTP 401): this endpoint requires a DBOS API key"), message);
    assertTrue(message.contains("Set transactEnterpriseKey"), message);
  }

  @Test
  public void conductorCredentialsNeverReachErrors() {
    conductor.reject(401, "invalid API key");
    var withCredentials = conductor.url.replace("ws://", "ws://user:secret@");
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                Entitlements.require(
                    List.of(SOME_FEATURE), "enterprise-key", null, withCredentials, false));
    assertTrue(e.getMessage().contains("rejected the key (HTTP 401)"), e.getMessage());
    assertFalse(e.getMessage().contains("secret"), e.getMessage());
    assertFalse(e.getMessage().contains("user"), e.getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "invalid API key",
        // A user credential, which the route refuses in favor of an API key.
        "this endpoint requires a DBOS API key"
      })
  public void aRejectedKeyIsRefused(String detail) {
    conductor.reject(401, detail);
    var message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("requires a valid DBOS key."), message);
    assertTrue(message.contains("rejected the key (HTTP 401): " + detail), message);
    assertFalse(message.contains("enterprise-key"), message);
  }

  @ParameterizedTest
  @CsvSource({"403, forbidden", "404, organization not found", "409, conflict"})
  public void anyOtherRefusalQuotesConductor(int status, String detail) {
    conductor.reject(status, detail);
    var message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("refused the check (HTTP " + status + "): " + detail), message);
    assertFalse(message.contains("rejected the key"), message);
    assertFalse(message.contains("enterprise-key"), message);
  }

  @Test
  public void aConductorWithoutTheRouteIsRefused() {
    // An older Conductor's router answers an unknown route with no problem detail.
    conductor.status = 404;
    conductor.body = "404 page not found";
    var message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("has no entitlements route (HTTP 404)"), message);
  }

  @Test
  public void aRedirectIsRefusedAndNeverCarriesTheKey() throws IOException {
    // Following a redirect would re-send the request, headers included, to the host the Location
    // names. That would hand the key to whoever answered.
    try (var elsewhere = new FakeConductor()) {
      var target = elsewhere.url.replace("ws://", "http://");
      conductor.status = 302;
      conductor.headers = Map.of("Location", target + "/v2/entitlements");
      var message = refused("enterprise-key").getMessage();
      assertTrue(message.contains("refused the check (HTTP 302)"), message);
      // The redirect target never saw the key, or anything at all.
      assertEquals(List.of(), elsewhere.requests);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "{\"entitlements\": null}", "{\"entitled\": true}", "not json", ""})
  public void aMalformedAnswerCountsAsAnOutage(String body) {
    conductor.body = body;
    var message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("Retry once Conductor is reachable"), message);
  }

  @Test
  public void anUnreachableConductorBlocksStartup() throws IOException {
    conductor.reject(503, "unavailable");
    var message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("checking it with Conductor at"), message);
    assertTrue(message.contains("failed (HTTP 503: unavailable)"), message);
    // A failure without a problem detail is reported by status alone.
    conductor.status = 502;
    conductor.body = "Bad Gateway";
    message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("failed (HTTP 502)."), message);
    // A port nothing listens on.
    int closed;
    try (var socket = new ServerSocket(0)) {
      closed = socket.getLocalPort();
    }
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                Entitlements.require(
                    List.of(SOME_FEATURE),
                    "enterprise-key",
                    null,
                    "ws://127.0.0.1:" + closed,
                    false));
    assertTrue(e.getMessage().contains("checking it with Conductor"), e.getMessage());
    assertTrue(e.getMessage().contains("failed"), e.getMessage());
    assertFalse(e.getMessage().contains("enterprise-key"), e.getMessage());
  }

  @Test
  public void aHostedConductorOutageDoesNotBlockStartup() {
    var asked = new ArrayList<URI>();
    Entitlements.Http unreachable =
        (uri, headers) -> {
          asked.add(uri);
          throw new ConnectException("connection refused");
        };
    // No Conductor URL means DBOS's hosted one, whose outage only warns.
    try (var logs = new LogCapture(Entitlements.class)) {
      assertDoesNotThrow(() -> checkHosted(null, unreachable));
      var warnings = logs.messages(Level.WARN, "Could not verify the DBOS key");
      assertEquals(1, warnings.size(), warnings.toString());
      assertTrue(warnings.get(0).contains("connection refused"), warnings.get(0));
      assertFalse(warnings.get(0).contains("enterprise-key"), warnings.get(0));
    }
    assertEquals(List.of(URI.create(HOSTED_ROUTE)), asked);
    // Another domain is not DBOS's hosted Conductor, so its outage still refuses.
    var e =
        assertThrows(
            IllegalStateException.class, () -> checkHosted("unreachable.example.com", unreachable));
    assertTrue(e.getMessage().contains("Retry once Conductor is reachable"), e.getMessage());
  }

  @ParameterizedTest
  @ValueSource(ints = {403, 407, 408, 425, 429, 500, 503})
  public void hostedThrottlingAndProxiesCountAsOutages(int status) {
    // From the hosted Conductor these mean it couldn't answer, not that it refused the key, so
    // startup warns and goes on as for any hosted outage.
    try (var logs = new LogCapture(Entitlements.class)) {
      assertDoesNotThrow(() -> checkHosted(null, answering(status, null)));
      var warnings = logs.messages(Level.WARN, "(HTTP " + status + ")");
      assertEquals(1, warnings.size(), warnings.toString());
    }
  }

  @Test
  public void hostedRefusalsStillRefuse() {
    var e =
        assertThrows(
            IllegalStateException.class, () -> checkHosted(null, answering(403, "past_limit")));
    assertTrue(e.getMessage().contains("past its plan limit"), e.getMessage());
    e = assertThrows(IllegalStateException.class, () -> checkHosted(null, answering(401, null)));
    assertTrue(e.getMessage().contains("rejected the key (HTTP 401)"), e.getMessage());
  }

  @Test
  public void anotherConductorsThrottlingStillRefuses() {
    conductor.reject(429, "slow down");
    var message = refused("enterprise-key").getMessage();
    assertTrue(message.contains("refused the check (HTTP 429)"), message);
  }

  @Test
  public void dbosCloudPassesWithoutACheck() {
    Entitlements.require(List.of(SOME_FEATURE), null, null, conductor.url, true);
    assertEquals(List.of(), conductor.requests);
  }

  @Test
  public void noFeatureNeedsNoCheck() {
    Entitlements.require(List.of(), null, null, conductor.url, false);
    Entitlements.require(List.of(), null, null, null, false);
    assertEquals(List.of(), conductor.requests);
  }

  @Test
  public void aDefaultConfigTurnsOnNoEnterpriseFeature() {
    assertEquals(List.of(), Entitlements.enabledFeatures(DBOSConfig.defaults("app")));
  }

  @Test
  public void theRequestNamesTheSdk() {
    var sent = new LinkedHashMap<String, String>();
    Entitlements.check(
        List.of(SOME_FEATURE),
        "enterprise-key",
        null,
        conductor.url,
        null,
        (uri, headers) -> {
          sent.putAll(headers);
          return new Answer(200, null, "{\"entitlements\": {\"someFeature\": true}}");
        });
    assertTrue(sent.get("User-Agent").startsWith("dbos-transact-java/"), sent.toString());
    assertEquals("application/json", sent.get("Accept"));
    assertEquals("Bearer enterprise-key", sent.get("Authorization"));
  }
}
