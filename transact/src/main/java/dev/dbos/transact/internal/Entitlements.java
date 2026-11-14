package dev.dbos.transact.internal;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * The entitlement check every Transact Enterprise feature passes before DBOS loads it: DBOS
 * Conductor must report the key's organization entitled to the feature.
 *
 * <p>Transact Enterprise ({@code dev.dbos:transact-enterprise}) is proprietary, not MIT-licensed
 * like this project. Its license (https://www.dbos.dev/conductor-license) forbids modifying it, and
 * makes disabling or circumventing the check here a material breach.
 */
public final class Entitlements {
  private static final Logger logger = LoggerFactory.getLogger(Entitlements.class);

  /**
   * A Transact Enterprise feature.
   *
   * @param entitlement the name Conductor reports the entitlement under, in camelCase
   * @param description what the feature does, as messages name it
   */
  public record Feature(String entitlement, String description) {}

  static final String ENTITLEMENTS_PATH = "/v2/entitlements";
  static final Duration TIMEOUT = Duration.ofSeconds(10);
  static final String HOSTED_CONDUCTOR_URL = "wss://cloud.dbos.dev/conductor/v1alpha1";
  // The hosted Conductor's entitlement route, which identifies it however its URL is written.
  static final String HOSTED_ROUTE = "https://cloud.dbos.dev/conductor" + ENTITLEMENTS_PATH;
  // Conductor's machine-readable error code, in this header, for an org past its plan limit.
  private static final String ERROR_CODE_HEADER = "X-DBOS-Error";
  private static final String PAST_LIMIT = "past_limit";
  // From the hosted Conductor, these mean it could not answer, not that it refused the key.
  private static final Set<Integer> HOSTED_UNAVAILABLE = Set.of(403, 407, 408, 425, 429);
  private static final String LICENSE_URL = "https://www.dbos.dev/conductor-license";
  private static final String LICENSE_NOTICE =
      "Transact Enterprise is licensed under the DBOS Conductor Software License ("
          + LICENSE_URL
          + "), which makes disabling or circumventing this check a material breach.";
  private static final String KEY_HINT =
      "Set transactEnterpriseKey or conductorKey in your DBOSConfig.";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Entitlements() {}

  /**
   * The enterprise features a config turns on. Each entitled feature adds its switch here when it
   * lands, so the check runs for it at launch.
   */
  public static List<Feature> enabledFeatures(DBOSConfig config) {
    return List.of();
  }

  /**
   * Refuses to go on unless Conductor reports the organization entitled to every feature in {@code
   * features}. A self-hosted Conductor may answer without a key. DBOS's hosted Conductor fails
   * open, with a warning, when it cannot answer; any other fails closed. DBOS Cloud passes without
   * a check.
   *
   * <p>A license control: the Transact Enterprise license makes disabling or circumventing it a
   * material breach.
   *
   * @param features the features to check; nothing is checked when empty
   * @param enterpriseKey the configured Transact Enterprise key, or null
   * @param conductorKey the configured Conductor key, which stands in for a missing enterprise key
   * @param conductorUrl the configured Conductor URL, or null for DBOS's hosted Conductor
   * @param dbosCloud whether this application runs in DBOS Cloud
   * @throws IllegalStateException if the check refuses, naming why
   */
  public static void require(
      List<Feature> features,
      @Nullable String enterpriseKey,
      @Nullable String conductorKey,
      @Nullable String conductorUrl,
      boolean dbosCloud) {
    if (features.isEmpty() || dbosCloud) {
      return;
    }
    check(
        features,
        enterpriseKey,
        conductorKey,
        conductorUrl,
        System.getenv("DBOS_DOMAIN"),
        http(TIMEOUT));
  }

  /** The answer to the entitlement request. */
  record Answer(int status, @Nullable String errorCode, String body) {}

  /** Sends the entitlement request. */
  @FunctionalInterface
  interface Http {
    Answer get(URI uri, Map<String, String> headers) throws IOException, InterruptedException;
  }

  static void check(
      List<Feature> features,
      @Nullable String enterpriseKey,
      @Nullable String conductorKey,
      @Nullable String conductorUrl,
      @Nullable String dbosDomain,
      Http http) {
    if (features.isEmpty()) {
      return;
    }
    var names = join(features, Feature::description);
    var entitlements = join(features, Feature::entitlement);
    // As DBOSConfig reads them: blank is unset, and surrounding whitespace (a trailing newline from
    // a secret file) is not part of the value.
    var key = setting(enterpriseKey);
    if (key == null) {
      key = setting(conductorKey);
    }
    conductorUrl = setting(conductorUrl);
    var target = conductorUrl != null ? conductorUrl : defaultConductorUrl(dbosDomain);
    String url;
    try {
      url = entitlementUrl(target);
    } catch (InvalidUrl e) {
      // The URL is not quoted: it may carry credentials.
      throw new IllegalStateException(
          names
              + " requires an entitled DBOS key, but the Conductor URL is invalid: "
              + e.getMessage());
    }
    // DBOS's hosted Conductor however its URL is written (a trailing slash, upper case), by the
    // route the check asks.
    var hosted = url.equalsIgnoreCase(HOSTED_ROUTE);
    // Only a Conductor named explicitly is asked without a key, and never DBOS's hosted one.
    if (key == null && (conductorUrl == null || hosted)) {
      throw new IllegalStateException(names + " requires a DBOS key. " + KEY_HINT);
    }
    // A DBOS key is printable ASCII. Anything else is refused here, unquoted, rather than by the
    // HTTP client, whose message would quote the key.
    if (key != null && !key.chars().allMatch(c -> c > 0x20 && c < 0x7f)) {
      throw new IllegalStateException(
          names
              + " requires a DBOS key, but the configured key is not one: it contains a character"
              + " other than printable ASCII.");
    }
    var headers = new HashMap<String, String>();
    headers.put("User-Agent", "dbos-transact-java/" + DBOS.version());
    headers.put("Accept", "application/json");
    if (key != null) {
      headers.put("Authorization", "Bearer " + key);
    }

    String failure;
    Exception cause = null;
    try {
      var answer = http.get(URI.create(url), headers);
      var status = answer.status();
      if (status >= 200 && status < 300) {
        var granted = parseEntitlements(answer.body());
        var missing =
            features.stream().filter(f -> !granted.getOrDefault(f.entitlement(), false)).toList();
        if (!missing.isEmpty()) {
          throw refusal(
              "Your DBOS organization is not entitled to "
                  + join(missing, Feature::entitlement)
                  + " ("
                  + join(missing, Feature::description)
                  + "): its plan does not include it. Contact DBOS to enable it.");
        }
        return;
      }
      // Conductor's own words, but never the key, should an answer echo it.
      var detail = redact(problemDetail(answer.body()), key);
      var reason = detail != null ? ": " + detail : "";
      if (status == 403 && PAST_LIMIT.equals(answer.errorCode())) {
        throw refusal(
            "Your DBOS organization is past its plan limit, so it is not entitled to "
                + entitlements
                + " ("
                + names
                + "). Upgrade its plan to continue.");
      }
      if (hosted && HOSTED_UNAVAILABLE.contains(status)) {
        // An outage, handled below.
      } else if (status == 404 && detail == null) {
        // A bare 404 is a missing route: a Conductor predating per-feature entitlements.
        throw refusal(
            names
                + " requires an entitled DBOS key, but Conductor at "
                + url
                + " has no entitlements route (HTTP 404). Upgrade Conductor, or check its URL.");
      } else if (status == 401 && key == null) {
        throw new IllegalStateException(
            names
                + " requires a DBOS key: Conductor at "
                + url
                + " refused to check without one (HTTP 401)"
                + reason
                + ". "
                + KEY_HINT);
      } else if (status == 401) {
        // An unknown key, or a user credential instead of an API key.
        throw refusal(
            names
                + " requires a valid DBOS key. Conductor at "
                + url
                + " rejected the key (HTTP 401)"
                + reason);
      } else if (status < 500) {
        // Any other refusal, such as the key's organization being gone, or a redirect.
        throw refusal(
            names
                + " requires an entitled DBOS key, but Conductor at "
                + url
                + " refused the check (HTTP "
                + status
                + ")"
                + reason);
      }
      failure = "HTTP " + status + reason;
    } catch (MalformedAnswer e) {
      failure = e.getMessage();
      cause = e;
    } catch (IOException e) {
      rethrowError(e);
      failure = e.getMessage() != null ? redact(e.getMessage(), key) : e.getClass().getSimpleName();
      cause = e;
    } catch (IllegalArgumentException e) {
      // The HTTP client refused to send the request: not an outage, so it never fails open. Its
      // message may quote a header, so it is not passed on.
      throw refusal(
          names
              + " requires an entitled DBOS key, but the entitlement request to Conductor at "
              + url
              + " could not be sent.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while checking the DBOS key's entitlements with Conductor at " + url, e);
    }
    if (hosted) {
      logger.warn(
          "Could not verify the DBOS key's entitlements with Conductor at {} ({}); starting anyway.",
          url,
          failure);
      return;
    }
    // Any other Conductor fails closed, so an unreachable URL cannot stand in for a key.
    var refused =
        refusal(
            names
                + " requires an entitled DBOS key, but checking it with Conductor at "
                + url
                + " failed ("
                + failure
                + "). Retry once Conductor is reachable.");
    if (cause != null) {
      refused.initCause(cause);
    }
    throw refused;
  }

  /** DBOS's hosted Conductor, on the domain {@code DBOS_DOMAIN} names. */
  static String defaultConductorUrl(@Nullable String dbosDomain) {
    var domain = dbosDomain == null || dbosDomain.isBlank() ? "cloud.dbos.dev" : dbosDomain.trim();
    return "wss://" + domain + "/conductor/v1alpha1";
  }

  /** {@code text} with every occurrence of {@code key} masked, for a diagnostic. */
  private static @Nullable String redact(@Nullable String text, @Nullable String key) {
    return text == null || key == null ? text : text.replace(key, "***");
  }

  /** A setting without surrounding whitespace, or null if it is unset or blank. */
  private static @Nullable String setting(@Nullable String value) {
    if (value == null) {
      return null;
    }
    var stripped = value.strip();
    return stripped.isEmpty() ? null : stripped;
  }

  /** A Conductor URL that cannot be used, with a message that never quotes it. */
  static final class InvalidUrl extends Exception {
    InvalidUrl(String message) {
      super(message);
    }
  }

  /** The entitlement route of the Conductor at {@code conductorUrl}, over HTTP(S). */
  static String entitlementUrl(String conductorUrl) throws InvalidUrl {
    URI uri;
    try {
      uri = new URI(conductorUrl);
    } catch (URISyntaxException e) {
      // The reason, but not the message, which quotes the whole URL.
      throw new InvalidUrl(e.getReason());
    }
    var scheme = uri.getScheme();
    var authority = uri.getRawAuthority();
    if (scheme == null || authority == null) {
      throw new InvalidUrl("it is not an absolute URL");
    }
    scheme =
        switch (scheme.toLowerCase()) {
          // The Conductor client is a WebSocket client, which takes only these.
          case "wss" -> "https";
          case "ws" -> "http";
          default -> throw new InvalidUrl("its scheme is not ws or wss");
        };
    // A host the parser cannot identify (an underscore, a raw '/' or '@' in a password) leaves the
    // credentials unseparated. The Conductor client cannot connect to such a URL either.
    if (uri.getHost() == null) {
      throw new InvalidUrl("its host is not valid");
    }
    // A raw '/' in a password can also parse as a port and a path ("user:2024/pass@host"), so the
    // host is wrong and the rest of the password sits in the path. A Conductor URL never has '@'
    // after its authority.
    for (var part : new String[] {uri.getRawPath(), uri.getRawQuery(), uri.getRawFragment()}) {
      if (part != null && part.contains("@")) {
        throw new InvalidUrl("it has an '@' after its host");
      }
    }
    if (uri.getPort() > 65535) {
      throw new InvalidUrl("its port is out of range");
    }
    // Without any user:password@, which would otherwise be sent and quoted in errors.
    var host = uri.getHost() + (uri.getPort() == -1 ? "" : ":" + uri.getPort());
    // The hosted websocket URL carries an API version; routes sit beside it.
    var path = uri.getRawPath() == null ? "" : uri.getRawPath().replaceAll("/+$", "");
    if (path.endsWith("/v1alpha1")) {
      path = path.substring(0, path.length() - "/v1alpha1".length());
    }
    return scheme + "://" + host + path + ENTITLEMENTS_PATH;
  }

  /** Sends the request over HTTP, giving the whole exchange, body included, {@code timeout}. */
  static Http http(Duration timeout) {
    return (uri, headers) -> {
      var builder = HttpClient.newBuilder().connectTimeout(timeout);
      // Never follow a redirect: it would carry the key to whatever host it names.
      builder.followRedirects(HttpClient.Redirect.NEVER);
      // Built per call, so proxy settings are read when the check runs.
      var proxies = ProxySelector.getDefault();
      if (proxies != null) {
        builder.proxy(proxies);
      }
      // No request timeout: it would end at the headers, and race the deadline below.
      var request = HttpRequest.newBuilder(uri).GET();
      headers.forEach(request::header);
      var pending =
          builder.build().sendAsync(request.build(), HttpResponse.BodyHandlers.ofString());
      HttpResponse<String> response;
      try {
        // One deadline for the whole exchange, body included, so an answer that stalls mid-body
        // cannot hold up launch.
        response = pending.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        pending.cancel(true);
        throw new HttpTimeoutException("no answer within " + timeout.toMillis() + " ms");
      } catch (InterruptedException e) {
        pending.cancel(true);
        throw e;
      } catch (ExecutionException e) {
        // The exchange failed in flight: an outage. The JDK reports some malformed answers (a
        // non-numeric Content-Length) as an IllegalArgumentException, which must not read as a
        // request this client refused to send. A port out of range, which the client would also
        // refuse in flight, is refused by entitlementUrl before any request.
        var cause = e.getCause();
        rethrowError(cause);
        if (cause instanceof IOException io) {
          throw io;
        }
        throw new IOException(
            "the exchange failed (" + cause.getClass().getSimpleName() + ")", cause);
      }
      return new Answer(
          response.statusCode(),
          response.headers().firstValue(ERROR_CODE_HEADER).orElse(null),
          response.body());
    };
  }

  /**
   * Throws the first {@link Error} in {@code failure}'s cause chain. Not an outage: a damaged JVM
   * must never fail open, and the HTTP client reports one (an OutOfMemoryError in its selector
   * thread) as the cause of an IOException.
   */
  private static void rethrowError(Throwable failure) {
    var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    for (var t = failure; t != null && seen.add(t); t = t.getCause()) {
      if (t instanceof Error error) {
        throw error;
      }
    }
  }

  /** An answer that is not an entitlement list, which counts as Conductor failing to answer. */
  private static final class MalformedAnswer extends Exception {
    MalformedAnswer(String message) {
      super(message);
    }
  }

  private static Map<String, Boolean> parseEntitlements(String body) throws MalformedAnswer {
    JsonNode parsed;
    try {
      parsed = MAPPER.readTree(body);
    } catch (JacksonException e) {
      throw new MalformedAnswer("the response is not JSON");
    }
    var entitlements = parsed == null ? null : parsed.get("entitlements");
    if (entitlements == null || !entitlements.isObject()) {
      throw new MalformedAnswer("the response has no entitlements");
    }
    var granted = new HashMap<String, Boolean>();
    // Anything but true grants nothing.
    for (var entry : entitlements.properties()) {
      granted.put(entry.getKey(), entry.getValue().isBoolean() && entry.getValue().booleanValue());
    }
    return granted;
  }

  /** The reason Conductor gave in its JSON error body, or null if it sent none. */
  private static @Nullable String problemDetail(String body) {
    JsonNode parsed;
    try {
      parsed = MAPPER.readTree(body);
    } catch (JacksonException e) {
      // Not JSON: a bare router answer, such as "404 page not found".
      return null;
    }
    if (parsed == null || !parsed.isObject()) {
      return null;
    }
    for (var field : List.of("detail", "message")) {
      var value = parsed.get(field);
      if (value == null || value.isNull()) {
        continue;
      }
      var text = value.isString() ? value.asString() : value.toString();
      if (!text.isEmpty()) {
        return text;
      }
    }
    return null;
  }

  private static IllegalStateException refusal(String message) {
    return new IllegalStateException(message + " " + LICENSE_NOTICE);
  }

  private static String join(List<Feature> features, Function<Feature, String> name) {
    return features.stream().map(name).collect(Collectors.joining(" and "));
  }
}
