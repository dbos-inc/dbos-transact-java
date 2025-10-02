package dev.dbos.transact.database;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

record DatabaseInfo(
    String scheme,
    String host,
    int port,
    String database,
    String username,
    String password,
    Map<String, String> query) {

  DatabaseInfo {
    Objects.requireNonNull(scheme);
    Objects.requireNonNull(host);
    Objects.requireNonNull(database);
  }

  public DatabaseInfo withDatabase(String database) {
    return new DatabaseInfo(scheme, host, port, database, username, password, query);
  }

  static Map<String, String> parseQuery(String query) {
    Map<String, String> params = new HashMap<>();
    if (query != null) {
      for (var pair : query.split("&")) {
        var index = pair.indexOf('=');
        if (index > 0) {
          var key = decode(pair.substring(0, index));
          var value = decode(pair.substring(index + 1));
          params.put(key, value);
        }
      }
    }
    return params;
  }

  private static String decode(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  public static DatabaseInfo fromUri(String dbUrl) {
    Objects.requireNonNull(dbUrl);
    boolean jdbcUrl = false;
    if (dbUrl.startsWith("jdbc:")) {
      jdbcUrl = true;
      dbUrl = dbUrl.substring(5);
    }

    var uri = URI.create(dbUrl);
    var scheme = decode(uri.getScheme());
    var userInfoValue = uri.getUserInfo();
    var userInfo = userInfoValue != null ? decode(userInfoValue).split(":") : new String[0];
    var username = userInfo.length > 0 ? userInfo[0] : null;
    var password = userInfo.length > 1 ? userInfo[1] : null;
    var path = decode(uri.getPath());
    var database = path.startsWith("/") ? path.substring(1) : path;
    var query = parseQuery(uri.getQuery());
    if (jdbcUrl) {
      if (query.containsKey("user")) {
        username = query.get("user");
        query.remove("user");
      }
      if (query.containsKey("password")) {
        password = query.get("password");
        query.remove("password");
      }
    }

    return new DatabaseInfo(
        scheme,
        decode(uri.getHost()),
        uri.getPort(),
        database,
        username,
        password,
        Collections.unmodifiableMap(query));
  }

  public String jdbcUrl() {
    final var builder = new StringBuilder("jdbc:");

    builder.append(scheme).append("://").append(encode(host()));
    if (port > 0) {
      builder.append(":").append(port);
    }
    builder.append("/").append(encode(database));

    if (query != null) {
      var qs = query.entrySet().stream()
          .map(e -> encode(e.getKey()) + "=" + encode(e.getValue()))
          .collect(Collectors.joining("&"));
      if (qs != null && qs.length() > 0) {
        builder.append("?").append(qs);
      }
    }

    return builder.toString();
  }
}
