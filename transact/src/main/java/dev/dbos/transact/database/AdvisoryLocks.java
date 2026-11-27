package dev.dbos.transact.database;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.jspecify.annotations.Nullable;

/**
 * Names and keys for the PostgreSQL advisory locks DBOS takes.
 *
 * <p>Executors in every language share a system database, so each derives a lock's key the same
 * way, byte for byte. A key that differed in any SDK would let its executors and everyone else's
 * hold "the same" lock at once.
 */
public final class AdvisoryLocks {

  private AdvisoryLocks() {}

  /**
   * A lock's name: its parts as a compact JSON array, exactly as JavaScript's {@code
   * JSON.stringify} writes it -- no whitespace, {@code null} for a missing part, and non-ASCII
   * characters as themselves rather than escaped.
   */
  public static String lockName(@Nullable String... parts) {
    var sb = new StringBuilder("[");
    for (int i = 0; i < parts.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      if (parts[i] == null) {
        sb.append("null");
      } else {
        appendJsonString(sb, parts[i]);
      }
    }
    return sb.append(']').toString();
  }

  /** A bigint advisory lock key: the leading 8 bytes of the name's SHA-256, big-endian signed. */
  public static long lockKey(String name) {
    return ByteBuffer.wrap(sha256(name), 0, Long.BYTES).getLong();
  }

  /**
   * The lock a queue claim that spends a shared budget takes first.
   *
   * @param partitionKey the partition whose budget the claim spends, or null for the whole queue
   */
  public static long queueClaimKey(String schema, String queueName, @Nullable String partitionKey) {
    return lockKey(lockName("dbos.queue_claim", schema, queueName, partitionKey));
  }

  /**
   * The lock that guards one schema's retention rounds. Every SDK derives it from the plain name
   * {@code dbos.retention.<schema>}, so rounds in different languages against one system database
   * contend for the same lock.
   */
  public static long retentionKey(String schema) {
    return lockKey("dbos.retention." + schema);
  }

  /**
   * The lock that lets one of an application's processes run a background job at a time, from the
   * plain name {@code dbos.background_job.<schema>.<application name or ''>.<job name>}.
   */
  public static long backgroundJobKey(String schema, @Nullable String appName, String jobName) {
    return lockKey(
        "dbos.background_job.%s.%s.%s".formatted(schema, appName == null ? "" : appName, jobName));
  }

  /**
   * The first half of a batched partition sweep's two-int lock keys, naming the queue: bytes 4-7 of
   * the SHA-256 of its lock name, big-endian signed. That is the low half of {@link #lockKey}'s
   * value for the same name.
   */
  public static int partitionClaimQueueId(String schema, String queueName) {
    return ByteBuffer.wrap(sha256(lockName("dbos.partition_claim", schema, queueName)), 4, 4)
        .getInt();
  }

  /**
   * The second half of a batched partition sweep's two-int lock keys, naming the partition: bytes
   * 0-3 of the SHA-256 of its key, big-endian signed.
   */
  public static int partitionClaimId(String partitionKey) {
    return ByteBuffer.wrap(sha256(partitionKey), 0, 4).getInt();
  }

  private static byte[] sha256(String text) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  /** Appends {@code s} as a JSON string literal, escaped exactly as {@code JSON.stringify} does. */
  private static void appendJsonString(StringBuilder sb, String s) {
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\b' -> sb.append("\\b");
        case '\f' -> sb.append("\\f");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20 || isLoneSurrogate(s, i)) {
            // Lowercase hex, as JavaScript writes it; a lone surrogate has no UTF-8 encoding.
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    sb.append('"');
  }

  private static boolean isLoneSurrogate(String s, int i) {
    char c = s.charAt(i);
    if (Character.isHighSurrogate(c)) {
      return i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1));
    }
    if (Character.isLowSurrogate(c)) {
      return i == 0 || !Character.isHighSurrogate(s.charAt(i - 1));
    }
    return false;
  }
}
