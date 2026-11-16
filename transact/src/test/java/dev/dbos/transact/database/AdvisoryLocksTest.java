package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Lock names and keys, checked against values computed with the Python SDK's {@code lock_name} and
 * {@code advisory_lock_key}, and with its two-int partition key arithmetic, and cross-checked
 * against {@code JSON.stringify}. A Java key that differed would let Java claimers and the other
 * SDKs' claimers on one queue each hold "the same" lock at once.
 */
public class AdvisoryLocksTest {

  static Stream<Arguments> queueClaimLocks() {
    return Stream.of(
        Arguments.of(
            new String[] {"dbos.queue_claim", "dbos", "q", null},
            "[\"dbos.queue_claim\",\"dbos\",\"q\",null]",
            -8324755576819551426L),
        Arguments.of(
            new String[] {"dbos.queue_claim", "dbos", "q", "p"},
            "[\"dbos.queue_claim\",\"dbos\",\"q\",\"p\"]",
            948831083418116994L),
        Arguments.of(
            new String[] {"dbos.queue_claim", "my_schema", "orders", "customer-42"},
            "[\"dbos.queue_claim\",\"my_schema\",\"orders\",\"customer-42\"]",
            7376312163726221232L),
        // Non-ASCII characters are written as themselves, not escaped.
        Arguments.of(
            new String[] {"dbos.queue_claim", "dbos", "Warteschlange-\u00fc", "\u5206\u533a-1"},
            "[\"dbos.queue_claim\",\"dbos\",\"Warteschlange-\u00fc\",\"\u5206\u533a-1\"]",
            3899298939503614244L),
        // A character outside the BMP, a surrogate pair in Java's UTF-16.
        Arguments.of(
            new String[] {"dbos.queue_claim", "dbos", "rocket-\uD83D\uDE80", null},
            "[\"dbos.queue_claim\",\"dbos\",\"rocket-\uD83D\uDE80\",null]",
            -3387402394760232040L),
        // Escapes: quote and backslash, the short forms, other control characters as lowercase
        // \\u00XX; '/' and DEL as themselves.
        Arguments.of(
            new String[] {"dbos.queue_claim", "dbos", "q\"\\/", "a\nb\t\u0001\u001f\u007f"},
            "[\"dbos.queue_claim\",\"dbos\",\"q\\\"\\\\/\",\"a\\nb\\t\\u0001\\u001f\u007f\"]",
            2688697942837141003L));
  }

  @ParameterizedTest
  @MethodSource("queueClaimLocks")
  public void queueClaimLockMatchesTheOtherSdks(String[] parts, String name, long key) {
    assertEquals(name, AdvisoryLocks.lockName(parts));
    assertEquals(key, AdvisoryLocks.lockKey(name));
    assertEquals(key, AdvisoryLocks.queueClaimKey(parts[1], parts[2], parts[3]));
  }

  @Test
  public void loneSurrogatesAreEscapedAsJavaScriptDoes() {
    // As JSON.stringify writes a lone surrogate, which has no UTF-8 encoding. Python cannot encode
    // one at all, so no Python claimer shares such a lock.
    assertEquals("[\"\\ud800\",\"a\\udc00b\"]", AdvisoryLocks.lockName("\uD800", "a\uDC00b"));
  }

  @Test
  public void retentionKeyMatchesTheOtherSdks() {
    // sha256("dbos.retention.dbos")[:8], big-endian signed, as Python, TypeScript and Go compute
    // it.
    assertEquals(7208852302618897048L, AdvisoryLocks.retentionKey("dbos"));
  }

  @Test
  public void partitionClaimKeysMatchTheOtherSdks() {
    assertEquals(-294798389, AdvisoryLocks.partitionClaimQueueId("dbos", "q"));
    assertEquals(
        Arrays.asList(379280258, 1567581053, -474954686, -339997912),
        Stream.of("p0", "\u5206\u533a", "", "\uD83D\uDE80")
            .map(AdvisoryLocks::partitionClaimId)
            .toList());

    assertEquals(-1711261800, AdvisoryLocks.partitionClaimQueueId("my_schema", "orders"));
    assertEquals(-1606030541, AdvisoryLocks.partitionClaimId("customer-42"));
  }

  @Test
  public void partitionClaimQueueIdIsTheLowHalfOfTheBigintKey() {
    // TypeScript derives it this way, as BigInt.asIntN(32, advisoryLockKey(name)).
    var name = AdvisoryLocks.lockName("dbos.partition_claim", "dbos", "q");
    assertEquals(
        (int) AdvisoryLocks.lockKey(name), AdvisoryLocks.partitionClaimQueueId("dbos", "q"));
  }
}
