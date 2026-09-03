package dev.dbos.transact.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfiguration;
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfigurationStrategy;

/**
 * Decides how many tests run at once, which is the same question as how many database containers
 * run at once: every concurrent test holds one of its own.
 *
 * <p>That makes memory the binding constraint rather than CPU. A machine with more cores than it
 * has memory to feed containers will happily start all of them and then spend its time swapping, or
 * lose a container to the OOM killer and report it as a test failure somewhere unrelated. So the
 * parallelism is whichever of the two limits is lower.
 *
 * <p>The memory figure is what is <i>available</i>, not what is installed: a developer machine is
 * usually running other things, and CI containers are often given a fraction of the host. On Linux
 * that is MemAvailable, the kernel's own estimate of what can be had without swapping, which is
 * exactly the question being asked. Anywhere else, and if the file cannot be read, it falls back to
 * the conservative fixed answer this class had before it looked at memory at all.
 */
public class ResourceAwareParallelExecutionConfigurationStrategy
    implements ParallelExecutionConfigurationStrategy {

  /**
   * What one container is assumed to need, in MiB.
   *
   * <p>CockroachDB is given eight times PostgreSQL's share because it asks for far more: it sizes
   * its own caches from what it believes the machine has, and a single-node server carries a SQL
   * layer and a KV store where PostgreSQL carries a handful of backends.
   *
   * <p>These are deliberately generous, because underestimating does not show up as a slow suite --
   * it shows up as failures somewhere else entirely. Measured on a 12-core machine with about 4GiB
   * free, a CockroachDB budget of 1GiB gave a parallelism of 4 and cost twelve failures: the tests
   * that start from an empty server timed out at two minutes or failed to launch a container at
   * all, because those still run the whole migration corpus and were starved by the prebaked
   * containers around them. A timing-sensitive garbage-collection test failed too, its one-second
   * window having drifted under load. None of them were broken; they were crowded. At 2GiB the same
   * machine runs two, which is what the CockroachDB leg has always used.
   */
  private static final int POSTGRES_CONTAINER_MIB = 256;

  private static final int COCKROACH_CONTAINER_MIB = 2048;

  /**
   * Kept for the machines this cannot measure. It is what the CockroachDB leg used to run at
   * unconditionally, and it is deliberately small: guessing high on an unknown machine is how a
   * suite ends up thrashing.
   */
  private static final int FALLBACK_PARALLELISM = 2;

  @Override
  public ParallelExecutionConfiguration createConfiguration(
      ConfigurationParameters configurationParameters) {
    return fixedConfig(parallelism());
  }

  static int parallelism() {
    var override = System.getenv("DBOS_TEST_PARALLELISM");
    if (override != null && !override.isBlank()) {
      return Math.max(1, Integer.parseInt(override.trim()));
    }

    var cores = Runtime.getRuntime().availableProcessors();
    var perContainerMib =
        PgContainer.USE_COCKROACH_DB ? COCKROACH_CONTAINER_MIB : POSTGRES_CONTAINER_MIB;

    var availableMib = availableMemoryMib();
    if (availableMib < 0) {
      return Math.min(cores, FALLBACK_PARALLELISM);
    }

    var byMemory = (int) (availableMib / perContainerMib);
    return Math.max(1, Math.min(cores, byMemory));
  }

  /** MemAvailable from /proc/meminfo in MiB, or -1 where that cannot be read. */
  private static long availableMemoryMib() {
    try {
      for (var line : Files.readAllLines(Path.of("/proc/meminfo"))) {
        if (line.startsWith("MemAvailable:")) {
          // "MemAvailable:   12345678 kB"
          var parts = line.split("\\s+");
          return Long.parseLong(parts[1]) / 1024;
        }
      }
      return -1;
    } catch (IOException | RuntimeException e) {
      return -1;
    }
  }

  private static ParallelExecutionConfiguration fixedConfig(int parallelism) {
    return new ParallelExecutionConfiguration() {
      @Override
      public int getParallelism() {
        return parallelism;
      }

      @Override
      public int getMinimumRunnable() {
        return parallelism;
      }

      @Override
      public int getMaxPoolSize() {
        return parallelism + 256;
      }

      @Override
      public int getCorePoolSize() {
        return parallelism;
      }

      @Override
      public int getKeepAliveSeconds() {
        return 30;
      }
    };
  }
}
