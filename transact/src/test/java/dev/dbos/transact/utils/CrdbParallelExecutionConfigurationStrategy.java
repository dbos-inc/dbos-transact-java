package dev.dbos.transact.utils;

import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.support.hierarchical.DefaultParallelExecutionConfigurationStrategy;
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfiguration;
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfigurationStrategy;

public class CrdbParallelExecutionConfigurationStrategy
    implements ParallelExecutionConfigurationStrategy {

  @Override
  public ParallelExecutionConfiguration createConfiguration(
      ConfigurationParameters configurationParameters) {
    if (PgContainer.USE_COCKROACH_DB) {
      int parallelism = Runtime.getRuntime().availableProcessors() >= 8 ? 2 : 1;
      return fixedConfig(parallelism);
    }
    return DefaultParallelExecutionConfigurationStrategy.DYNAMIC.createConfiguration(
        configurationParameters);
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
