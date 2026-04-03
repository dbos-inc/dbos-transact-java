package dev.dbos.transact.spring;

import dev.dbos.transact.config.DBOSConfig;

/**
 * Callback for customizing a {@link DBOSConfig} built from {@code dbos.*} application properties.
 *
 * <p>Declare one or more {@code @Bean} instances of this type to modify the auto-configured {@link
 * DBOSConfig} without replacing it entirely. Customizers are applied in {@link
 * org.springframework.core.annotation.Order} order after all properties have been bound.
 *
 * <p>Example — register a queue and override the executor ID at startup:
 *
 * <pre>{@code
 * @Bean
 * DBOSConfigCustomizer myCustomizer(MyQueue queue) {
 *   return config -> config
 *       .withListenQueue(queue.name())
 *       .withExecutorId("worker-1");
 * }
 * }</pre>
 */
@FunctionalInterface
public interface DBOSConfigCustomizer {

  /**
   * Customize the given {@link DBOSConfig} and return the (possibly new) instance.
   *
   * @param config the config built from application properties; never {@code null}
   * @return the customized config; must not be {@code null}
   */
  DBOSConfig customize(DBOSConfig config);
}
