package dev.dbos.transact.spring.txstep;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration properties for {@link TransactionalStep @TransactionalStep} support. */
@ConfigurationProperties(prefix = "dbos.txstep")
public class TransactionalStepProperties {

  /**
   * Schema for the {@code tx_step_outputs} table. When not set, falls back to the DBOS system
   * database schema ({@code dbos.datasource.schema}, or {@code dbos} if that is also unset).
   */
  private String schema;

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }
}
