package dev.dbos.transact;

public class Constants {

  public static final String DB_SCHEMA = "dbos";
  public static final String SYS_DB_SUFFIX = "_dbos_sys";
  public static final String POSTGRES_DEFAULT_DB = "postgres";

  public static final String POSTGRES_PASSWORD_ENV_VAR = "PGPASSWORD";
  public static final String POSTGRES_USER_ENV_VAR = "PGUSER";

  public static final String DEFAULT_APP_VERSION = "";
  public static final String DEFAULT_EXECUTORID = "local";

  public static final String DBOS_NULL_TOPIC = "__null__topic__";

  public static final String DBOS_INTERNAL_QUEUE = "_dbos_internal_queue";
  public static final String DBOS_SCHEDULER_QUEUE = "schedulerQueue";

  public static final String SYSTEM_JDBC_URL_ENV_VAR = "DBOS_SYSTEM_JDBC_URL";

  public static final int DEFAULT_MAX_RECOVERY_ATTEMPTS = 100;
}
