CREATE TABLE "%1$s".application_versions (
  version_id TEXT NOT NULL PRIMARY KEY,
  version_name TEXT NOT NULL UNIQUE,
  version_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
  created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);