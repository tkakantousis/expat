package io.hops.hopsworks.expat.migrations;

public interface MigrateStep {
  void migrate() throws MigrationException;
  void rollback() throws RollbackException;
}
