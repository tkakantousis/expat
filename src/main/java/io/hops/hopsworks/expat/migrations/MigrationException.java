package io.hops.hopsworks.expat.migrations;

public class MigrationException extends Exception {
  public MigrationException(String message, Throwable t) {
    super(message, t);
  }
}
