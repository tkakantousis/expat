package io.hops.hopsworks.expat.migrations;

public class RollbackException extends Exception {
  public RollbackException(String message, Throwable t) {
    super(message, t);
  }
}
