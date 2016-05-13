package org.apache.ambari.view.hive2.exceptions;

/**
 * Exception thrown when the connection is not made and we try to execute some job
 */
public class NotConnectedException extends RuntimeException {
  public NotConnectedException(String message) {
    super(message);
  }
}
