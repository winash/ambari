package org.apache.ambari.view.hive2.actor.message.job;

/**
 * Created by dbhowmick on 5/19/16.
 */
public class Failure {
  private final Throwable error;
  private final String message;

  public Failure(String message, Throwable error) {
    this.message = message;
    this.error = error;
  }

  public Throwable getError() {
    return error;
  }

  public String getMessage() {
    return message;
  }
}
