package org.apache.ambari.view.hive2.actor.message.job;

/**
 * Created by dbhowmick on 5/19/16.
 */
public class ExecutionFailed extends Failure {

  public ExecutionFailed(String message, Throwable error) {
    super(message, error);
  }

  public ExecutionFailed(String message) {
    super(message, new Exception(message));
  }

}
