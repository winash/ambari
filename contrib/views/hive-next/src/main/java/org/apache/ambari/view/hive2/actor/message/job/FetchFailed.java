package org.apache.ambari.view.hive2.actor.message.job;

/**
 * Created by dbhowmick on 5/19/16.
 */
public class FetchFailed extends Failure{

  public FetchFailed(String message, Throwable error) {
    super(message, error);
  }

  public FetchFailed(String message) {
    this(message, new Exception(message));
  }

}
