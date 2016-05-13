package org.apache.ambari.view.hive2.actor.message;

/**
 * Created by dbhowmick on 5/13/16.
 */
public class FreeConnector extends ConnectorLifecycle {
  public FreeConnector(String username, String jobId) {
    super(username, jobId);
  }
}
