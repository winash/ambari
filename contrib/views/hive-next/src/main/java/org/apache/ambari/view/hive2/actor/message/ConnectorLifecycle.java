package org.apache.ambari.view.hive2.actor.message;

/**
 * Created by dbhowmick on 5/13/16.
 */
public class ConnectorLifecycle {

  private final String username;
  private final String jobId;

  public ConnectorLifecycle(String username, String jobId) {
    this.username = username;
    this.jobId = jobId;
  }

  public String getUsername() {
    return username;
  }

  public String getJobId() {
    return jobId;
  }
}
