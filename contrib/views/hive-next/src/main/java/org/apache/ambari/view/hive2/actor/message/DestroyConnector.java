package org.apache.ambari.view.hive2.actor.message;

public class DestroyConnector  {

  private final String username;
  private final String jobId;

  public DestroyConnector(String username, String jobId) {
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
