package org.apache.ambari.view.hive2.actor.message.lifecycle;

public class DestroyConnector {
  private final String username;
  private final String jobId;
  private final boolean forAsync;

  public DestroyConnector(String username, String jobId, boolean forAsync) {
    this.username = username;
    this.jobId = jobId;
    this.forAsync = forAsync;
  }

  public String getUsername() {
    return username;
  }

  public String getJobId() {
    return jobId;
  }

  public boolean isForAsync() {
    return forAsync;
  }

  @Override
  public String toString() {
    return "DestroyConnector{" +
      "username='" + username + '\'' +
      ", jobId='" + jobId + '\'' +
      ", forAsync=" + forAsync +
      '}';
  }
}
