package org.apache.ambari.view.hive2.actor.message.lifecycle;

public class FreeConnector  {

  private final String username;
  private final String jobId;
  private final boolean forAsync;

  public FreeConnector(String username, String jobId, boolean forAsync) {
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
    return "FreeConnector{" +
      "username='" + username + '\'' +
      ", jobId='" + jobId + '\'' +
      ", forAsync=" + forAsync +
      '}';
  }
}
