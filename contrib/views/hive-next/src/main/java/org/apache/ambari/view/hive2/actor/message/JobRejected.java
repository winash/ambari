package org.apache.ambari.view.hive2.actor.message;

/**
 * Created by dbhowmick on 5/13/16.
 */
public class JobRejected {

  private final String username;
  private final String jobId;
  private final String message;

  public JobRejected(String username, String jobId, String message) {
    this.username = username;
    this.jobId = jobId;
    this.message = message;
  }

  public String getUsername() {
    return username;
  }

  public String getJobId() {
    return jobId;
  }

  public String getMessage() {
    return message;
  }
}
