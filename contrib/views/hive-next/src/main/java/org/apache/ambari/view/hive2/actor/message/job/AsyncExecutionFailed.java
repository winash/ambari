package org.apache.ambari.view.hive2.actor.message.job;

/**
 * Created by dbhowmick on 5/23/16.
 */
public class AsyncExecutionFailed extends ExecutionFailed {
  private final String jobId;

  public AsyncExecutionFailed(String jobId, String message, Throwable error) {
    super(message, error);
    this.jobId = jobId;
  }

  public AsyncExecutionFailed(String jobId, String message) {
    super(message);
    this.jobId = jobId;
  }

  public String getJobId() {
    return jobId;
  }
}
