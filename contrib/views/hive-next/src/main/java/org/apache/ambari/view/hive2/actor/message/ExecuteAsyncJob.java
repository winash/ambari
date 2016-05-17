package org.apache.ambari.view.hive2.actor.message;

/**
 * Message to be sent when a statement has to be executed
 */
public class ExecuteAsyncJob extends HiveJob {
  private final String jobId;
  private final String logFile;

  public ExecuteAsyncJob(String jobId, String username, String[] statements, String logFile) {
    super(Type.ASYNC, statements, username);
    this.jobId = jobId;
    this.logFile = logFile;
  }

  public String getJobId() {
    return jobId;
  }

  public String getLogFile() {
    return logFile;
  }

}
