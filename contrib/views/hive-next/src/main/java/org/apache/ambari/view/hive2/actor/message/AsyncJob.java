package org.apache.ambari.view.hive2.actor.message;

import org.apache.ambari.view.ViewContext;

/**
 * Message to be sent when a statement has to be executed
 */
public class AsyncJob extends DDLJob {
  private final String jobId;
  private final String logFile;

  public AsyncJob(String jobId, String username, String[] statements, String logFile,ViewContext viewContext) {
    super(Type.ASYNC, statements, username,viewContext);
    this.jobId = jobId;
    this.logFile = logFile;
  }

  public String getJobId() {
    return jobId;
  }

  public String getLogFile() {
    return logFile;
  }


  @Override
  public String toString() {
    return "AsyncJob{" +
            "jobId='" + jobId + '\'' +
            ", logFile='" + logFile + '\'' +
            "} " + super.toString();
  }
}
