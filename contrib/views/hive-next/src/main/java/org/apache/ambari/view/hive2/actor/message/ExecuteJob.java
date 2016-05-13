package org.apache.ambari.view.hive2.actor.message;

/**
 * Message to be sent when a statement has to be executed
 */
public class ExecuteJob {
  private final String jobId;
  private final String username;
  private final String[] statements;

  public ExecuteJob(String jobId, String username, String[] statements) {
    this.jobId = jobId;
    this.username = username;
    this.statements = statements;
  }

  public String getJobId() {
    return jobId;
  }

  public String[] getStatements() {
    return statements;
  }

  public String getUsername() {
    return username;
  }
}
