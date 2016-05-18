package org.apache.ambari.view.hive2.actor.message;

public class ExecuteJob {
  public final static String SYNC_JOB_MARKER = "SYSC";
  private final Connect connect;
  private final HiveJob job;

  public ExecuteJob(Connect connect, HiveJob job) {
    this.connect = connect;
    this.job = job;
  }

  public Connect getConnect() {
    return connect;
  }

  public HiveJob getJob() {
    return job;
  }
}
