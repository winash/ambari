package org.apache.ambari.view.hive2.actor.message;

public class Job {
  public final Connect connect;
  public final ExecuteJob executeJob;

  public Job(Connect connect, ExecuteJob executeJob) {
    this.connect = connect;
    this.executeJob = executeJob;
  }
}
