package org.apache.ambari.view.hive2.actor.message;

/**
 * Created by dbhowmick on 5/17/16.
 */
public class ExecuteSyncJob extends HiveJob {
  public ExecuteSyncJob(String username, String[] statements) {
    super(Type.SYNC, statements, username);
  }
}
