package org.apache.ambari.view.hive2.actor.message;

import org.apache.ambari.view.ViewContext;

public class SyncJob extends HiveJob {
  public SyncJob(String username, String[] statements,ViewContext viewContext) {
    super(Type.SYNC, statements, username,viewContext);
  }
}
