package org.apache.ambari.view.hive2.actor.message.lifecycle;

import akka.actor.ActorRef;
import org.apache.ambari.view.hive2.actor.message.lifecycle.InactivityCheck;

public class FreeConnector  {

  private boolean wasSync;
  private ActorRef refToFree;
  private String userName;
  private String jobId;

  public FreeConnector(InactivityCheck message) {
    userName = message.getUserName();
    jobId = message.getJobId();
    wasSync = message.isJobSync();
  }

  public String getJobId() {
    return jobId;
  }

  public ActorRef getRefToFree() {
    return refToFree;
  }

  public String getUserName() {
    return userName;
  }

  public boolean wasJobSync() {
    return wasSync;
  }

  public FreeConnector(ActorRef which,InactivityCheck message) {
    refToFree = which;
    wasSync = message.isJobSync();
    userName = message.getUserName();
  }
}
