package org.apache.ambari.view.hive2.actor.message;


public class TerminateInactivityCheck {

    private final String userName;
    private  akka.actor.ActorRef toTerminate;
    private String jobId;
    private final boolean isJobSync;


  public TerminateInactivityCheck(org.apache.ambari.view.hive2.actor.message.Connect message) {
        userName = message.getUsername();
        jobId = message.getJobId();
        isJobSync = message.isSync();
  }


  public String getJobId() {
        return jobId;
    }

    public String getUserName() {
        return userName;
    }

  public akka.actor.ActorRef getToTerminate() {
    return toTerminate;
  }

  public boolean isJobSync() {
    return isJobSync;
  }
}
