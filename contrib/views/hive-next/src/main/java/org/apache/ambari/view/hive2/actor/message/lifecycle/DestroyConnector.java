package org.apache.ambari.view.hive2.actor.message.lifecycle;

public class DestroyConnector  {

  private final String username;
  private boolean wasSync;
  private akka.actor.ActorRef toDestory;
  private String jobId;

  public DestroyConnector(String username, String jobId) {
    this.username = username;

    this.jobId = jobId;
  }

  public DestroyConnector(akka.actor.ActorRef which, String userName) {
    this.username = userName;
    toDestory = which;
    this.wasSync = true;

  }

  public String getUsername() {
    return username;
  }

  public String getJobId() {
    return jobId;
  }

  public boolean WasJobSync() {
    return wasSync;
  }

  public akka.actor.ActorRef getToDestory() {
    return toDestory;
  }
}
