package org.apache.ambari.view.hive2.actor.message;


public class TerminateInactivityCheck {

    private final String userName;
    private final String jobId;

    public TerminateInactivityCheck(Connect message) {
        userName = message.getUsername();
        jobId = message.getJobId();

    }

    public String getJobId() {
        return jobId;
    }

    public String getUserName() {
        return userName;
    }
}
