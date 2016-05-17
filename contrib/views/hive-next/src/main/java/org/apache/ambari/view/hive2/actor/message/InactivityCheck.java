package org.apache.ambari.view.hive2.actor.message;

public class InactivityCheck {
    private final String userName;
    private final String jobId;

    public InactivityCheck(ExecuteJob message) {
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
