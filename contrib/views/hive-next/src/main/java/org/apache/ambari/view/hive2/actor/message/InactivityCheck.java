package org.apache.ambari.view.hive2.actor.message;

public class InactivityCheck {

    private final String userName;
    private final String jobId;
    private final boolean isJobSync;


    public InactivityCheck(ExecuteAsyncJob message) {
        userName = message.getUsername();
        jobId = message.getJobId();
        isJobSync = false;
    }

    public InactivityCheck(ExecuteSyncJob job) {
        userName = job.getUsername();
        jobId = null;
        isJobSync = true;
    }


    public String getJobId() {
        return jobId;
    }

    public String getUserName() {
        return userName;
    }

    public boolean isJobSync() {
        return isJobSync;
    }
}
