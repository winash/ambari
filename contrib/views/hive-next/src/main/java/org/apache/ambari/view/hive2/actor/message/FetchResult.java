package org.apache.ambari.view.hive2.actor.message;

/**
 *
 * Fetch the result for
 *
 */
public class FetchResult {
    private final String jobId;
    private final String username;

    public FetchResult(String jobId, String username) {
        this.jobId = jobId;
        this.username = username;
    }

    public String getJobId() {
        return jobId;
    }

    public String getUsername() {
        return username;
    }
}
