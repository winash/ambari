package org.apache.ambari.view.hive2.actor.message;

public class AdvanceCursor {

    private String job;

    public AdvanceCursor(String job) {
        this.job = job;
    }

    public String getJob() {
        return job;
    }
}
