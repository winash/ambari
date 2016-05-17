package org.apache.ambari.view.hive2.actor;

public class GetResultHolder {

    private String jobId;
    private String userName;

    public GetResultHolder(String jobId, String userName) {
        this.jobId = jobId;
        this.userName = userName;
    }


    public String getJobId() {
        return jobId;
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public String toString() {
        return "GetResultHolder{" +
                "jobId='" + jobId + '\'' +
                ", userName='" + userName + '\'' +
                '}';
    }
}
