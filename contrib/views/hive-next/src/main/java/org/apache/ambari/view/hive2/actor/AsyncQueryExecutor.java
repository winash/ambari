package org.apache.ambari.view.hive2.actor;

import org.apache.ambari.view.hive2.actor.message.ExecuteQuery;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.internal.AsyncExecutionSuccess;
import org.apache.ambari.view.hive2.persistence.Storage;
import org.apache.ambari.view.hive2.persistence.utils.ItemNotFound;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.Job;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.JobImpl;

import java.sql.SQLException;
import java.sql.Statement;

public class AsyncQueryExecutor extends HiveActor {


    private Statement statement;
    private final Storage storage;
    private final String jobId;

    public AsyncQueryExecutor(Statement statement, Storage storage, String jobId) {
        this.statement = statement;
        this.storage = storage;
        this.jobId = jobId;
    }

    @Override
    public void handleMessage(HiveMessage hiveMessage) {
        Object message = hiveMessage.getMessage();

        if (message instanceof ExecuteQuery) {
            executeQuery();
        }

    }

    private void executeQuery() {
        JobImpl job = null;
        try {
            job = storage.load(JobImpl.class, jobId);
            statement.getUpdateCount();
            job.setStatus(Job.JOB_STATE_FINISHED);
            storage.store(JobImpl.class, job);
            sender().tell(new AsyncExecutionSuccess(),self());
        } catch (SQLException e) {
            job.setStatus(Job.JOB_STATE_ERROR);
            sender().tell(new AsyncExecutionFailed(jobId, e.getMessage(), e), self());
            storage.store(JobImpl.class, job);
        } catch (ItemNotFound itemNotFound) {
            sender().tell(new AsyncExecutionFailed(jobId, "Cannot load job", itemNotFound), self());
        }

    }


}



