package org.apache.ambari.view.hive2.actor;

import org.apache.ambari.view.hive2.actor.message.ExecuteQuery;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
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

    public AsyncQueryExecutor(Statement statement, Storage storage,String jobId) {
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

    private void executeQuery()  {
        try {
            statement.getUpdateCount();
            try {
                JobImpl job = storage.load(JobImpl.class, jobId);
                job.setStatus(Job.JOB_STATE_FINISHED);
                storage.store(JobImpl.class, job);
            } catch (ItemNotFound itemNotFound) {
                //TODO: Handle error
            }
            sender().tell(new AsyncExecutionSuccess(),self());
            //TODO: Close statement and actor
        } catch (SQLException e) {
            sender().tell(new ExecutionFailed("Cannot execute query",e),self());
        }


    }


}
