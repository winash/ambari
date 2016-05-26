package org.apache.ambari.view.hive.client;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive.resources.jobs.viewJobs.Job;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.internal.AsyncExecutionSuccess;
import org.apache.ambari.view.hive2.internal.Either;

public interface AsyncJobRunner {

    Either<AsyncExecutionSuccess, AsyncExecutionFailed> submitJob(ConnectionConfig connectionConfig, AsyncJob asyncJob, Job job);

    Optional<NonPersistentCursor> getCursor(String jobId, String username);
}
