package org.apache.ambari.view.hive.client;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive.resources.jobs.viewJobs.Job;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.JobSubmitted;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.hive2.internal.HiveResult;

public interface AsyncJobRunner {

    Either<JobSubmitted,AsyncExecutionFailed> submitJob(ConnectionConfig connectionConfig,AsyncJob asyncJob);

}
