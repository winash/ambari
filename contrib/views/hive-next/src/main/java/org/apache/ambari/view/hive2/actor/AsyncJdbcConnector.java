package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.persistence.Storage;
import org.apache.ambari.view.hive2.persistence.utils.ItemNotFound;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.Job;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.JobImpl;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.ResultReady;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.lifecycle.InactivityCheck;
import org.apache.ambari.view.hive2.actor.message.StartLogAggregation;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class AsyncJdbcConnector extends JdbcConnector {

  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private ActorRef logAggregator = null;
  private ActorRef asyncQueryExecutor = null;
  private ActorRef resultSetActor = null;


  public AsyncJdbcConnector(ViewContext viewContext, HdfsApi hdfsApi, ActorSystem system, ActorRef parent, ConnectionDelegate connectionDelegate, Storage storage) {
    super(viewContext, hdfsApi, system, parent, connectionDelegate, storage);
  }

  @Override
  protected void handleJobMessage(HiveMessage message) {
    Object job = message.getMessage();
    if (job instanceof AsyncJob) {
      LOG.debug("Executing async job " + message.toString());
      execute((AsyncJob) job);
    }
  }

  @Override
  protected boolean isAsync() {
    return true;
  }

  @Override
  protected void cleanUpChildren() {
    if(logAggregator != null && !logAggregator.isTerminated()) {
      LOG.debug("Sending poison pill to log aggregator");
      logAggregator.tell(PoisonPill.getInstance(), self());
    }

    if(asyncQueryExecutor != null && !asyncQueryExecutor.isTerminated()) {
      LOG.debug("Sending poison pill to Async Query Executor");
      asyncQueryExecutor.tell(PoisonPill.getInstance(), self());
    }

    if(resultSetActor != null && !resultSetActor.isTerminated()) {
      LOG.debug("Sending poison pill to Resultset Actor");
      resultSetActor.tell(PoisonPill.getInstance(), self());
    }
  }

  private void execute(AsyncJob message) {
    this.jobId = message.getJobId();
    updateJobStatus(jobId,Job.JOB_STATE_INITIALIZED);
    String errorMessage = "Cannot execute job for id: " + message.getJobId() + ", user: " + message.getUsername() + ". Not connected to Hive";
    if (connectable == null) {
      exceptionWriter.tell(new AsyncExecutionFailed(message.getJobId(), errorMessage), ActorRef.noSender());
      cleanUp();
      return;
    }

    Optional<HiveConnection> connectionOptional = connectable.getConnection();
    if (!connectionOptional.isPresent()) {
      exceptionWriter.tell(new AsyncExecutionFailed(message.getJobId(), errorMessage), ActorRef.noSender());
      cleanUp();
      return;
    }

    try {
      Optional<ResultSet> resultSetOptional = connectionDelegate.execute(connectionOptional.get(), message);
      Optional<HiveStatement> currentStatement = connectionDelegate.getCurrentStatement();
      // There should be a result set, which either has a result set, or an empty value
      // for operations which do not return anything

      logAggregator = getContext().actorOf(
        Props.create(LogAggregator.class, system, hdfsApi, currentStatement.get(), message.getLogFile()), message.getUsername() + ":" + message.getJobId() + "-logAggregator"
      );

      updateGuidInJob(jobId, currentStatement.get());
      updateJobStatus(jobId,Job.JOB_STATE_RUNNING);

      if (resultSetOptional.isPresent()) {
        // Start a result set aggregator on the same context, a notice to the parent will kill all these as well
        // tell the result holder to assign the result set for further operations
        resultSetActor = getContext().actorOf(Props.create(ResultSetIterator.class, self(), resultSetOptional.get(),storage));
        sender().tell(new ResultReady(jobId,username, Either.<ActorRef, ActorRef>left(resultSetActor)), self());
        parent.tell(new ResultReady(jobId,username, Either.<ActorRef, ActorRef>left(resultSetActor)), self());

        // Start a actor to query ATS
      } else {
        // Case when this is an Update/query with no results
        // Wait for operation to complete and add results;

        ActorRef asyncQueryExecutor = getContext().actorOf(
                Props.create(AsyncQueryExecutor.class,currentStatement.get(),storage,jobId),
                message.getUsername() + ":" + message.getJobId() + "-asyncQueryExecutor");
        sender().tell(new ResultReady(jobId,username, Either.<ActorRef, ActorRef>right(asyncQueryExecutor)), self());
        parent.tell(new ResultReady(jobId,username, Either.<ActorRef, ActorRef>right(asyncQueryExecutor)), self());

      }
      // Start a actor to query log
      logAggregator.tell(new StartLogAggregation(), self());


    } catch (SQLException e) {
      // update the error on the log
      AsyncExecutionFailed failure = new AsyncExecutionFailed(message.getJobId(),
              e.getMessage(), e);
      sender().tell(failure, self());
      exceptionWriter.tell(failure, ActorRef.noSender());
      // Update the operation controller to write an error on the right side
      LOG.error("Caught SQL excpetion for job-"+message,e);

    }

    // Start Inactivity timer to close the statement
    this.inactivityScheduler = system.scheduler().schedule(
      Duration.Zero(), Duration.create(15 * 1000, TimeUnit.MILLISECONDS),
      this.self(), new InactivityCheck(), system.dispatcher(), null);
  }

  private void updateJobStatus(String jobId, String jobState) {
    JobImpl job = null;
    try {
      job = storage.load(JobImpl.class, jobId);
    } catch (ItemNotFound itemNotFound) {
      itemNotFound.printStackTrace();
    }
    job.setStatus(jobState);
    storage.store(JobImpl.class, job);
  }
}
