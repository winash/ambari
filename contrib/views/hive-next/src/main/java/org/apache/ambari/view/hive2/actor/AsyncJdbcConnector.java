package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.persistence.Storage;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.AssignResultSet;
import org.apache.ambari.view.hive2.actor.message.AssignStatement;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.lifecycle.InactivityCheck;
import org.apache.ambari.view.hive2.actor.message.StartLogAggregation;
import org.apache.ambari.view.hive2.exceptions.NotConnectedException;
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

  protected final Logger LOG = LoggerFactory.getLogger(getClass());


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

  private void execute(AsyncJob message) {
    this.jobId = message.getJobId();
    String errorMessage = "Cannot execute job for id: " + message.getJobId() + ", user: " + message.getUsername() + ". Not connected to Hive";
    if (connectable == null) {
      exceptionWriter.tell(new AsyncExecutionFailed(message.getJobId(), errorMessage), ActorRef.noSender());
    }

    Optional<HiveConnection> connectionOptional = connectable.getConnection();
    if (!connectionOptional.isPresent()) {
      exceptionWriter.tell(new AsyncExecutionFailed(message.getJobId(), errorMessage), ActorRef.noSender());
    }

    try {

      Optional<ResultSet> resultSetOptional = connectionDelegate.execute(connectionOptional.get(), message);
      Optional<HiveStatement> currentStatement = connectionDelegate.getCurrentStatement();
      // There should be a result set, which either has a result set, or an empty value
      // for operations which do not return anything
      ActorRef resultHolder = getContext().actorOf(
        Props.create(AsyncResultHolder.class, parent, message),
        message.getUsername() + ":" + message.getJobId() + "-resultsHolder");

      ActorRef logAggregator = getContext().actorOf(
        Props.create(LogAggregator.class, system, hdfsApi, currentStatement.get(), message.getLogFile()), message.getUsername() + ":" + message.getJobId() + "-logAggregator"
      );

      if (resultSetOptional.isPresent()) {
        // Start a result set aggregator on the same context, a notice to the parent will kill all these as well
        // tell the result holder to assign the result set for further operations
        resultHolder.tell(new AssignResultSet(resultSetOptional), self());

        // Start a actor to query ATS
      } else {
        // Case when this is an Update/query with no results
        // Wait for operation to complete and add results;
        resultHolder.tell(new AssignStatement(currentStatement.get()), self());

      }
      // Start a actor to query log
      logAggregator.tell(new StartLogAggregation(), self());
      Optional<HiveStatement> statementOptional = currentStatement;

      if (statementOptional.isPresent()) {
//        updateGuidInJob(jobId, statementOptional.get());
        // Wait for the result in the Holder and update HDFS with the error log if any

      }

    } catch (SQLException e) {
      exceptionWriter.tell(new AsyncExecutionFailed(message.getJobId(),
        e.getMessage(), e), ActorRef.noSender());
    }

    // Start Inactivity timer to close the statement
    this.inactivityScheduler = system.scheduler().schedule(
      Duration.Zero(), Duration.create(15 * 1000, TimeUnit.MILLISECONDS),
      this.self(), new InactivityCheck(), system.dispatcher(), null);
  }
}
