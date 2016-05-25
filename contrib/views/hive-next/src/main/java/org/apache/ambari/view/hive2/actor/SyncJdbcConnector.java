package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.persistence.Storage;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.GetColumnMetadataJob;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.SyncJob;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.job.NoResult;
import org.apache.ambari.view.hive2.actor.message.job.ResultSetHolder;
import org.apache.ambari.view.hive2.actor.message.lifecycle.DestroyConnector;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.hive.jdbc.HiveConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SyncJdbcConnector extends JdbcConnector {

  public SyncJdbcConnector(ViewContext viewContext, HdfsApi hdfsApi, ActorSystem system, ActorRef parent, ConnectionDelegate connectionDelegate, Storage storage) {
    super(viewContext, hdfsApi, system, parent, connectionDelegate, storage);
  }

  @Override
  protected void handleJobMessage(HiveMessage message) {
    Object job = message.getMessage();
    if(job instanceof SyncJob) {
      execute((SyncJob) job);
    } else if (job instanceof GetColumnMetadataJob) {
      getColumnMetaData((GetColumnMetadataJob) job);
    }
  }

  @Override
  protected boolean isAsync() {
    return false;
  }

  protected void execute(final SyncJob job) {
    executeJob(new Operation<SyncJob>() {
      @Override
      SyncJob getJob() {
        return job;
      }

      @Override
      Optional<ResultSet> call(HiveConnection connection) throws SQLException {
        return connectionDelegate.executeSync(connection, job);
      }

      @Override
      String notConnectedErrorMessage() {
        return "Cannot execute sync job for user: " + job.getUsername() + ". Not connected to Hive";
      }

      @Override
      String executionFailedErrorMessage() {
        return "Failed to execute Jdbc Statement";
      }
    });
  }


  private void getColumnMetaData(final GetColumnMetadataJob job) {
    executeJob(new Operation<GetColumnMetadataJob>() {

      @Override
      GetColumnMetadataJob getJob() {
        return job;
      }

      @Override
      Optional<ResultSet> call(HiveConnection connection) throws SQLException {
        return connectionDelegate.getColumnMetadata(connection, job);
      }

      @Override
      String notConnectedErrorMessage() {
        return String.format("Cannot get column metadata for user: %s, schema: %s, table: %s, column: %s" +
            ". Not connected to Hive", job.getUsername(), job.getSchemaPattern(), job.getTablePattern(),
          job.getColumnPattern());
      }

      @Override
      String executionFailedErrorMessage() {
        return "Failed to execute Jdbc Statement";
      }
    });
  }

  private void executeJob(Operation operation) {
    ActorRef sender = this.getSender();
    String errorMessage = operation.notConnectedErrorMessage();
    if (connectable == null) {
      sender.tell(new ExecutionFailed(errorMessage), ActorRef.noSender());
    }

    Optional<HiveConnection> connectionOptional = connectable.getConnection();
    if (!connectionOptional.isPresent()) {
      sender.tell(new ExecutionFailed(errorMessage), ActorRef.noSender());
    }

    try {
      Optional<ResultSet> resultSetOptional = operation.call(connectionOptional.get());
      if(resultSetOptional.isPresent()) {
        ActorRef resultSetActor = getContext().actorOf(Props.create(ResultSetIterator.class, self(), resultSetOptional.get()));
        sender.tell(new ResultSetHolder(resultSetActor), self());
      } else {
        sender.tell(new NoResult(), self());
        parent.tell(new DestroyConnector(username, jobId, isAsync()), self());
        // TODO: tell parent to freeup connection.
      }
    } catch (SQLException e) {
      sender.tell(new ExecutionFailed(operation.executionFailedErrorMessage(), e), self());
    }
  }

  private abstract class Operation<T> {
    abstract T getJob();
    abstract Optional<ResultSet> call(HiveConnection connection) throws SQLException;
    abstract String notConnectedErrorMessage();
    abstract String executionFailedErrorMessage();
  }
}
