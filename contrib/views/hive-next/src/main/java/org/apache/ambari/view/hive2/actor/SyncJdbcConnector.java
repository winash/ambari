package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.persistence.Storage;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.HiveJob;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.SyncJob;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.job.NoResult;
import org.apache.ambari.view.hive2.actor.message.job.ResultSetHolder;
import org.apache.ambari.view.hive2.exceptions.NotConnectedException;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.hive.jdbc.HiveConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by dbhowmick on 5/19/16.
 */
public class SyncJdbcConnector extends JdbcConnector {

  public SyncJdbcConnector(ViewContext viewContext, HdfsApi hdfsApi, ActorSystem system, ActorRef parent, ConnectionDelegate connectionDelegate, Storage storage) {
    super(viewContext, hdfsApi, system, parent, connectionDelegate, storage);
  }

  @Override
  protected void handleJobMessage(HiveMessage message) {
    Object job = message.getMessage();
    if(job instanceof SyncJob) {
      execute((SyncJob) job);
    }
  }

  protected void execute(SyncJob job) {
    ActorRef sender = this.getSender();
    if (connectable == null) {
      throw new NotConnectedException("Cannot execute sync job for user: " + job.getUsername() + ". Not connected to Hive");
    }

    Optional<HiveConnection> connectionOptional = connectable.getConnection();
    if (!connectionOptional.isPresent()) {
      throw new NotConnectedException("Cannot execute sync job for user: " + job.getUsername() + ". Not connected to Hive");
    }

    try {

      Optional<ResultSet> resultSetOptional = connectionDelegate.executeSync(connectionOptional.get(), job);
      if(resultSetOptional.isPresent()) {
        ActorRef resultSetActor = getContext().actorOf(Props.create(ResultSetIterator.class, self(), resultSetOptional.get()));
        sender.tell(new ResultSetHolder(resultSetActor), self());
      } else {
        sender.tell(new NoResult(), self());
        //parent.tell(new FreeConnector(self() new InactivityCheck()), ActorRef.noSender());
        // TODO: tell parent to freeup connection.
      }


    } catch (SQLException e) {
      // Something went wrong with executing the Statement
      // the statement will be closes and also the associated result set
      // TODO: mark the job as failed in the DB
      sender.tell(new ExecutionFailed("Failed to execute Jdbc Statement", e), self());
    }
  }
}
