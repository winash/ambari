package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.DestroyConnector;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.FreeConnector;
import org.apache.ambari.view.hive2.actor.message.InactivityCheck;
import org.apache.ambari.view.hive2.actor.message.TerminateInactivityCheck;
import org.apache.ambari.view.hive2.exceptions.NotConnectedException;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.HiveConnectionWrapper;
import org.apache.hive.jdbc.HiveConnection;
import scala.concurrent.duration.Duration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Wraps one Jdbc connection per user, per instance. This is used to delegate execute the statements and
 * creates child actors to delegate the resultset extraction, YARN/ATS querying for Job info and Log Aggregation
 */
public class JdbcConnector extends UntypedActor {

  /**
   * Interval for maximum inactivity allowed
   */
  private final static long MAX_INACTIVITY_INTERVAL = 5 * 60 * 1000;

  /**
   * Interval for maximum inactivity allowed before termination
   */
  private static final long MAX_TERMINATION_INACTIVITY_INTERVAL = 10 * 60 * 1000;

  private final ViewContext viewContext;
  private final ActorSystem system;

  /**
   * Keeps track of the timestamp when the last activity has happened. This is
   * used to calculate the inactivity period and take lifecycle decisions based
   * on it.
   */
  private long lastActivityTimestamp;

  /**
   * Akka scheduler to tick at an interval to deal with inactivity of this actor
   */
  private Cancellable inactivityScheduler;

  /**
   * Akka scheduler to tick at an interval to deal with the inactivity after which
   * the actor should be killed and connectable should be released
   */
  private Cancellable terminateActorScheduler;

  private Connectable connectable = null;
  private final ConnectionDelegate connectionDelegate;
  private final ActorRef parent;

  private String username;
  private String jobId;


  public JdbcConnector(ViewContext viewContext, ActorSystem system, ActorRef parent, ConnectionDelegate connectionDelegate) {
    this.viewContext = viewContext;
    this.system = system;
    this.parent = parent;
    this.connectionDelegate = connectionDelegate;
    this.lastActivityTimestamp = System.currentTimeMillis();
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Connect) {
      connect((Connect)message);
    }

    if (message instanceof ExecuteJob) {
      executeJob((ExecuteJob) message);
    }

    if (message instanceof InactivityCheck) {
      checkInactivity();
    }

    if (message instanceof TerminateInactivityCheck) {
      checkTerminationInactivity();
    }
  }

  private void connect(Connect message) {
    // check the connectable
    if (connectable != null) {
      connectable = new HiveConnectionWrapper(message.getJdbcUrl(), message.getUsername(), message.getPassword());
    }
    // make the connectable to Hive
    try {
      if(!connectable.isOpen()) {
        connectable.connect();
      }
    } catch (ConnectionException e) {

    }

    this.terminateActorScheduler = system.scheduler().schedule(
      Duration.Zero(), Duration.create(60 * 1000, TimeUnit.MILLISECONDS),
      this.getSelf(), new TerminateInactivityCheck(), system.dispatcher(), null);

  }

  private void executeJob(ExecuteJob message) {
    if (connectable == null) {
      throw new NotConnectedException("Cannot execute job for id: " + message.getJobId() + ", user: " + message.getUsername() + ". Not connected to Hive");
    }

    Optional<HiveConnection> connectionOptional  = connectable.getConnection();
    if (!connectionOptional.isPresent()) {
      throw new NotConnectedException("Cannot execute job for id: " + message.getJobId() + ", user: " + message.getUsername() + ". Not connected to Hive");
    }
    Optional<ResultSet> resultSetOptional = connectionDelegate.execute(connectionOptional.get(), message);

    if (resultSetOptional.isPresent()) {
      // Start a sub-actor to fetch the resultset Information
      // Start a actor to query ATS
      // Start a actor to query log
    } else {
      // Write the job completion status in DB
    }

    // Start Inactivity timer to close the statement
    this.inactivityScheduler = system.scheduler().schedule(
      Duration.Zero(), Duration.create(15 * 1000, TimeUnit.MILLISECONDS),
      this.self(), new InactivityCheck(), system.dispatcher(), null);
  }

  private void checkInactivity() {
    long current = System.currentTimeMillis();
    if ((current - lastActivityTimestamp) > MAX_INACTIVITY_INTERVAL) {
      // Stop all the sub-actors created
      try {
        connectionDelegate.closeStatement();
        connectionDelegate.closeResultSet();
      } catch (SQLException e) {
        // TODO: check this
      }
      // Tell the router actor to render this connectable actor as free.
      parent.tell(new FreeConnector(username, jobId), this.self());
      inactivityScheduler.cancel();
    }
  }

  private void checkTerminationInactivity() {
    long current = System.currentTimeMillis();
    if((current - lastActivityTimestamp) > MAX_TERMINATION_INACTIVITY_INTERVAL) {
      // Stop all sub-actors if any currently live
      try {
        connectionDelegate.closeStatement();
        connectionDelegate.closeResultSet();
      } catch (SQLException e) {
        // TODO: check this
      }
      // Tell the router actor to remove the reference from its cache
      parent.tell(new DestroyConnector(username, jobId), this.self());
      self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }
  }

  @Override
  public void postStop() throws Exception {
    if (!(inactivityScheduler == null || inactivityScheduler.isCancelled())) {
      inactivityScheduler.cancel();
    }
    if (!(terminateActorScheduler == null || terminateActorScheduler.isCancelled())) {
      terminateActorScheduler.cancel();
    }

    if (connectable.isOpen()) {
      connectable.disconnect();
    }
  }
}
