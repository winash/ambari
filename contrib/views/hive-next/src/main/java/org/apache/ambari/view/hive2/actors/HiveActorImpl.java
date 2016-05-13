package org.apache.ambari.view.hive2.actors;

import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.TypedActor;
import akka.actor.TypedProps;
import akka.japi.Creator;
import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.exceptions.NotConnectedException;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.HiveTask;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.hive.jdbc.HiveConnection;
import scala.concurrent.duration.Duration;

import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

public class HiveActorImpl implements HiveActor, TypedActor.PostStop {

  /**
   * Interval for maximum inactivity allowed
   */
  private final static long MAX_INACTIVITY_INTERVAL = 5 * 60 * 1000;

  /**
   * Interval for maximum inactivity allowed before termination
   */
  private static final long MAX_TERMINATION_INACTIVITY_INTERVAL = 10 * 60 * 1000;

  /**
   * The dispatcher takes care of running the query
   * and has semantics for timeouts and parsing result sets
   * The dispatcher lifecycle is tied to the lifecycle of
   * the Hive actor which composed it
   */
  private final HiveQueryDispatch dispatch;
  private final ActorSystem system;
  private Connectable connectable = null;
  private Status status = Status.IDLE;

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

  public HiveActorImpl(ActorSystem system) {
    this.system = system;
    // build query dispatcher
    this.dispatch = TypedActor.get(system).typedActorOf(
      new TypedProps<>(HiveQueryDispatch.class,
        new Creator<HiveQueryDispatch>() {
          public HiveQueryDispatch create() {
            return new HiveQueryDispatchImpl();
          }
        }));

    this.lastActivityTimestamp = System.currentTimeMillis();
  }

  @Override
  public void connect(Connect properties) {
    // check the connectable
    if (connectable != null) {
      //connectable = new HiveConnectionWrapper(properties);
    }
    // make the connectable to Hive
    try {
      if(!connectable.isOpen()) {
        connectable.connect();
      }
    } catch (ConnectionException e) {
      status = Status.DISCONNECTED;
    }

    this.terminateActorScheduler = system.scheduler().schedule(
      Duration.Zero(), Duration.create(60 * 1000, TimeUnit.MILLISECONDS),
      getTeminationCheckRunnable(), system.dispatcher());
  }

  @Override
  public void execute(HiveTask hiveTask) {

    if (connectable == null) {
      throw new NotConnectedException("Cannot execute job for id: " + hiveTask.getId() + ", user: " + hiveTask.getUser() + ". Not connected to Hive");
    }

    Optional<HiveConnection> connectionOptional  = connectable.getConnection();
    if (!connectionOptional.isPresent()) {
      throw new NotConnectedException("Cannot execute job for id: " + hiveTask.getId() + ", user: " + hiveTask.getUser() + ". Not connected to Hive");
    }
    Optional<ResultSet> resultSetOptional = dispatch.executeQuery(connectionOptional.get(), hiveTask);

    this.inactivityScheduler = system.scheduler().schedule(
      Duration.Zero(), Duration.create(15 * 1000, TimeUnit.MILLISECONDS),
      getInactivityRunnable(), system.dispatcher());
  }

  @Override
  public void closeConnection() {
    /*if (connectable.isPresent()) {
      try {
        connectable.get().disconnect();

      } catch (ConnectionException e) {
        e.printStackTrace();
      }
    }*/
  }

  @Override
  public void checkInactivity() {
    long current = System.currentTimeMillis();
    if ((current - lastActivityTimestamp) > MAX_INACTIVITY_INTERVAL) {
      // Stop all the sub-actors created
      // Cancel Statement and resultSet
      // Tell the router actor to render this connectable actor as free.
      inactivityScheduler.cancel();
    }
  }

  @Override
  public void checkTerminationInactivity() {
    long current = System.currentTimeMillis();
    if((current - lastActivityTimestamp) > MAX_TERMINATION_INACTIVITY_INTERVAL) {
      // Stop all sub-actors if any currently live
      // Cancel Statement and resultSet if any
      // Tell the router actor to remove the reference from its cache
      // Send a poison pill to current
    }
  }

  @Override
  public void postStop() {
    closeConnection();
    if (!(inactivityScheduler == null || inactivityScheduler.isCancelled())) {
      inactivityScheduler.cancel();
    }
    terminateActorScheduler.cancel();
  }

  private Runnable getTeminationCheckRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        checkTerminationInactivity();
      }
    };
  }

  private Runnable getInactivityRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        checkInactivity();
      }
    };
  }
}
