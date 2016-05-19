package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.persistence.Storage;
import org.apache.ambari.view.hive.persistence.utils.ItemNotFound;
import org.apache.ambari.view.hive.resources.jobs.viewJobs.JobImpl;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.AssignResultSet;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.DestroyConnector;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.ExecuteQuery;
import org.apache.ambari.view.hive2.actor.message.HiveJob;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.SyncJob;
import org.apache.ambari.view.hive2.actor.message.FreeConnector;
import org.apache.ambari.view.hive2.actor.message.InactivityCheck;
import org.apache.ambari.view.hive2.actor.message.StartLogAggregation;
import org.apache.ambari.view.hive2.actor.message.TerminateInactivityCheck;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.job.NoResult;
import org.apache.ambari.view.hive2.actor.message.job.ResultSetHolder;
import org.apache.ambari.view.hive2.exceptions.NotConnectedException;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;
import scala.concurrent.duration.Duration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Wraps one Jdbc connection per user, per instance. This is used to delegate execute the statements and
 * creates child actors to delegate the resultset extraction, YARN/ATS querying for ExecuteJob info and Log Aggregation
 */
public abstract class JdbcConnector extends HiveActor {

  /**
   * Interval for maximum inactivity allowed
   */
  private final static long MAX_INACTIVITY_INTERVAL = 5 * 10 * 1000;

  /**
   * Interval for maximum inactivity allowed before termination
   */
  private static final long MAX_TERMINATION_INACTIVITY_INTERVAL = 10 * 60 * 1000;

  protected final ViewContext viewContext;
  protected final ActorSystem system;
  protected final Storage storage;

  /**
   * Keeps track of the timestamp when the last activity has happened. This is
   * used to calculate the inactivity period and take lifecycle decisions based
   * on it.
   */
  private long lastActivityTimestamp;

  /**
   * Akka scheduler to tick at an interval to deal with inactivity of this actor
   */
  protected Cancellable inactivityScheduler;

  /**
   * Akka scheduler to tick at an interval to deal with the inactivity after which
   * the actor should be killed and connectable should be released
   */
  protected Cancellable terminateActorScheduler;

  protected Connectable connectable = null;
  protected final ConnectionDelegate connectionDelegate;
  protected final ActorRef parent;
  protected final HdfsApi hdfsApi;

  // The result Holder assigned to this Connector
  protected ActorRef resultHolder;


  /**
   * true if the actor is currently executing any job.
   */
  private boolean executing = false;

  /**
   * true if the currently executing job is async job.
   */
  private boolean async = true;

  public JdbcConnector(ViewContext viewContext, HdfsApi hdfsApi, ActorSystem system, ActorRef parent,
                       ConnectionDelegate connectionDelegate, Storage storage) {
    this.viewContext = viewContext;
    this.hdfsApi = hdfsApi;
    this.system = system;
    this.parent = parent;
    this.connectionDelegate = connectionDelegate;
    this.storage = storage;
    this.lastActivityTimestamp = System.currentTimeMillis();
  }

  @Override
  public void handleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();
    if (message instanceof Connect) {
      connect((Connect) message);
    } else if (message instanceof InactivityCheck) {
      checkInactivity((InactivityCheck) message);
    } else if (message instanceof TerminateInactivityCheck) {
      checkTerminationInactivity((TerminateInactivityCheck)message);
    } else {
      handleJobMessage(hiveMessage);
    }
  }

  protected abstract void handleJobMessage(HiveMessage message);

  private void connect(Connect message) {
    // check the connectable
    if (connectable == null) {
      connectable = message.getConnectable();
    }
    // make the connectable to Hive
    try {
      if (!connectable.isOpen()) {
        connectable.connect();
      }
    } catch (ConnectionException e) {
      //TODO: Terminate the actor immedeatly
    }

    this.terminateActorScheduler = system.scheduler().schedule(
      Duration.Zero(), Duration.create(60 * 1000, TimeUnit.MILLISECONDS),
      this.getSelf(), new TerminateInactivityCheck(message), system.dispatcher(), null);

  }

  private void updateGuidInJob(String jobId, HiveStatement statement) {
    String yarnAtsGuid = statement.getYarnATSGuid();
    try {
      JobImpl job = storage.load(JobImpl.class, jobId);
      job.setGuid(yarnAtsGuid);
      storage.store(JobImpl.class, job);
    } catch (ItemNotFound itemNotFound) {
      // Cannot do anything if the job is not present
    }

  }

  private void checkInactivity(InactivityCheck message) {
    long current = System.currentTimeMillis();
    long l = current - lastActivityTimestamp;
    System.out.println(l);
    if (l > MAX_INACTIVITY_INTERVAL) {
      // Stop all the sub-actors created
      try {
        connectionDelegate.closeStatement();
        connectionDelegate.closeResultSet();
      } catch (SQLException e) {
        // TODO: check this
      }
      //Poison the Result holder
      resultHolder.tell(PoisonPill.getInstance(), self());
//      //nullify the reference
      resultHolder = null;
      // Tell the router actor to remove the reference from its cache
      // Tell the router actor to render this connectable actor as free.
      if(message.isJobSync()){
        parent.tell(new FreeConnector(self(),message), this.self());
      }else {
        parent.tell(new FreeConnector(message), this.self());
      }
      inactivityScheduler.cancel();
    }
  }

  private void checkTerminationInactivity(TerminateInactivityCheck message) {
    long current = System.currentTimeMillis();
    if ((current - lastActivityTimestamp) > MAX_TERMINATION_INACTIVITY_INTERVAL) {
      // Stop all sub-actors if any currently live
      try {
        connectionDelegate.closeStatement();
        connectionDelegate.closeResultSet();
      } catch (SQLException e) {
        // TODO: check this
      }
      if(resultHolder != null){
        resultHolder.tell(PoisonPill.getInstance(), self());
       //nullify the reference
        resultHolder = null;
      }

      if(message.isJobSync()){
        parent.tell(new DestroyConnector(self(),message.getUserName()), this.self());
      } else {
        parent.tell(new DestroyConnector(message.getUserName(),message.getJobId()), this.self());

      }
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
