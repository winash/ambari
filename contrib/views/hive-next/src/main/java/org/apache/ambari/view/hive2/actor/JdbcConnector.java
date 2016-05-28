package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.RegisterActor;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.lifecycle.CleanUp;
import org.apache.ambari.view.hive2.actor.message.lifecycle.DestroyConnector;
import org.apache.ambari.view.hive2.actor.message.lifecycle.FreeConnector;
import org.apache.ambari.view.hive2.actor.message.lifecycle.InactivityCheck;
import org.apache.ambari.view.hive2.actor.message.lifecycle.KeepAlive;
import org.apache.ambari.view.hive2.actor.message.lifecycle.TerminateInactivityCheck;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.persistence.Storage;
import org.apache.ambari.view.hive2.persistence.utils.ItemNotFound;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.JobImpl;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;


/**
 * Wraps one Jdbc connection per user, per instance. This is used to delegate execute the statements and
 * creates child actors to delegate the resultset extraction, YARN/ATS querying for ExecuteJob info and Log Aggregation
 */
public abstract class JdbcConnector extends HiveActor {

  private final Logger LOG = LoggerFactory.getLogger(getClass());

  /**
   * Interval for maximum inactivity allowed
   */
  private final static long MAX_INACTIVITY_INTERVAL = 3 * 60 * 1000;

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
  protected final ActorRef deathWatch;
  protected final ConnectionDelegate connectionDelegate;
  protected final ActorRef parent;
  protected final ActorRef exceptionWriter;
  protected final HdfsApi hdfsApi;

  /**
   * true if the actor is currently executing any job.
   */
  private boolean executing = false;

  /**
   * true if the currently executing job is async job.
   */
  private boolean async = true;
  protected String username;
  protected String jobId;

  public JdbcConnector(ViewContext viewContext, HdfsApi hdfsApi, ActorSystem system, ActorRef parent, ActorRef deathWatch,
                       ConnectionDelegate connectionDelegate, Storage storage) {
    this.viewContext = viewContext;
    this.hdfsApi = hdfsApi;
    this.system = system;
    this.parent = parent;
    this.deathWatch = deathWatch;
    this.connectionDelegate = connectionDelegate;
    this.storage = storage;
    this.lastActivityTimestamp = System.currentTimeMillis();
    exceptionWriter = getContext().actorOf(Props.create(ExceptionWriter.class, hdfsApi, storage), "Exception-Writer-" + viewContext.getUsername() + "-" + viewContext.getInstanceName());
    deathWatch.tell(new RegisterActor(exceptionWriter), self());
  }

  @Override
  public void handleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();
    if (message instanceof InactivityCheck) {
      checkInactivity();
    } else if (message instanceof TerminateInactivityCheck) {
      checkTerminationInactivity();
    } else if (message instanceof KeepAlive) {
      keepAlive();
    } else if (message instanceof CleanUp) {
      cleanUp();
    } else {
      handleNonLifecycleMessage(hiveMessage);
    }
  }

  private void handleNonLifecycleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();
    keepAlive();
    if (message instanceof Connect) {
      connect((Connect) message);
    } else {
      handleJobMessage(hiveMessage);
    }

  }

  protected abstract void handleJobMessage(HiveMessage message);

  protected abstract boolean isAsync();

  protected abstract void cleanUpChildren();

  private void keepAlive() {
    LOG.info("Keep alive for {} sent by {}", self(), sender());
    lastActivityTimestamp = System.currentTimeMillis();
  }

  protected Optional<String> getJobId() {
    return Optional.fromNullable(jobId);
  }

  protected Optional<String> getUsername() {
    return Optional.fromNullable(username);
  }

  private void connect(Connect message) {
    this.username = message.getUsername();
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
      exceptionWriter.tell(new ExecutionFailed(e.getMessage(), e), ActorRef.noSender());
    }

    this.terminateActorScheduler = system.scheduler().schedule(
      Duration.Zero(), Duration.create(60 * 1000, TimeUnit.MILLISECONDS),
      this.getSelf(), new TerminateInactivityCheck(), system.dispatcher(), null);

  }

  protected void updateGuidInJob(String jobId, HiveStatement statement) {
    String yarnAtsGuid = statement.getYarnATSGuid();
    try {
      JobImpl job = storage.load(JobImpl.class, jobId);
      job.setGuid(yarnAtsGuid);
      storage.store(JobImpl.class, job);
    } catch (ItemNotFound itemNotFound) {
      // Cannot do anything if the job is not present
    }
  }

  private void checkInactivity() {
    LOG.info("Inactivity check");
    long current = System.currentTimeMillis();
    long l = current - lastActivityTimestamp;
    if (l > MAX_INACTIVITY_INTERVAL) {
      // Stop all the sub-actors created
      cleanUp();
    }
  }

  private void checkTerminationInactivity() {
    if (!isAsync()) {
      // Should not terminate if job is sync. Will terminate after the job is finished.
      stopTeminateInactivityScheduler();
      return;
    }
    long current = System.currentTimeMillis();
    if ((current - lastActivityTimestamp) > MAX_TERMINATION_INACTIVITY_INTERVAL) {
      cleanUpWithTermination();
    }
  }

  protected void cleanUp() {
    cleanUpStatementAndResultSet();
    LOG.info("Sending poison pill to exception writer");

    cleanUpChildren();
    cleanupExceptionWriter();
    stopInactivityScheduler();
    parent.tell(new FreeConnector(username, jobId, isAsync()), self());
  }

  protected void cleanUpWithTermination() {
    cleanUpStatementAndResultSet();

    cleanUpChildren();
    cleanupExceptionWriter();
    stopInactivityScheduler();
    stopTeminateInactivityScheduler();
    parent.tell(new DestroyConnector(username, jobId, isAsync()), this.self());
    self().tell(PoisonPill.getInstance(), ActorRef.noSender());
  }

  private void cleanupExceptionWriter() {
    if (!(exceptionWriter == null || exceptionWriter.isTerminated())) {
      LOG.debug("Sending poison pill to exception writer from: {}", getSelf().path());
      exceptionWriter.tell(PoisonPill.getInstance(), self());
    }
  }

  private void cleanUpStatementAndResultSet() {
    connectionDelegate.closeStatement();
    connectionDelegate.closeResultSet();
  }

  private void stopTeminateInactivityScheduler() {
    if (!(terminateActorScheduler == null || terminateActorScheduler.isCancelled())) {
      terminateActorScheduler.cancel();
    }
  }

  private void stopInactivityScheduler() {
    if (!(inactivityScheduler == null || inactivityScheduler.isCancelled())) {
      inactivityScheduler.cancel();
    }
  }

  @Override
  public void postStop() throws Exception {
    stopInactivityScheduler();
    stopTeminateInactivityScheduler();

    if (connectable.isOpen()) {
      connectable.disconnect();
    }
  }


}
