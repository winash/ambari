package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.actor.message.RegisterActor;
import org.apache.ambari.view.hive2.persistence.Storage;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.FetchResult;
import org.apache.ambari.view.hive2.actor.message.HiveJob;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.JobRejected;
import org.apache.ambari.view.hive2.actor.message.ResultReady;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.lifecycle.DestroyConnector;
import org.apache.ambari.view.hive2.actor.message.lifecycle.FreeConnector;
import org.apache.ambari.view.hive2.internal.ContextSupplier;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.ambari.view.utils.hdfs.HdfsApiException;
import org.apache.ambari.view.utils.hdfs.HdfsUtil;
import org.apache.commons.collections4.map.HashedMap;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

/**
 * Router actor to control the operations. This delegates the operations to underlying child actors and
 * store the state for them.
 */
public class OperationController extends HiveActor {

  private final ActorSystem system;
  private final ActorRef deathWatch;
  private final ContextSupplier<ConnectionDelegate> connectionSupplier;
  private final ContextSupplier<Storage> storageSupplier;
  private final ContextSupplier<Optional<HdfsApi>> hdfsApiSupplier;

  /**
   * Store the connection per user which are currently not working
   */
  private final Map<String, Queue<ActorRef>> availableConnections;

  /**
   * Store the connection per user/per job which are currently working.
   */
  private final Map<String, Map<String, ActorRefResultContainer>> busyConnections;

  /**
   * Store the connection per user which will be used to execute sync jobs
   * like fetching databases, tables etc.
   */
  private final Map<String, Set<ActorRef>> syncBusyConnections;

  public OperationController(ActorSystem system,
                             ActorRef deathWatch,
                             ContextSupplier<ConnectionDelegate> connectionSupplier,
                             ContextSupplier<Storage> storageSupplier,
                             ContextSupplier<Optional<HdfsApi>> hdfsApiSupplier) {
    this.system = system;
    this.deathWatch = deathWatch;
    this.connectionSupplier = connectionSupplier;
    this.storageSupplier = storageSupplier;
    this.hdfsApiSupplier = hdfsApiSupplier;
    this.availableConnections = new HashMap<>();
    this.busyConnections = new HashedMap<>();
    this.syncBusyConnections = new HashMap<>();
  }

  @Override
  public void handleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();

    if (message instanceof ExecuteJob) {
      ExecuteJob job = (ExecuteJob) message;
      if (job.getJob().getType() == HiveJob.Type.ASYNC) {
        sendJob(job.getConnect(), (AsyncJob) job.getJob());
      } else if (job.getJob().getType() == HiveJob.Type.SYNC) {
        sendSyncJob(job.getConnect(), job.getJob());
      }
    }

    if (message instanceof ResultReady) {
      updateResultContainer((ResultReady) message);
    }

    if (message instanceof GetResultHolder) {
      getResultHolder((GetResultHolder) message);
    }

    if (message instanceof FetchResult) {
      fetchResultActorRef((FetchResult) message);
    }

    if (message instanceof FreeConnector) {
      System.out.println(getSender());
      System.out.println(message);
      freeConnector((FreeConnector) message);
    }

    if (message instanceof DestroyConnector) {
      System.out.println(getSender());
      System.out.println(message);
      destroyConnector((DestroyConnector) message);
    }
  }

  private void getResultHolder(GetResultHolder message) {
    String userName = message.getUserName();
    String jobId = message.getJobId();
    if(busyConnections.containsKey(userName) && busyConnections.get(userName).containsKey(jobId))
      sender().tell(busyConnections.get(userName).get(jobId).result, self());
    else {
      Either<ActorRef, AsyncExecutionFailed> right = Either.right(new AsyncExecutionFailed(message.getJobId(), "Could not find the job, maybe the pool expired"));
      sender().tell(right, self());
    }
  }

  private void updateResultContainer(ResultReady message) {
    // update the result
    String jobId = message.getJobId();
    String username = message.getUsername();
    busyConnections.get(username).get(jobId).result = message.getResult();
  }

  private void fetchResultActorRef(FetchResult message) {
    //Gets an Either actorRef,result implementation
    // and send back to the caller
    String username = message.getUsername();
    String jobId = message.getJobId();
    Either<ActorRef, ActorRef> result = busyConnections.get(username).get(jobId).result;
    sender().tell(result,self());

  }

  private void sendJob(Connect connect, AsyncJob job) {
    String username = job.getUsername();
    String jobId = job.getJobId();
    ActorRef subActor = null;
    // Check if there is available actors to process this
    subActor = getActorRefFromPool(username, subActor);
    ViewContext viewContext = job.getViewContext();
    if (subActor == null) {
      Optional<HdfsApi> hdfsApiOptional = hdfsApiSupplier.get(viewContext);
      if (!hdfsApiOptional.isPresent()) {
        sender().tell(new JobRejected(username, jobId, "Failed to connect to Hive."), ActorRef.noSender());
        return;
      }
      HdfsApi hdfsApi = hdfsApiOptional.get();

      subActor = system.actorOf(
        Props.create(AsyncJdbcConnector.class, viewContext, hdfsApi, system, self(),deathWatch, connectionSupplier.get(viewContext), storageSupplier.get(viewContext)),
        username + ":" + "jobId:" + jobId + ":" + UUID.randomUUID().toString() + ":asyncjdbcConnector");
      deathWatch.tell(new RegisterActor(subActor),self());

    }

    if (busyConnections.containsKey(username)) {
      Map<String, ActorRefResultContainer> actors = busyConnections.get(username);
      if (!actors.containsKey(jobId)) {
        actors.put(jobId, new ActorRefResultContainer(subActor));
      } else {
        // Reject this as with the same jobId one connection is already in progress.
        sender().tell(new JobRejected(username, jobId, "Existing job in progress with same jobId."), ActorRef.noSender());
      }
    } else {
      Map<String, ActorRefResultContainer> actors = new HashMap<>();
      actors.put(jobId, new ActorRefResultContainer(subActor));
      busyConnections.put(username, actors);
    }

    // set up the connect with ExecuteJob id for terminations
    subActor.tell(connect, self());
    subActor.tell(job, sender());

  }

  private ActorRef getActorRefFromPool(String username, ActorRef subActor) {
    if (availableConnections.containsKey(username)) {
      Queue<ActorRef> availableActors = availableConnections.get(username);
      if (availableActors.size() != 0) {
        subActor = availableActors.poll();
      }
    } else {
      availableConnections.put(username, new LinkedList<ActorRef>());
    }
    return subActor;
  }

  private void sendSyncJob(Connect connect, HiveJob job) {
    String username = job.getUsername();
    ActorRef subActor = null;
    // Check if there is available actors to process this
    subActor = getActorRefFromPool(username, subActor);
    ViewContext viewContext = job.getViewContext();

    if (subActor == null) {
      Optional<HdfsApi> hdfsApiOptional = hdfsApiSupplier.get(viewContext);
      if(!hdfsApiOptional.isPresent()){
          sender().tell(new JobRejected(username, ExecuteJob.SYNC_JOB_MARKER, "Failed to connect to HDFS."), ActorRef.noSender());
          return;
        }
      HdfsApi hdfsApi = hdfsApiOptional.get();

      subActor = system.actorOf(
        Props.create(SyncJdbcConnector.class, viewContext, hdfsApi, system, self(),deathWatch, connectionSupplier.get(viewContext), storageSupplier.get(viewContext)),
        username + ":" + UUID.randomUUID().toString() + ":SyncjdbcConnector" );
      deathWatch.tell(new RegisterActor(subActor),self());

    }

    if (syncBusyConnections.containsKey(username)) {
      Set<ActorRef> actors = syncBusyConnections.get(username);
      actors.add(subActor);
    } else {
      LinkedHashSet<ActorRef> actors = new LinkedHashSet<>();
      actors.add(subActor);
      syncBusyConnections.put(username, actors);
    }

    // Termination requires that the ref is known in case of sync jobs
    subActor.tell(connect, self());
    subActor.tell(job, sender());
  }


  private void destroyConnector(DestroyConnector message) {
    ActorRef sender = getSender();
    if (message.isForAsync()) {
      removeFromBusyPool(message.getUsername(), message.getJobId());
    } else {
      removeFromSyncPool(message.getUsername(), sender);
    }
    removeFromAvailable(message.getUsername(), sender);
  }

  private void freeConnector(FreeConnector message) {
    ActorRef sender = getSender();
    if (message.isForAsync()) {
      Optional<ActorRef> refOptional = removeFromBusyPool(message.getUsername(), message.getJobId());
      if (refOptional.isPresent()) {
        addToAvailable(message.getUsername(), refOptional.get());
      }
      return;
    }
    // Was a sync job, remove from sync pool
    Optional<ActorRef> refOptional = removeFromSyncPool(message.getUsername(), sender);
    if (refOptional.isPresent()) {
      addToAvailable(message.getUsername(), refOptional.get());
    }


  }

  private Optional<ActorRef> removeFromSyncPool(String userName, ActorRef refToFree) {
    if (syncBusyConnections.containsKey(userName)) {
      Set<ActorRef> actorRefs = syncBusyConnections.get(userName);
      actorRefs.remove(refToFree);
    }
    return Optional.of(refToFree);
  }

  private Optional<ActorRef> removeFromBusyPool(String username, String jobId) {
    ActorRef ref = null;
    if (busyConnections.containsKey(username)) {
      Map<String, ActorRefResultContainer> actors = busyConnections.get(username);
      if (actors.containsKey(jobId)) {
        ref = actors.get(jobId).actorRef;
        actors.remove(jobId);
      }
    }
    return Optional.fromNullable(ref);
  }

  private void addToAvailable(String username, ActorRef actor) {
    if (!availableConnections.containsKey(username)) {
      availableConnections.put(username, new LinkedList<ActorRef>());
    }

    Queue<ActorRef> availableActors = availableConnections.get(username);
    availableActors.add(actor);
  }

  private void removeFromAvailable(String username, ActorRef sender) {
    if (!availableConnections.containsKey(username)) {
      return;
    }
    Queue<ActorRef> actors = availableConnections.get(username);
    actors.remove(sender);
  }

  private static class ActorRefResultContainer {

    ActorRef actorRef;

    Either<ActorRef, ActorRef> result = Either.none();

    public ActorRefResultContainer(ActorRef actorRef) {
      this.actorRef = actorRef;
    }
  }


}


