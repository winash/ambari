package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.persistence.DataStoreStorage;
import org.apache.ambari.view.hive2.HiveJdbcConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.DestroyConnector;
import org.apache.ambari.view.hive2.actor.message.ExecuteAsyncJob;
import org.apache.ambari.view.hive2.actor.message.ExecuteSyncJob;
import org.apache.ambari.view.hive2.actor.message.FetchResult;
import org.apache.ambari.view.hive2.actor.message.FreeConnector;
import org.apache.ambari.view.hive2.actor.message.HiveJob;
import org.apache.ambari.view.hive2.actor.message.Job;
import org.apache.ambari.view.hive2.actor.message.JobRejected;
import org.apache.ambari.view.hive2.actor.message.JobSubmitted;
import org.apache.ambari.view.hive2.actor.message.ResultReady;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.hive2.internal.ExecutionResult;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.ambari.view.utils.hdfs.HdfsApiException;
import org.apache.commons.collections4.map.HashedMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

/**
 * Router actor to control the operations. This delegates the operations to underlying child actors and
 * store the state for them.
 */
public class OperationController extends UntypedActor {

  private final ViewContext viewContext;
  private final ActorSystem system;

  /**
   * Store the connection per user which are currently not working
   */
  private final Map<String, Queue<ActorRef>> availableConnections;

  /**
   * Store the connection per user/per job which are currently working.
   */
  private final Map<String, Map<String, Container>> busyConnections;

  /**
   * Store the connection per user which will be used to execute sync jobs
   * like fetching databases, tables etc.
   */
  private final Map<String, List<ActorRef>> syncBusyConnections;

  public OperationController(ViewContext viewContext, ActorSystem system) {
    this.viewContext = viewContext;
    this.system = system;
    this.availableConnections = new HashMap<>();
    this.busyConnections = new HashedMap<>();
    this.syncBusyConnections = new HashMap<>();
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Job) {
      Job job = (Job) message;
      if(job.getJob().getType() == HiveJob.Type.ASYNC) {
        sendJob(job.getConnect(), (ExecuteAsyncJob)job.getJob());
      } else if (job.getJob().getType() == HiveJob.Type.SYNC) {
        sendSyncJob(job.getConnect(), (ExecuteSyncJob) job.getJob());
      }
    }

    if(message instanceof ResultReady){
      updateResultContainer((ResultReady)message);
    }

    if(message instanceof GetResultHolder){
      getResultHolder((GetResultHolder)message);
    }

    if(message instanceof FetchResult){
      fetchResultActorRef((FetchResult)message);

    }

    if (message instanceof FreeConnector) {
      freeConnector((FreeConnector) message);
    }

    if (message instanceof DestroyConnector) {
      destroyConnector((DestroyConnector) message);
    }
  }

  private void getResultHolder(GetResultHolder message) {
    sender().tell(busyConnections.get(message.getUserName()).get(message.getJobId()).result,self());
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
    Either<ActorRef, ExecutionResult> result = busyConnections.get(username).get(jobId).result;
    sender().tell(result,self());

  }

  private void sendJob(Connect connect, ExecuteAsyncJob job) {
    String username = connect.getUsername();
    String jobId = job.getJobId();
    ActorRef subActor = null;
    // Check if there is available actors to process this
    if(availableConnections.containsKey(username)) {
      Queue<ActorRef> availableActors = availableConnections.get(username);
      if(availableActors.size() != 0) {
        subActor = availableActors.poll();
      }
    } else {
      availableConnections.put(username, new LinkedList<ActorRef>());
    }

    if (subActor == null) {

      HdfsApi hdfsApi;
      try {
        hdfsApi = getHdfsApi();
      } catch (HdfsApiException e) {
        // TODO: LOG Here
        sender().tell(new JobRejected(username, jobId, "Failed to connect to HDFS."), ActorRef.noSender());
        return;
      }

      subActor = getContext().actorOf(
        Props.create(JdbcConnector.class,viewContext, hdfsApi, system, self(), new HiveJdbcConnectionDelegate(),new DataStoreStorage(viewContext)),
        username + ":" + UUID.randomUUID().toString());

    }

    if (busyConnections.containsKey(username)) {
      Map<String, Container> actors = busyConnections.get(username);
      if(!actors.containsKey(jobId)) {
        actors.put(jobId, new Container(subActor));
      } else {
        // Reject this as with the same jobId one connection is already in progress.
        sender().tell(new JobRejected(username, jobId, "Existing job in progress with same jobId."), ActorRef.noSender());
      }
    } else {
      Map<String, Container> actors = new HashMap<>();
      actors.put(jobId, new Container(subActor));
      busyConnections.put(username, actors);
    }

    subActor.tell(connect, self());
    subActor.tell(job, self());

    sender().tell(new JobSubmitted(username, jobId), ActorRef.noSender());
  }

  private void sendSyncJob(Connect connect, ExecuteSyncJob job) {
    String username = job.getUsername();
    ActorRef subActor = null;
    // Check if there is available actors to process this
    if(availableConnections.containsKey(username)) {
      Queue<ActorRef> availableActors = availableConnections.get(username);
      if(availableActors.size() != 0) {
        subActor = availableActors.poll();
      }
    } else {
      availableConnections.put(username, new LinkedList<ActorRef>());
    }

    if (subActor == null) {
      HdfsApi hdfsApi;
      try {
        hdfsApi = getHdfsApi();
      } catch (HdfsApiException e) {
        // TODO: LOG Here
        sender().tell(new JobRejected(username, Job.SYNC_JOB_MARKER, "Failed to connect to HDFS."), ActorRef.noSender());
        return;
      }

      subActor = getContext().actorOf(
        Props.create(JdbcConnector.class,viewContext, hdfsApi, system, self(), new HiveJdbcConnectionDelegate(),new DataStoreStorage(viewContext)),
        username + ":" + UUID.randomUUID().toString());
    }

    if (syncBusyConnections.containsKey(username)) {
      List<ActorRef> actors = syncBusyConnections.get(username);
      actors.add(subActor);
    } else {
      List<ActorRef> actors = new ArrayList<>();
      actors.add(subActor);
      syncBusyConnections.put(username, actors);
    }

    subActor.tell(connect, self());
    subActor.tell(job, self());
  }

  private HdfsApi getHdfsApi() throws HdfsApiException {
    return null;
//    return HdfsUtil.connectToHDFSApi(viewContext);
  }

  private void destroyConnector(DestroyConnector message) {
    ActorRef sender = getSender();
    removeFromBusy(message.getUsername(), message.getJobId());
    removeFromAvailable(message.getUsername(), sender);
  }

  private void freeConnector(FreeConnector message) {
    Optional<ActorRef> refOptional = removeFromBusy(message.getUsername(), message.getJobId());
    if(refOptional.isPresent()) {
      addToAvailable(message.getUsername(), refOptional.get());
    }
  }

  private Optional<ActorRef> removeFromBusy(String username, String jobId) {
    ActorRef ref = null;
    if (busyConnections.containsKey(username)) {
      Map<String, Container> actors = busyConnections.get(username);
      if(actors.containsKey(jobId)) {
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
    if(!availableConnections.containsKey(username)) {
      return;
    }
    Queue<ActorRef> actors = availableConnections.get(username);
    actors.remove(sender);
  }

  private static class Container {

    ActorRef actorRef;
    Either<ActorRef,ExecutionResult> result = Either.none();

    public Container(ActorRef actorRef) {
      this.actorRef = actorRef;
    }
  }



}


