package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.HiveJdbcConnectionDelegate;
import org.apache.ambari.view.hive2.actor.message.DestroyConnector;
import org.apache.ambari.view.hive2.actor.message.FreeConnector;
import org.apache.ambari.view.hive2.actor.message.Job;
import org.apache.ambari.view.hive2.actor.message.JobRejected;
import org.apache.ambari.view.hive2.actor.message.JobSubmitted;
import org.apache.commons.collections4.map.HashedMap;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

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
  private final Map<String, Map<String, ActorRef>> busyConnections;

  public OperationController(ViewContext viewContext, ActorSystem system) {
    this.viewContext = viewContext;
    this.system = system;
    this.availableConnections = new HashMap<>();
    this.busyConnections = new HashedMap<>();
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Job) {
      sendJob((Job) message);
    }

    if (message instanceof FreeConnector) {
      freeConnector((FreeConnector) message);
    }

    if (message instanceof DestroyConnector) {
      destroyConnector((DestroyConnector) message);
    }
  }

  private void sendJob(Job job) {
    String username = job.connect.getUsername();
    String jobId = job.executeJob.getJobId();
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
      subActor = getContext().actorOf(
        Props.create(JdbcConnector.class,viewContext, system, self(), new HiveJdbcConnectionDelegate()),
        username + ":" + jobId);
    }

    if (busyConnections.containsKey(username)) {
      Map<String, ActorRef> actors = busyConnections.get(username);
      if(!actors.containsKey(jobId)) {
        actors.put(jobId, subActor);
      } else {
        // Reject this as with the same jobId one connection is already in progress.
        sender().tell(new JobRejected(username, jobId, "Existing job in progress with same jobId."), ActorRef.noSender());
      }
    } else {
      Map<String, ActorRef> actors = new HashMap<>();
      actors.put(jobId, subActor);
      busyConnections.put(username, actors);
    }

    subActor.tell(job.connect, self());
    subActor.tell(job.executeJob, self());

    sender().tell(new JobSubmitted(username, jobId), ActorRef.noSender());
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
      Map<String, ActorRef> actors = busyConnections.get(username);
      if(actors.containsKey(jobId)) {
        ref = actors.get(jobId);
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
}
