package org.apache.ambari.view.hive2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.apache.ambari.view.hive2.actor.OperationController;

/**
 * Created by dbhowmick on 5/20/16.
 */
public class ConnectionSystem {

  private static final String ACTOR_SYSTEM_NAME = "HiveViewActorSystem";
  private ActorSystem actorSystem = null;
  private static volatile ConnectionSystem instance = null;
  private static final Object lock = new Object();

  private ActorRef operationController = null;

  private ConnectionSystem() {
    actorSystem = ActorSystem.create(ACTOR_SYSTEM_NAME);
  }

  public static ConnectionSystem getInstance() {
    if(instance == null) {
      synchronized (lock) {
        if(instance == null) {
          instance = new ConnectionSystem();
        }
      }
    }
    return instance;
  }

  public synchronized void intialize() {
    createOperationController();
  }

  private void createOperationController() {
  }

  public ActorSystem getActorSystem() {
    return actorSystem;
  }

  public void shutdown() {
    if(!actorSystem.isTerminated()) {
      actorSystem.shutdown();
    }
  }

  public ActorRef getOperationController() {
    return operationController;
  }
}
