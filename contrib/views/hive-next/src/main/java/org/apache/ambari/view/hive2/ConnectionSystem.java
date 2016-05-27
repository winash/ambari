package org.apache.ambari.view.hive2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.apache.ambari.view.hive2.actor.DeathWatch;
import org.apache.ambari.view.hive2.actor.OperationController;
import org.apache.ambari.view.hive2.internal.ConnectionSupplier;
import org.apache.ambari.view.hive2.internal.DataStorageSupplier;
import org.apache.ambari.view.hive2.internal.HdfsApiSupplier;

public class ConnectionSystem {

  private static final String ACTOR_SYSTEM_NAME = "HiveViewActorSystem";
  private ActorSystem actorSystem = null;
  private static volatile ConnectionSystem instance = null;
  private static final Object lock = new Object();

  private ActorRef operationController = null;

  private ConnectionSystem() {
    actorSystem = ActorSystem.create(ACTOR_SYSTEM_NAME);
    createOperationController();
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

  private void createOperationController() {
    ActorRef deathWatch = actorSystem.actorOf(Props.create(DeathWatch.class));
    this.operationController = actorSystem.actorOf(Props.create(OperationController.class, actorSystem,deathWatch, new ConnectionSupplier(), new DataStorageSupplier(), new HdfsApiSupplier()));
  }

  public ActorSystem getActorSystem() {
    return actorSystem;
  }

  public ActorRef getOperationController() {
    return operationController;
  }

  public void shutdown() {
    if(!actorSystem.isTerminated()) {
      actorSystem.shutdown();
    }
  }
}
