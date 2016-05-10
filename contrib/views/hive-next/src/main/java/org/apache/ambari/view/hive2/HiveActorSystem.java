package org.apache.ambari.view.hive2;


import akka.actor.ActorSystem;
import akka.actor.TypedActor;
import akka.actor.TypedProps;
import akka.japi.Creator;
import com.google.inject.Singleton;
import org.apache.ambari.view.hive2.actors.HiveTaskRouter;
import org.apache.ambari.view.hive2.actors.HiveTaskRouterImpl;

@Singleton
/**
 * Holder class for the actor system
 * All system management methods should go here
 */
public class HiveActorSystem {

    public static final String HIVE_VIEW_SYSTEM = "HiveViewSystem";

    private final ActorSystem system = ActorSystem.create(HIVE_VIEW_SYSTEM);

    public ActorSystem getSystem() {
        return system;

    }

    private HiveTaskRouter hiveTaskRouter;

    public HiveActorSystem(){
        // Set up all the actors and start the system
          hiveTaskRouter = TypedActor.get(system).typedActorOf(
                  new TypedProps<>(HiveTaskRouter.class,
                          new Creator<HiveTaskRouter>() {
                              public HiveTaskRouter create() {
                                  return new HiveTaskRouterImpl(system);
                              }
                          }),
                        "name");


    }


    public HiveTaskRouter getRouter() {
        return hiveTaskRouter;
    }
}
