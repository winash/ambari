package org.apache.ambari.view.hive2;


import akka.actor.ActorSystem;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.ambari.view.ViewContext;

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

    @Inject
    ViewContext viewContext;


    public HiveActorSystem(){


    }

}
