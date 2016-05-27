package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.RegisterActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class DeathWatch extends HiveActor {

    private final static Logger LOG =
            LoggerFactory.getLogger(DeathWatch.class);

    @Override
    void handleMessage(HiveMessage hiveMessage) {
        Object message = hiveMessage.getMessage();
        if(message instanceof RegisterActor){
            RegisterActor registerActor = (RegisterActor) message;
            ActorRef actorRef = registerActor.getActorRef();
            this.getContext().watch(actorRef);
            LOG.info("Registered new actor "+ actorRef);
            LOG.info("Registration for {} at {}", actorRef,new Date());
        }
        if(message instanceof Terminated){
            Terminated terminated = (Terminated) message;
            ActorRef actor = terminated.actor();
            LOG.info("Received deathwatch for actor "+ actor);
            LOG.info("Termination for {} at {}", actor,new Date());

        }

    }
}
