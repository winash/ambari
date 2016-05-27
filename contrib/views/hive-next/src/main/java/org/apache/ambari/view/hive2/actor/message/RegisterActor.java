package org.apache.ambari.view.hive2.actor.message;

import akka.actor.ActorRef;

public class RegisterActor {

    private ActorRef actorRef;

    public RegisterActor(ActorRef actorRef) {
        this.actorRef = actorRef;
    }

    public ActorRef getActorRef() {
        return actorRef;
    }
}
