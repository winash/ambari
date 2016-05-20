package org.apache.ambari.view.hive2.actor.message.job;

import akka.actor.ActorRef;

public class ResultSetHolder {
  private final ActorRef iterator;

  public ResultSetHolder(ActorRef iterator) {
    this.iterator = iterator;
  }

  public ActorRef getIterator() {
    return iterator;
  }
}
