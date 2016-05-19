package org.apache.ambari.view.hive2.actor.message.job;

import akka.actor.ActorRef;

/**
 * Created by dbhowmick on 5/19/16.
 */
public class ResultSetHolder {
  private final ActorRef iterator;

  public ResultSetHolder(ActorRef iterator) {
    this.iterator = iterator;
  }

  public ActorRef getIterator() {
    return iterator;
  }
}
