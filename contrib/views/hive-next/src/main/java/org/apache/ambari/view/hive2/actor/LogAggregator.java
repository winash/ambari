package org.apache.ambari.view.hive2.actor;

import akka.actor.UntypedActor;

/**
 * Reads the logs for a Job from the Statement and writes them into hdfs.
 */
public class LogAggregator extends UntypedActor {
  @Override
  public void onReceive(Object message) throws Exception {

  }
}
