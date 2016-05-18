package org.apache.ambari.view.hive2.actor;

import akka.actor.UntypedActor;

/**
 * Queries YARN/ATS time to time to fetch the status of the ExecuteJob and updates database
 */
public class YarnAtsParser extends UntypedActor {
  @Override
  public void onReceive(Object message) throws Exception {

  }
}
