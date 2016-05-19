package org.apache.ambari.view.hive2.actor;

import akka.actor.UntypedActor;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;

/**
 * Queries YARN/ATS time to time to fetch the status of the ExecuteJob and updates database
 */
public class YarnAtsParser extends HiveActor {
  @Override
  public void handleMessage(HiveMessage hiveMessage) {

  }
}
