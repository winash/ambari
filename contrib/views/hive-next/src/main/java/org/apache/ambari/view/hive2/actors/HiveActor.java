package org.apache.ambari.view.hive2.actors;

import org.apache.ambari.view.hive2.internal.HiveTask;
import org.apache.ambari.view.hive2.actor.message.Connect;

public interface HiveActor {

  void connect(Connect properties);
  /**
   * Execute a Hive Task
   *
   * @param hiveTask
   */
  void execute(HiveTask hiveTask);

  void closeConnection();

  void checkInactivity();

  void checkTerminationInactivity();

  /**
   * Actor lifecycle
   */
  enum Status {
    DISCONNECTED,
    CONNECTED,
    EXECUTING,
    DONE,
    IDLE
  }

}
