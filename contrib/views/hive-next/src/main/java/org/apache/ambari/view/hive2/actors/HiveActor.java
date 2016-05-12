package org.apache.ambari.view.hive2.actors;

import org.apache.ambari.view.hive2.internal.HiveTask;

public interface HiveActor {

    /**
     * Execute a Hive Task
     * @param hiveTask
     */
    void execute(HiveTask hiveTask);

    void closeConnection();

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
