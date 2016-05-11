package org.apache.ambari.view.hive2.actors;

import org.apache.ambari.view.hive2.internal.HiveTask;

public interface HiveTaskRouter {

    void execute(HiveTask hiveTask);

}
