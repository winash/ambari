package org.apache.ambari.view.hive2.actors;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.messages.HiveTask;

public interface HiveActor {

    void execute(HiveTask hiveTask);


}
