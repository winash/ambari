package org.apache.ambari.view.hive2.actors;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.messages.HiveTask;

public class HiveActorImpl implements HiveActor {

    private Optional<HiveConnection> connection = Optional.absent();

    @Override
    public void execute(HiveTask hiveTask) {
        // Connect if no connection
        //and process the task at hand
    }
}
