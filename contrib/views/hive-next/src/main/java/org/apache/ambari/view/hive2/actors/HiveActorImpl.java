package org.apache.ambari.view.hive2.actors;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.internal.HiveTask;

import java.io.IOException;

public class HiveActorImpl implements HiveActor {

    private Optional<HiveConnection> connection = Optional.absent();

    @Override
    public void execute(HiveTask hiveTask) {
        // Connect if no connection
        //and process the task at hand
    }

    @Override
    public void closeConnection() {
        if(connection.isPresent()){
            try {
                connection.get().close();
            } catch (IOException e) {
                // TODO: report close error and evict parent cache
            }
        }
    }
}
