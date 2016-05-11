package org.apache.ambari.view.hive2.actors;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.HiveTask;

import java.sql.Connection;

public class HiveActorImpl implements HiveActor {

    private Optional<HiveConnection> hiveConnection = Optional.absent();

    @Override
    public void execute(HiveTask hiveTask) {

        // check the connection
        if(!hiveConnection.isPresent()){
            hiveConnection = Optional.of(HiveConnection.from(hiveTask.getConnectionProperties()));
        }
        // make the connection to Hive
        try {
            if(!(hiveConnection.get().isOpen()))
                hiveConnection.get().connect();
        } catch (ConnectionException e) {
            // TODO: handle connection failure
        }

        // at this point we should have a hive connection, and an underlying JDBC
        // javax.sql hiveConnection
        Optional<Connection> connection = this.hiveConnection.get().getConnection();

        // Do something



    }

    @Override
    public void closeConnection() {
        if(hiveConnection.isPresent()){
            try {
                hiveConnection.get().disconnect();

            } catch (ConnectionException e) {
                e.printStackTrace();
            }
        }
    }
}
