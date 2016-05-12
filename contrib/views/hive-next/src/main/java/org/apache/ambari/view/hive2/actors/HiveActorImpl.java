package org.apache.ambari.view.hive2.actors;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.ConnectionProperties;
import org.apache.ambari.view.hive2.internal.HiveTask;

import java.sql.Connection;

public class HiveActorImpl implements HiveActor {

    private Optional<Connectable> connection = Optional.absent();

    @Override
    public void execute(HiveTask hiveTask) {

        // check the connection
        if (!connection.isPresent()) {
            ConnectionProperties connectionProperties = hiveTask.getConnectionProperties();
            Connectable connectable = hiveTask.getConnectionClass();
            connectable.setProperties(connectionProperties);
            connection = Optional.of(connectable);
        }
        // make the connection to Hive
        try {
            if (!(connection.get().isOpen()))
                connection.get().connect();
        } catch (ConnectionException e) {
            // TODO: handle connection failure
        }

        // at this point we should have a hive connection, and an underlying JDBC
        // javax.sql hiveConnection
        Optional<Connection> connection = this.connection.get().getConnection();
        //
        Connection sqlConnection = connection.get();



    }

    @Override
    public void closeConnection() {
        if (connection.isPresent()) {
            try {
                connection.get().disconnect();

            } catch (ConnectionException e) {
                e.printStackTrace();
            }
        }
    }
}
