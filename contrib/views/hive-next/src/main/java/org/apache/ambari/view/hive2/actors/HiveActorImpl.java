package org.apache.ambari.view.hive2.actors;

import akka.actor.ActorSystem;
import akka.actor.TypedActor;
import akka.actor.TypedProps;
import akka.japi.Creator;
import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.ConnectionProperties;
import org.apache.ambari.view.hive2.internal.HiveTask;

import java.sql.Connection;

public class HiveActorImpl implements HiveActor,TypedActor.PostStop {

    /**
     * The dispatcher takes care of running the query
     * and has semantics for timeouts and parsing result sets
     * The dispatcher lifecycle is tied to the lifecycle of
     * the Hive actor which composed it
     */
    private final HiveQueryDispatch dispatch;
    private Optional<Connectable> connection = Optional.absent();
    private Status status = Status.IDLE;

    public HiveActorImpl(ActorSystem actorSystem) {
        // build query dispatcher
        this.dispatch = TypedActor.get(actorSystem).typedActorOf(
                new TypedProps<>(HiveQueryDispatch.class,
                        new Creator<HiveQueryDispatch>() {
                            public HiveQueryDispatch create() {
                                return new HiveQueryDispatchImpl();
                            }
                        }));

    }


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
            status = Status.DISCONNECTED;
        }

        // at this point we should have a hive connection, and an underlying JDBC
        // javax.sql hiveConnection
        // update state
        status = Status.CONNECTED;

        Optional<Connection> connection = this.connection.get().getConnection();
        //
        Connection sqlConnection = connection.get();
        //dispatch.executeQuery(sqlConnection,hiveTask);




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

    @Override
    public void postStop() {
        closeConnection();
    }
}
