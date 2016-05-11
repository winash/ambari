package org.apache.ambari.view.hive2.actors;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.HiveConnectionProps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Composition over a Hive jdbc connection
 * This class only provides a connection over which
 * callers should run their own JDBC statements

 */
public class HiveConnection implements Connectable {

    private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private final HiveConnectionProps connectionProps;

    private Optional<Connection> connection = Optional.absent();

    public HiveConnection(HiveConnectionProps connectionProps) {
        this.connectionProps = connectionProps;
    }


    @Override
    public void connect() throws ConnectionException {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
           throw new ConnectionException(e,"Cannot load the hive JDBC driver");
        }

        try {
            Connection con = DriverManager.getConnection(connectionProps.asUrl());
            connection = Optional.of(con);

        } catch (SQLException e) {
            throw new ConnectionException(e,"Cannot open a hive connection with connect string "+connectionProps.asUrlWithoutCredentials());
        }


    }

    @Override
    public void reconnect() throws ConnectionException {

    }

    @Override
    public void disconnect() throws ConnectionException {
        if(connection.isPresent()){
            try {
                connection.get().close();
            } catch (SQLException e) {
                throw new ConnectionException(e,"Cannot close the hive connection with connect string "+ connectionProps.asUrlWithoutCredentials());
            }
        }
    }

    public Optional<Connection> getConnection() {
        return connection;
    }

    @Override
    public boolean isOpen() {
        try {
            return connection.isPresent() && !connection.get().isClosed();
        } catch (SQLException e) {
            // in case of an SQ error just return
            return false;
        }
    }

}
