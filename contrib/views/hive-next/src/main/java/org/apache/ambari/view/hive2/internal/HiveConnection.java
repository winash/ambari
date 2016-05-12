package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Composition over a Hive jdbc connection
 * This class only provides a connection over which
 * callers should run their own JDBC statements

 */
public class HiveConnection implements Connectable {

    private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private  ConnectionProperties connectionProps;

    private Optional<Connection> connection = Optional.absent();


    @Override
    public void connect() throws ConnectionException {
        Preconditions.checkNotNull(connectionProps,"Connection properties have not been set");
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

    /**
     * Set connection properties
     *
     * @param properties
     */
    @Override
    public void setProperties(ConnectionProperties properties) {
        this.connectionProps = properties;
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
