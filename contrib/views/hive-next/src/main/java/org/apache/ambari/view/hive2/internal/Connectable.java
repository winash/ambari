package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Optional;

import java.sql.Connection;

/**
 * Life cycle management for java.sql.Connection
 */
public interface Connectable  {

    /**
     * Set connection properties
     * @param properties
     */
    void setProperties(ConnectionProperties properties);
    /**
     * Get the underlying connection
     * @return an optional wrapping the connection
     */
    Optional<Connection> getConnection();

    /**
     * Check if the connection is open
     * @return
     */
    boolean isOpen();

    /**
     * Open a connection
     * @throws ConnectionException
     */
    void connect() throws ConnectionException;

    /**
     * Reconnect if closed
     * @throws ConnectionException
     */
    void reconnect() throws ConnectionException;

    /**
     * Close the connection
     * @throws ConnectionException
     */
    void disconnect() throws ConnectionException;

}
