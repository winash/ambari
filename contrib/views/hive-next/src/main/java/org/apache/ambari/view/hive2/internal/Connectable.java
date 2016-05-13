package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Optional;
import org.apache.hive.jdbc.HiveConnection;

/**
 * Life cycle management for java.sql.Connection
 */
public interface Connectable  {

    /**
     * Get the underlying connection
     * @return an optional wrapping the connection
     */
    Optional<HiveConnection> getConnection();

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
