package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Optional;
import org.apache.hive.jdbc.HiveConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Composition over a Hive jdbc connection
 * This class only provides a connection over which
 * callers should run their own JDBC statements
 */
public class HiveConnectionWrapper implements Connectable {

  private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private final String jdbcUrl;
  private final String username;
  private final String password;

  private HiveConnection connection = null;

  public HiveConnectionWrapper(String jdbcUrl, String username, String password) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
  }


  @Override
  public void connect() throws ConnectionException {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      throw new ConnectionException(e, "Cannot load the hive JDBC driver");
    }

    try {
      Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
      connection = (HiveConnection)conn;

    } catch (SQLException e) {
      throw new ConnectionException(e, "Cannot open a hive connection with connect string " + jdbcUrl);
    }


  }

  @Override
  public void reconnect() throws ConnectionException {

  }

  @Override
  public void disconnect() throws ConnectionException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        throw new ConnectionException(e, "Cannot close the hive connection with connect string " + jdbcUrl);
      }
    }
  }

  public Optional<HiveConnection> getConnection() {
    return Optional.of(connection);
  }

  @Override
  public boolean isOpen() {
    try {
      return connection != null && !connection.isClosed();
    } catch (SQLException e) {
      // in case of an SQ error just return
      return false;
    }
  }

}
