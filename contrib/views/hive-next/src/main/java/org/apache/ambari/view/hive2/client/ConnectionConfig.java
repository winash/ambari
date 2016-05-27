package org.apache.ambari.view.hive2.client;

import org.apache.ambari.view.hive2.actor.message.Connect;

/**
 * Created by dbhowmick on 5/20/16.
 */
public class ConnectionConfig {
  private final String username;
  private final String password;
  private final String jdbcUrl;

  public ConnectionConfig(String username, String password, String jdbcUrl) {
    this.username = username;
    this.password = password;
    this.jdbcUrl = jdbcUrl;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public Connect createConnectMessage() {
    return new Connect(username, password, jdbcUrl);
  }


}
