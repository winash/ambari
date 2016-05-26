package org.apache.ambari.view.hive2.actor.message;

import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.HiveConnectionWrapper;

/**
 * Connect message to be sent to the Connection Actor with the connection parameters
 */
public class Connect {

  private final String username;
  private final String password;
  private final String jdbcUrl;


  public Connect(String username, String password, String jdbcUrl) {
    this.username = username;
    this.password = password;
    this.jdbcUrl = jdbcUrl;
  }

  public Connectable getConnectable(){
    return new HiveConnectionWrapper(getJdbcUrl(),username,password);
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

}
