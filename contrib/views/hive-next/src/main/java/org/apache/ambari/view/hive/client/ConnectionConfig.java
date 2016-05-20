package org.apache.ambari.view.hive.client;

import com.beust.jcommander.internal.Maps;
import org.apache.ambari.view.hive2.actor.message.Connect;

import java.util.Map;

/**
 * Created by dbhowmick on 5/20/16.
 */
public class ConnectionConfig {
  private String username;
  private String password;
  private String host;
  private int port;
  private Map<String,String> authParams;

  private ConnectionConfig(String username, String password, String host, int port, Map<String, String> authParams) {
    this.username = username;
    this.password = password;
    this.host = host;
    this.port = port;
    this.authParams = authParams;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public Map<String, String> getAuthParams() {
    return authParams;
  }

  public Connect createConnectMessage() {
    return new Connect(username, password, host, port, authParams);
  }

  public static class ConnectionConfigBuilder {
    private String username = "";
    private String password = "";
    private String host = "127.0.0.1";
    private int port = 10000;
    private Map<String,String> authParams = Maps.newHashMap();

    public ConnectionConfigBuilder withUsername(String username) {
      this.username = username;
      return this;
    }

    public ConnectionConfigBuilder withPassword(String password) {
      this.password = password;
      return this;
    }

    public ConnectionConfigBuilder withHost(String host) {
      this.host = host;
      return this;
    }

    public ConnectionConfigBuilder withPort(int port) {
      this.port = port;
      return this;
    }

    public ConnectionConfigBuilder withAuthParams(Map<String, String> params) {
      this.authParams.putAll(params);
      return this;
    }

    public ConnectionConfigBuilder addAuthParams(String key, String value) {
      this.authParams.put(key, value);
      return this;
    }

    public ConnectionConfig build() {
      return new ConnectionConfig(username, password, host, port, authParams);
    }
  }
}
