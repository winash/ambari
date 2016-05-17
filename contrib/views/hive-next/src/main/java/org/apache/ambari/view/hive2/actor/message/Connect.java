package org.apache.ambari.view.hive2.actor.message;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Connect message to be sent to the Connection Actor with the connection parameters
 */
public class Connect {

  private final String username;
  private final String password;
  private final String host;
  private final int port;
  private final Map<String, String> authParams;
  private String jobId;

  public Connect(String jobId,String username, String password, String host, int port, Map<String, String> authParams) {
    this.jobId = jobId;
    this.username = username;
    this.password = password;
    this.host = host;
    this.port = port;
    this.authParams = Collections.unmodifiableMap(authParams);
  }

  public Connect(String jobId,String username, String password, String host, int port) {
    this(jobId,username, password, host, port, Maps.<String, String>newHashMap());
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

  public String getJdbcUrl() {
    StringBuilder builder = new StringBuilder();
    builder.append("jdbc:hive2://")
      .append(host)
      .append(":")
      .append(port).append("/");

    if (!(authParams == null || authParams.isEmpty())) {
      builder.append(";");
      builder.append(getAuthParamsString());
    }

    return builder.toString();
  }

  private String getAuthParamsString() {
    List<String> entries = FluentIterable.from(authParams.entrySet())
      .transform(new Function<Map.Entry<String,String>, String>() {
        @Override
        public String apply(Map.Entry<String, String> entry) {
          return entry.getKey() + "=" + entry.getValue();
        }
      }).toList();

    return Joiner.on(";").join(entries);
  }

  @Override
  public String toString() {
    return "Connect{" +
      "username='" + username + '\'' +
      ", password='" + password + '\'' +
      ", host='" + host + '\'' +
      ", port=" + port +
      ", authParams=" + authParams +
      '}';
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }
}
