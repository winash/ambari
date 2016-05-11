package org.apache.ambari.view.hive2.internal;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Holds all information needed to connect to HS2
 */
public class HiveConnectionProps {

    private String host;
    private int port;
    private String userName;
    private String password;
    private Map<String, String> authParams = Maps.newHashMap();

    public Map<String, String> getAuthParams() {
        return authParams;
    }

    public void addAuthParam(String key,String value){
        authParams.put(key, value);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        return "HiveConnectionProps{" +
                "authParams=" + authParams +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
