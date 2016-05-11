package org.apache.ambari.view.hive2.internal;

public class ConnectionException extends Exception {
    public ConnectionException(Exception e, String message) {
        super(message,e);
    }
}
