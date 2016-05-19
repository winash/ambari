package org.apache.ambari.view.hive2.actor.message;


import java.util.UUID;

/**
 * Message wrapper, Each message has a unique ID
 */
public class HiveMessage {

    private String id = UUID.randomUUID().toString();

    private Object message;

    public HiveMessage(Object message) {
        this.message = message;
    }


    public Object getMessage() {
        return message;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "HiveMessage{" +
                "message=" + message +
                ", id='" + id + '\'' +
                '}';
    }
}
