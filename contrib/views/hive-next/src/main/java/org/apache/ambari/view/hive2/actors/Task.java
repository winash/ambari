package org.apache.ambari.view.hive2.actors;

import org.apache.ambari.view.hive2.internal.HiveTask;

public class Task {


    private Long id;
    private String user;
    private String instance;

    private Task(Long id, String user, String instance) {
        this.id = id;
        this.user = user;
        this.instance = instance;
    }


    public Long getId() {
        return id;
    }

    public String getUser() {
        return user;
    }

    public String getInstance() {
        return instance;
    }

    public static Task from(HiveTask  hiveTask){
        return new Task(hiveTask.getId(),hiveTask.getUser(),hiveTask.getInstance());
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Task task = (Task) o;

        if (id != null ? !id.equals(task.id) : task.id != null) return false;
        if (user != null ? !user.equals(task.user) : task.user != null) return false;
        return instance != null ? instance.equals(task.instance) : task.instance == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (instance != null ? instance.hashCode() : 0);
        return result;
    }

    public String taskAsString() {
        return id + ":" + user + ":" + instance;
    }
}
