package org.apache.ambari.view.hive2.internal;

import org.apache.ambari.view.ViewContext;

public class HiveTaskMessage implements HiveTask {

    private Long id;
    private String instance;
    private HiveConnectionProps connectionProps;
    private HiveQuery.HiveQueries queries;


    public void setConnectionProps(HiveConnectionProps connectionProps) {
        this.connectionProps = connectionProps;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public void setQueries(HiveQuery.HiveQueries queries) {
        this.queries = queries;
    }

    /**
     * The task id for this task
     *
     * @return task Id
     */
    @Override
    public Long getId() {
        return id;
    }

    /**
     * The user for which this task was submitted
     *
     * @return
     */
    @Override
    public String getUser() {
        return connectionProps.getUserName();
    }

    /**
     * The view instance tied to this task
     *
     * @return
     */
    @Override
    public String getInstance() {
        return instance;
    }

    /**
     * Connection properties pulled from the view context and request
     *
     * @return
     */
    @Override
    public HiveConnectionProps getConnectionProperties() {
        return connectionProps;
    }

    @Override
    public HiveQuery.HiveQueries getQueries() {
        return queries;
    }


    @Override
    public String toString() {
        return "HiveTaskMessage{" +
                "connectionProps=" + connectionProps +
                ", id=" + id +
                ", instance='" + instance + '\'' +
                ", queries=" + queries +
                '}';
    }
}
