package org.apache.ambari.view.hive2.internal;

public interface HiveTask {

    /**
     * The task id for this task
     * @return task Id
     */
    Long getId();

    /**
     * The user for which this task was submitted
     * @return
     */
    String getUser();

    /**
     * The view instance tied to this task
     * @return
     */
    String getInstance();

    /**
     * Connection properties pulled from the view context and request
     * @return
     */
    HiveConnectionProps getCredentials();

    HiveQuery.HiveQueries getQueries();


}
