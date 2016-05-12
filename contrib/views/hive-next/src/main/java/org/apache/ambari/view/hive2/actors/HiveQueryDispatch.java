package org.apache.ambari.view.hive2.actors;

import org.apache.ambari.view.hive2.internal.HiveResult;
import org.apache.ambari.view.hive2.internal.HiveTask;

import java.sql.Connection;
import java.util.concurrent.TimeoutException;

public interface HiveQueryDispatch {

    /**
     * Blocking call to make the actual hive call and block for the reply over a connection
     *
     * @param sqlConnection
     * @param hiveTask
     * @return
     */
    HiveResult executeQuery(Connection sqlConnection, HiveTask hiveTask) throws TimeoutException;

}
