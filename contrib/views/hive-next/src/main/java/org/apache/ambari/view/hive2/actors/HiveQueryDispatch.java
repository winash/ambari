package org.apache.ambari.view.hive2.actors;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.internal.HiveResult;
import org.apache.ambari.view.hive2.internal.HiveTask;
import org.apache.hive.jdbc.HiveConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.TimeoutException;

public interface HiveQueryDispatch {

    /**
     * Blocking call to make the actual hive call and block for the reply over a connection
     *
     * @param connection
     * @param hiveTask
     * @return
     */
    Optional<ResultSet> executeQuery(HiveConnection connection, HiveTask hiveTask);

}
