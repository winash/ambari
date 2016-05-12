package org.apache.ambari.view.hive2.actors;

import org.apache.ambari.view.hive2.internal.HiveResult;
import org.apache.ambari.view.hive2.internal.HiveTask;

import java.sql.Connection;

public class HiveQueryDispatchImpl implements HiveQueryDispatch {

    @Override
    public HiveResult executeQuery(Connection sqlConnection, HiveTask hiveTask) {
        return null;
    }
}
