package org.apache.ambari.view.hive2.actors;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.internal.HiveTask;
import org.apache.hive.jdbc.HiveConnection;

import java.sql.ResultSet;

public class HiveQueryDispatchImpl implements HiveQueryDispatch {

  @Override
  public Optional<ResultSet> executeQuery(HiveConnection connection, HiveTask hiveTask) {
    return null;
  }
}
