package org.apache.ambari.view.hive2;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ConnectionDelegate {
  Optional<ResultSet> execute(HiveConnection connection, ExecuteJob job) throws SQLException;
  Optional<ResultSet> getCurrentResultSet();
  Optional<HiveStatement> getCurrentStatement();
  boolean closeResultSet() throws SQLException;
  boolean closeStatement() throws SQLException;
}
