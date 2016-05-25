package org.apache.ambari.view.hive2;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.actor.message.DDLJob;
import org.apache.ambari.view.hive2.actor.message.GetColumnMetadataJob;
import org.apache.ambari.view.hive2.actor.message.HiveJob;
import org.apache.ambari.view.hive2.internal.HiveResult;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ConnectionDelegate {
  Optional<ResultSet> execute(HiveConnection connection, DDLJob job) throws SQLException;
  Optional<ResultSet> executeSync(HiveConnection connection, DDLJob job) throws SQLException;
  Optional<ResultSet> getColumnMetadata(HiveConnection connection, GetColumnMetadataJob job) throws SQLException;
  Optional<ResultSet> getCurrentResultSet();
  Optional<HiveStatement> getCurrentStatement();
  boolean closeResultSet() throws SQLException;
  boolean closeStatement() throws SQLException;
}
