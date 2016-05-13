package org.apache.ambari.view.hive2;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by dbhowmick on 5/13/16.
 */
public class HiveJdbcConnectionDelegate implements ConnectionDelegate {
  private HiveStatement currentStatement;
  private ResultSet currentResultSet;

  @Override
  public Optional<ResultSet> execute(HiveConnection connection, ExecuteJob job) {
    return null;
  }

  @Override
  public Optional<ResultSet> getCurrentResultSet() {
    return Optional.fromNullable(currentResultSet);
  }

  @Override
  public Optional<HiveStatement> getCurrentStatement() {
    return Optional.fromNullable(currentStatement);
  }

  @Override
  public boolean closeResultSet() throws SQLException {
    if (currentResultSet == null) {
      return false;
    }
    currentResultSet.close();
    return true;
  }

  @Override
  public boolean closeStatement() throws SQLException {
    if (currentStatement == null) {
      return false;
    }
    currentStatement.close();
    return true;
  }


}
