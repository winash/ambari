package org.apache.ambari.view.hive2;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcConnectionDelegate implements ConnectionDelegate {

  private ResultSet currentResultSet;
  private HiveStatement currentStatement;

  @Override
  public Optional<ResultSet> execute(HiveConnection connection, ExecuteJob job) throws SQLException {

    try {
      Statement statement = connection.createStatement();
      currentStatement = (HiveStatement) statement;

      for (String syncStatement : job.getSyncStatements()) {
        // we don't care about the result
        // fail all if one fails
        statement.execute(syncStatement);
      }

      HiveStatement hiveStatement = (HiveStatement) statement;
      boolean result = hiveStatement.executeAsync(job.getAsyncStatement());
      if(result){
        // query has a result set
        ResultSet resultSet = hiveStatement.getResultSet();
        currentResultSet = resultSet;
        Optional<ResultSet> resultSetOptional = Optional.of(resultSet);
        return resultSetOptional;

      }
      return Optional.absent();

    } catch (SQLException e) {
      // Close the statement on any error
      currentStatement.close();
      throw e;

    }
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
