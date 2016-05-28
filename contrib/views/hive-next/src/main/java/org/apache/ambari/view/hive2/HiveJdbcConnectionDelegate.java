package org.apache.ambari.view.hive2;

import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.actor.message.DDLJob;
import org.apache.ambari.view.hive2.actor.message.GetColumnMetadataJob;
import org.apache.ambari.view.hive2.actor.message.HiveJob;
import org.apache.ambari.view.hive2.internal.HiveResult;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcConnectionDelegate implements ConnectionDelegate {

  private ResultSet currentResultSet;
  private HiveStatement currentStatement;
  private String atsGuid;

  @Override
  public Optional<ResultSet> execute(HiveConnection connection, DDLJob job) throws SQLException {

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
      atsGuid = hiveStatement.getYarnATSGuid();
      if (result) {
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
  public Optional<ResultSet> executeSync(HiveConnection connection, DDLJob job) throws SQLException {
    try {
      Statement statement = connection.createStatement();
      currentStatement = (HiveStatement) statement;

      boolean hasResultSet = false;
      for (String syncStatement : job.getStatements()) {
        // we don't care about the result
        // fail all if one fails
        hasResultSet = statement.execute(syncStatement);
      }

      if (hasResultSet) {
        ResultSet resultSet = statement.getResultSet();
        //HiveResult result = new HiveResult(resultSet);
        return Optional.of(resultSet);
      } else {
        return Optional.absent();
      }
    } catch (SQLException e) {
      // Close the statement on any error
      currentStatement.close();
      throw e;
    }
  }


  @Override
  public Optional<ResultSet> getColumnMetadata(HiveConnection connection, GetColumnMetadataJob job) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns("", job.getSchemaPattern(), job.getTablePattern(), job.getColumnPattern());
    currentResultSet = resultSet;
    return Optional.of(resultSet);
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
  public void closeResultSet() {

    try {
      if (currentResultSet != null) {
        currentResultSet.close();
      }
    } catch (SQLException e) {
      // Cannot do anything here
    }
  }

  @Override
  public void closeStatement()  {
    try {
      if (currentStatement != null) {
        currentStatement.close();
      }
    } catch (SQLException e) {
      // cannot do anything here
    }
  }


}
