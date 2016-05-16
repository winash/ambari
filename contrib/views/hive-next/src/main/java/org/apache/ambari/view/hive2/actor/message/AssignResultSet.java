package org.apache.ambari.view.hive2.actor.message;

import com.google.common.base.Optional;

import java.sql.ResultSet;

public class AssignResultSet {

    private Optional<ResultSet> resultSet;

    public AssignResultSet(Optional<ResultSet> resultSet) {
        this.resultSet = resultSet;
    }


    public ResultSet getResultSet() {
        return resultSet.orNull();
    }

    @Override
    public String toString() {
        return "ExtractResultSet{" +
                "resultSet=" + resultSet +
                '}';
    }
}
