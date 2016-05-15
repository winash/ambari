package org.apache.ambari.view.hive2.actor.message;

import java.sql.ResultSet;

public class ExtractResultSet {

    private ResultSet resultSet;

    public ExtractResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }


    public ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public String toString() {
        return "ExtractResultSet{" +
                "resultSet=" + resultSet +
                '}';
    }
}
