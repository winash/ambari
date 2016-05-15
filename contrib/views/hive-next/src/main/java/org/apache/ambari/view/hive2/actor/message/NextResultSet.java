package org.apache.ambari.view.hive2.actor.message;

import java.sql.ResultSet;

public class NextResultSet {

    private ResultSet resultSet;

    public NextResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }


    public ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public String toString() {
        return "NextResultSet{" +
                "resultSet=" + resultSet +
                '}';
    }
}
