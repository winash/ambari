package org.apache.ambari.view.hive2.actor.message;

import com.google.common.base.Optional;

import java.sql.Statement;

public class AssignStatement {

    private Statement resultSet;

    public AssignStatement(Statement statement) {
        this.resultSet = statement;
    }


    public Statement getStatement() {
        return resultSet;
    }

    @Override
    public String toString() {
        return "AssignStatement{" +
                "resultSet=" + resultSet +
                '}';
    }


}
