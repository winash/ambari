package org.apache.ambari.view.hive2.actor.message;

import com.google.common.base.Optional;
import org.apache.hive.jdbc.HiveStatement;

public class ExecuteQuery {

    private final Optional<HiveStatement> statement;

    public ExecuteQuery(Optional<HiveStatement> statement) {
        this.statement = statement;
    }


    public Optional<HiveStatement> getStatement() {
        return statement;
    }

    @Override
    public String toString() {
        return "ExecuteQuery{" +
                "statement=" + statement +
                '}';
    }
}
