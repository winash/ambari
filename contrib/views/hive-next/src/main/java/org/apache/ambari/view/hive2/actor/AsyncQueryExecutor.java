package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import org.apache.ambari.view.hive2.actor.message.AssignStatement;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.ExecuteQuery;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.ResultReady;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.internal.AsyncExecutionSuccess;
import org.apache.ambari.view.hive2.internal.Either;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AsyncQueryExecutor extends HiveActor {


    private Statement statement;

    public AsyncQueryExecutor(Statement statement) {
        this.statement = statement;
    }

    @Override
    public void handleMessage(HiveMessage hiveMessage) {
        Object message = hiveMessage.getMessage();

        if (message instanceof ExecuteQuery) {
                executeQuery();
            }

    }

    private void executeQuery()  {
        try {
            statement.getUpdateCount();
            sender().tell(new AsyncExecutionSuccess(),self());
        } catch (SQLException e) {
            sender().tell(new ExecutionFailed("Cannot execute query",e),self());
        }


    }


}
