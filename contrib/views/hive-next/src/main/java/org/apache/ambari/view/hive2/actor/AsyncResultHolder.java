package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import org.apache.ambari.view.hive2.actor.message.AssignResultSet;
import org.apache.ambari.view.hive2.actor.message.AssignStatement;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.ExecuteQuery;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.ResultReady;
import org.apache.ambari.view.hive2.actor.message.job.FetchFailed;
import org.apache.ambari.view.hive2.actor.message.job.Next;
import org.apache.ambari.view.hive2.internal.AsyncExecutionSuccess;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.hive2.internal.AsyncExecutionFailure;
import org.apache.ambari.view.hive2.internal.HiveResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AsyncResultHolder extends HiveActor {


    /**
     * The top level parent
     */
    private final ActorRef operationController;
    /**
     * ExecuteJob for which this holder is assigned
     */
    private final AsyncJob executeJob;
    private ResultSet resultSet;
    private Statement statement;

    public AsyncResultHolder(ActorRef operationController, AsyncJob executeJob) {
        this.operationController = operationController;
        this.executeJob = executeJob;
    }

    @Override
    public void handleMessage(HiveMessage hiveMessage) {
        Object message = hiveMessage.getMessage();

        if (message instanceof AssignResultSet) {
            assignResultSet(message);
        }

        if(message instanceof AssignStatement){
            assignStatement((AssignStatement)message);
        }

        if (message instanceof ExecuteQuery) {
                executeQuery();
            }

        if (message instanceof Next) {
            parseResultSet();
        }

    }

    private void assignStatement(AssignStatement message) {
        statement = message.getStatement();
        operationController.tell(new ResultReady(executeJob.getJobId(),
                executeJob.getUsername(),
                Either.<ActorRef, AsyncExecutionFailure>left(self())), self());
    }

    private void parseResultSet() {
        try {
            HiveResult hiveResult = pullResultSet();
            sender().tell(hiveResult, self());
        } catch (SQLException e) {
            sender().tell(new FetchFailed("Could not read ay results", e), self());
        }
    }

    private void executeQuery()  {
        try {
            statement.getUpdateCount();
            sender().tell(new AsyncExecutionSuccess(),self());
        } catch (SQLException e) {
            sender().tell(new AsyncExecutionFailure(),self());
        }


    }

    private HiveResult pullResultSet() throws SQLException {
        ResultSet rs = this.resultSet;
        // make a blocking call to get the result set data
        // get the Hive result
        HiveResult hiveResult = new HiveResult(rs);
        return hiveResult;
    }


    /**
     * Assign the result set to this actor
     * Calling a next here would cause the operation to block
     * which would anyways happen when the ask to this ref is
     * executed
     * <p/>
     * To get results from this ref send a GetMoreResultsMessage
     *
     * @param extractMessage
     * @see Next
     */
    private void assignResultSet(Object extractMessage) {
        AssignResultSet message = (AssignResultSet) extractMessage;
        this.resultSet = message.getResultSet();
        operationController.tell(new ResultReady(executeJob.getJobId(),
                executeJob.getUsername(),
                Either.<ActorRef, AsyncExecutionFailure>left(self())), self());

    }

}
