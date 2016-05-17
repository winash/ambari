package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.actor.message.AssignResultSet;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.ExecuteQuery;
import org.apache.ambari.view.hive2.actor.message.GetMoreResults;
import org.apache.ambari.view.hive2.actor.message.ResultReady;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.hive2.internal.ExecutionResult;
import org.apache.ambari.view.hive2.internal.HiveResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extract and start buffering the result set information to the
 */
public class ResultHolder extends UntypedActor {

    private final ActorRef parent;
    private final ActorSystem system;
    private final ViewContext viewContext;

    /**
     * The top level parent
     */
    private final ActorRef operationController;
    /**
     * Job for which this holder is assigned
     */
    private final ExecuteJob executeJob;
    private ResultSet resultSet;

    public ResultHolder(ViewContext viewContext, ActorSystem system, ActorRef actorRef, ActorRef operationController,ExecuteJob executeJob) {
        this.parent = actorRef;

        this.system = system;
        this.viewContext = viewContext;
        this.operationController = operationController;
        this.executeJob = executeJob;
    }

    @Override
    public void onReceive(Object message) {
        if (message instanceof AssignResultSet) {
            assignResultSet(message);
        }

        if (message instanceof ExecuteQuery) {
            try {
                executeQuery(message);

            } catch (SQLException e) {
                //TODO:Send a failed message
                // Write the results to DB and send a reply holding that info
                //to the caller
            }
        }

        if(message instanceof GetMoreResults){
            try {
                HiveResult hiveResult = pullResultSet();
                System.out.println(hiveResult);
                sender().tell(hiveResult,self());
            } catch (SQLException e) {
                //TODO:Send a failed message

            }
        }

    }

    private void executeQuery(Object message) throws SQLException {
        ExecuteQuery executeQuery = (ExecuteQuery) message;
        Statement statement = executeQuery.getStatement().get();
        try {
            System.out.println("Before get UpdateCount"+ self());
            statement.getUpdateCount();
            System.out.println("After get UpdateCount"+ self());
            operationController.tell(new ResultReady(executeJob.getJobId(),
                    executeJob.getUsername(),
                    Either.<ActorRef, ExecutionResult>right(new ExecutionResult())),self());
        } catch (SQLException e) {
            throw e;
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
     *
     * To get results from this ref send a GetMoreResultsMessage
     * @see GetMoreResults
     *
     *
     * @param extractMessage
     */
    private void assignResultSet(Object extractMessage) {
        AssignResultSet message = (AssignResultSet) extractMessage;
        this.resultSet = message.getResultSet();
        operationController.tell(new ResultReady(executeJob.getJobId(),
                executeJob.getUsername(),
                Either.<ActorRef, ExecutionResult>left(self())),self());

    }

}
