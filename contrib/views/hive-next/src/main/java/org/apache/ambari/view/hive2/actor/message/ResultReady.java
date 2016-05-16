package org.apache.ambari.view.hive2.actor.message;

import akka.actor.ActorRef;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.hive2.internal.ExecutionResult;

/**
 *
 * Fetch the result for
 *
 */
public class ResultReady extends FetchResult {
    private Either<ActorRef, ExecutionResult> result;


    public ResultReady(String jobId, String username, Either<ActorRef, ExecutionResult> result) {
        super(jobId, username);
        this.result = result;
    }

    public Either<ActorRef, ExecutionResult> getResult() {
        return result;
    }

    public void setResult(Either<ActorRef, ExecutionResult> result) {
        this.result = result;
    }
}

