package org.apache.ambari.view.hive2.actor.message;

import akka.actor.ActorRef;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.hive2.internal.AsyncExecutionFailure;

/**
 *
 * Fetch the result for
 *
 */
public class ResultReady extends FetchResult {
    private Either<ActorRef, AsyncExecutionFailure> result;


    public ResultReady(String jobId, String username, Either<ActorRef, AsyncExecutionFailure> result) {
        super(jobId, username);
        this.result = result;
    }

    public Either<ActorRef, AsyncExecutionFailure> getResult() {
        return result;
    }

    public void setResult(Either<ActorRef, AsyncExecutionFailure> result) {
        this.result = result;
    }
}

