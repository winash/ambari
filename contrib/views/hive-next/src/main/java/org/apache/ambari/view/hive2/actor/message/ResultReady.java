package org.apache.ambari.view.hive2.actor.message;

import akka.actor.ActorRef;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.internal.Either;

/**
 *
 * Fetch the result for
 *
 */
public class ResultReady extends FetchResult {
    private Either<ActorRef, ActorRef> result;


    public ResultReady(String jobId, String username, Either<ActorRef, ActorRef> result) {
        super(jobId, username);
        this.result = result;
    }

    public Either<ActorRef, ActorRef> getResult() {
        return result;
    }

    public void setResult(Either<ActorRef, ActorRef> result) {
        this.result = result;
    }
}

