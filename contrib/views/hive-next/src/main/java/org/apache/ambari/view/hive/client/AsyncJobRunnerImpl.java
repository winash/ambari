package org.apache.ambari.view.hive.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.actor.GetResultHolder;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.ExecuteQuery;
import org.apache.ambari.view.hive2.actor.message.JobSubmitted;
import org.apache.ambari.view.hive2.actor.message.ResultReady;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.internal.AsyncExecutionFailure;
import org.apache.ambari.view.hive2.internal.AsyncExecutionSuccess;
import org.apache.ambari.view.hive2.internal.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class AsyncJobRunnerImpl implements AsyncJobRunner {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final ActorRef controller;
    private final ActorSystem system;
    private ViewContext viewContext;

    public AsyncJobRunnerImpl(ActorRef controller, ActorSystem system, ViewContext viewContext) {
        this.controller = controller;
        this.system = system;
        this.viewContext = viewContext;
    }


    @Override
    public Either<JobSubmitted, AsyncExecutionFailed> submitJob(ConnectionConfig config,AsyncJob job) {
        Connect connect = config.createConnectMessage();
        Inbox inbox = Inbox.create(system);
        ExecuteJob executeJob = new ExecuteJob(connect, job);
        inbox.send(controller, executeJob);

        try{
            Object submitted = inbox.receive(Duration.create(1, TimeUnit.MINUTES));
            if(submitted instanceof ResultReady){
                ResultReady resultReady = (ResultReady) submitted;
                Either<ActorRef, ActorRef> result = resultReady.getResult();
                if(result.isRight()){
                    // Query with no result sets to be returned
                    // execute right away
                    inbox.send(result.getRight(),new ExecuteQuery());
                    Object receive = inbox.receive(Duration.create(5, TimeUnit.MINUTES));
                    if(receive instanceof ExecutionFailed){
                        ExecutionFailed executionFailed = (ExecutionFailed) receive;
                        return Either.right(new AsyncExecutionFailed(job.getJobId(),executionFailed.getMessage(),executionFailed.getError()));
                    }
                    if(receive instanceof AsyncExecutionSuccess){
                        // Query execution was successfull
                        // make a left
                    }

                }
                if(result.isLeft()){
                    // There is a result set to be processed


                }
            }



        } catch (Throwable e){
            return Either.right(new AsyncExecutionFailed(job.getJobId(),"We could not sumbit the hive job to the pool",e));
        }


    }


}
