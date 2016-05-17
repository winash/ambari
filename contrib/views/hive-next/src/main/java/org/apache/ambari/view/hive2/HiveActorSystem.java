package org.apache.ambari.view.hive2;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import com.google.common.collect.Maps;
import com.google.inject.Singleton;
import org.apache.ambari.view.hive2.actor.GetResultHolder;
import org.apache.ambari.view.hive2.actor.OperationController;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.ExecuteAsyncJob;
import org.apache.ambari.view.hive2.actor.message.GetMoreResults;
import org.apache.ambari.view.hive2.actor.message.Job;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.hive2.internal.ExecutionResult;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Singleton
/**
 * Holder class for the actor system
 * All system management methods should go here
 */
public class HiveActorSystem {

    public static final String HIVE_VIEW_SYSTEM = "HiveViewSystem";

    private final ActorSystem system = ActorSystem.create(HIVE_VIEW_SYSTEM);




    public ActorSystem getSystem() {
        return system;

    }




    public static void main(String[] args) throws Exception {
        HiveActorSystem hiveActorSystem = new HiveActorSystem();
        ActorRef controller = hiveActorSystem.system.actorOf(Props.create(OperationController.class, null, hiveActorSystem.system),
                "controller");
        Thread.sleep(5000);

        String logFile = "";
        HashMap<String, String> objectObjectHashMap = Maps.newHashMap();
        objectObjectHashMap.put("serviceDiscoveryMode","zooKeeper");
        objectObjectHashMap.put("zooKeeperNamespace","hiveserver2");
        Connect connect = new Connect("10","admin", "admin", "c6401.ambari.apache.org", 2181, objectObjectHashMap);
        Job job = new Job(connect, new ExecuteAsyncJob("10","admin", new String[]{"select * from trucks_stage"}, logFile));

        Job job2 = new Job(connect, new ExecuteAsyncJob("100","admin",
                new String[]{"SELECT * FROM employee"}, logFile));

        Job job3 = new Job(connect, new ExecuteAsyncJob("11","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
//        Job job31 = new Job(connect, new ExecuteJob("12","admin",
//                new String[]{"insert into employee values(100,'adasdasdasd',100000,'Delhi')"}));
        Job job4 = new Job(connect, new ExecuteAsyncJob("13","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
        Job job5 = new Job(connect, new ExecuteAsyncJob("14","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
        Job job6 = new Job(connect, new ExecuteAsyncJob("15","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
        Job job7 = new Job(connect, new ExecuteAsyncJob("16","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
        Job job8 = new Job(connect, new ExecuteAsyncJob("17","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
        Job job9 = new Job(connect, new ExecuteAsyncJob("18","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
        Job job10 = new Job(connect, new ExecuteAsyncJob("19","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
        Job job11 = new Job(connect, new ExecuteAsyncJob("20","admin",
                new String[]{"SELECT * FROM employee"}, logFile));
        Job job12 = new Job(connect, new ExecuteAsyncJob("21","admin",
                new String[]{"SELECT * FROM employee"}, logFile));

//        Job job5 = new Job(connect, new ExecuteJob("14","admin",
//                new String[]{"insert into employee values(100,'terrrfdfsfdi',100000,'Delhi')"}));
//        Job job6 = new Job(connect, new ExecuteJob("15","admin",
//                new String[]{"insert into employee values(100,'sdfsdfsdff',100000,'Delhi')"}));




        controller.tell(job,ActorRef.noSender());
//        Thread.sleep(50000);
//        controller.tell(job2,ActorRef.noSender());
//
//        controller.tell(job3,ActorRef.noSender());

//        controller.tell(job4,ActorRef.noSender());
//        controller.tell(job5,ActorRef.noSender());
//        controller.tell(job6,ActorRef.noSender());
//        controller.tell(job7,ActorRef.noSender());
//        controller.tell(job8,ActorRef.noSender());
//        controller.tell(job9,ActorRef.noSender());
//        controller.tell(job10,ActorRef.noSender());
//        controller.tell(job11,ActorRef.noSender());
//        controller.tell(job12,ActorRef.noSender());
//        Thread.sleep(10000);
//
//        controller.tell(job5,ActorRef.noSender());
//        Thread.sleep(10000);
//
//        controller.tell(job6,ActorRef.noSender());
//
//        controller.tell(job,ActorRef.noSender());
//
//        System.out.println("Sleeping");

//        System.out.println("Done");
//
        Thread.sleep(20000);
//        Future<Object> ask = Patterns.ask(controller, new GetResultHolder("10","admin"), 2000);
//        Future<Object> ask2 = Patterns.ask(controller, new GetResultHolder("100","admin"), 2000);
//        Future<Object> ask3 = Patterns.ask(controller, new GetResultHolder("11","admin"), 2000);
//        Future<Object> ask4 = Patterns.ask(controller, new GetResultHolder("13","admin"), 2000);
//        Future<Object> ask5 = Patterns.ask(controller, new GetResultHolder("14","admin"), 2000);
//        Future<Object> ask6 = Patterns.ask(controller, new GetResultHolder("15","admin"), 2000);
//        Future<Object> ask7 = Patterns.ask(controller, new GetResultHolder("16","admin"), 2000);
//        Future<Object> ask8 = Patterns.ask(controller, new GetResultHolder("17","admin"), 2000);
//        Future<Object> ask9 = Patterns.ask(controller, new GetResultHolder("18","admin"), 2000);
//        Future<Object> ask10 = Patterns.ask(controller, new GetResultHolder("19","admin"), 2000);
//        Future<Object> ask11 = Patterns.ask(controller, new GetResultHolder("20","admin"), 2000);
//        Future<Object> ask12 = Patterns.ask(controller, new GetResultHolder("21","admin"), 2000);
//        Either<ActorRef,ExecutionResult> result = (Either<ActorRef,ExecutionResult>)Await.result(ask, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result2 = (Either<ActorRef,ExecutionResult>)Await.result(ask2, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result3 = (Either<ActorRef,ExecutionResult>)Await.result(ask3, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result4 = (Either<ActorRef,ExecutionResult>)Await.result(ask4, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result5 = (Either<ActorRef,ExecutionResult>)Await.result(ask5, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result6 = (Either<ActorRef,ExecutionResult>)Await.result(ask6, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result7 = (Either<ActorRef,ExecutionResult>)Await.result(ask7, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result8 = (Either<ActorRef,ExecutionResult>)Await.result(ask8, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result9 = (Either<ActorRef,ExecutionResult>)Await.result(ask9, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result10 = (Either<ActorRef,ExecutionResult>)Await.result(ask10, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result11 = (Either<ActorRef,ExecutionResult>)Await.result(ask11, Duration.create(1000, TimeUnit.MILLISECONDS));
//        Either<ActorRef,ExecutionResult> result12 = (Either<ActorRef,ExecutionResult>)Await.result(ask12, Duration.create(1000, TimeUnit.MILLISECONDS));
//        System.out.println(result.getLeft());
//        result.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result3.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result4.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result5.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result6.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result7.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result8.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result9.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result10.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result11.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//        result12.getLeft().tell(new GetMoreResults(),ActorRef.noSender());
//
////        controller.tell(job2,ActorRef.noSender());
////        controller.tell(job3,ActorRef.noSender());
////        controller.tell(job4,ActorRef.noSender());
////        controller.tell(job5,ActorRef.noSender());
////        controller.tell(job6,ActorRef.noSender());

        while (true){
            ;
        }

    }

}
