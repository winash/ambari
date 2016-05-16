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
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
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
        Connect connect = new Connect("admin", "admin", "c6401.ambari.apache.org", 2181, objectObjectHashMap);
        Job job = new Job(connect, new ExecuteJob("10","admin", new String[]{"SELECT * FROM employee"}, logFile));

        Job job2 = new Job(connect, new ExecuteJob("100","admin",
                new String[]{"insert into employee values(100,'terri',100000,'Delhi')"}));

        Job job3 = new Job(connect, new ExecuteJob("11","admin",
                new String[]{"insert into employee values(100,'asdasd',100000,'Delhi')"}));
        Job job31 = new Job(connect, new ExecuteJob("12","admin",
                new String[]{"insert into employee values(100,'adasdasdasd',100000,'Delhi')"}));
        Job job4 = new Job(connect, new ExecuteJob("13","admin",
                new String[]{"insert into employee values(100,'asdasdasdsd',100000,'Delhi')"}));
        Job job5 = new Job(connect, new ExecuteJob("14","admin",
                new String[]{"insert into employee values(100,'terrrfdfsfdi',100000,'Delhi')"}));
        Job job6 = new Job(connect, new ExecuteJob("15","admin",
                new String[]{"insert into employee values(100,'sdfsdfsdff',100000,'Delhi')"}));

//
//        controller.tell(job2,ActorRef.noSender());
//        controller.tell(job3,ActorRef.noSender());
//        controller.tell(job31,ActorRef.noSender());
//        controller.tell(job4,ActorRef.noSender());
//        controller.tell(job5,ActorRef.noSender());
//        controller.tell(job6,ActorRef.noSender());

        controller.tell(job,ActorRef.noSender());

        System.out.println("Sleeping");
        Thread.sleep(20000);
        System.out.println("Done");

        Future<Object> ask = Patterns.ask(controller, new GetResultHolder(), 2000);

        Either<ActorRef,ExecutionResult> result = (Either<ActorRef,ExecutionResult>)Await.result(ask, Duration.create(1000, TimeUnit.MILLISECONDS));
        result.getLeft().tell(new GetMoreResults(),ActorRef.noSender());

//        controller.tell(job2,ActorRef.noSender());
//        controller.tell(job3,ActorRef.noSender());
//        controller.tell(job4,ActorRef.noSender());
//        controller.tell(job5,ActorRef.noSender());
//        controller.tell(job6,ActorRef.noSender());

        while (true){
            ;
        }

    }

}
