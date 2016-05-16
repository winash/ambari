package org.apache.ambari.view.hive2;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.google.common.collect.Maps;
import com.google.inject.Singleton;
import org.apache.ambari.view.hive2.actor.OperationController;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.Job;

import java.util.HashMap;

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




    public static void main(String[] args) throws InterruptedException {
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

        controller.tell(job,ActorRef.noSender());

        while (true){
            ;
        }

    }

}
