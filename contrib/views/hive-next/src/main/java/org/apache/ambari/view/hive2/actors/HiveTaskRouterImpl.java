package org.apache.ambari.view.hive2.actors;

import akka.actor.ActorSystem;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.ambari.view.hive2.messages.HiveTask;

import java.util.concurrent.ExecutionException;

public class HiveTaskRouterImpl implements HiveTaskRouter {

    private final ActorSystem actorSystem;

    public HiveTaskRouterImpl(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    private final LoadingCache<Task,HiveActor> availableActors = CacheBuilder.newBuilder().build(new CacheLoader<Task, HiveActor>() {
        @Override
        public HiveActor load(Task key) throws Exception {
             // construct new actor
            return null;
        }
    });

    private final Cache<Task,HiveActor> actorsInUse = CacheBuilder.newBuilder().build();


    @Override
    public void execute(HiveTask hiveTask) {
        Task task = Task.from(hiveTask);
        // check if there was an actor for this task
        // if not create one
        try {
            HiveActor hiveActor = availableActors.get(task);
            //Handle the task

            hiveActor.execute(hiveTask);


        } catch (ExecutionException e) {
            // figure out what to do if something goes wrong in connection loading, maybe send out a message

            e.printStackTrace();
        }


    }
}
