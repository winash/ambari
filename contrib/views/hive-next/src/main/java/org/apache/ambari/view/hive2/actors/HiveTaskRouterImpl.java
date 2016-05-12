package org.apache.ambari.view.hive2.actors;

import akka.actor.ActorSystem;
import akka.actor.TypedActor;
import akka.actor.TypedProps;
import akka.japi.Creator;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.ambari.view.hive2.internal.HiveTask;

import java.util.concurrent.ExecutionException;

public class HiveTaskRouterImpl implements HiveTaskRouter {

    private final ActorSystem actorSystem;

    public HiveTaskRouterImpl(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    private final LoadingCache<Task,HiveActor> availableActors = CacheBuilder.newBuilder().removalListener(getListener()).build(getLoader());

    private RemovalListener<Task, HiveActor> getListener() {
        return new RemovalListener<Task, HiveActor>() {
            @Override
            public void onRemoval(RemovalNotification<Task, HiveActor> notification) {
                   // Figure what to do when an actor is removed from the pool
            }
        };
    }

    private CacheLoader<Task, HiveActor> getLoader() {
        return new CacheLoader<Task, HiveActor>() {
            @Override
            public HiveActor load(Task key) throws Exception {
                return  TypedActor.get(actorSystem).typedActorOf(
                        new TypedProps<>(HiveActor.class,
                                new Creator<HiveActor>() {
                                    public HiveActor create() {
                                        return new HiveActorImpl(actorSystem);
                                    }
                                }),
                        key.taskAsString());
            }
        };
    }

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
            // figure out what to do if something goes wrong in Creating a message
            e.printStackTrace();
        }


    }
}
