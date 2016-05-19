package org.apache.ambari.view.hive2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.ambari.view.hive2.actor.OperationController;
import org.apache.ambari.view.hive2.actor.ResultSetIterator;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.HiveJob;
import org.apache.ambari.view.hive2.actor.message.SyncJob;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.job.FetchFailed;
import org.apache.ambari.view.hive2.actor.message.job.Next;
import org.apache.ambari.view.hive2.actor.message.job.NoMoreItems;
import org.apache.ambari.view.hive2.actor.message.job.NoResult;
import org.apache.ambari.view.hive2.actor.message.job.Result;
import org.apache.ambari.view.hive2.actor.message.job.ResultSetHolder;
import org.apache.ambari.view.hive2.internal.DataStorageSupplier;
import org.apache.ambari.view.hive2.internal.DefaultSupplier;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by dbhowmick on 5/18/16.
 */
public class HiveActorSystem {
  public static final String HIVE_VIEW_SYSTEM = "HiveViewSystem";
  private final ActorSystem system = ActorSystem.create(HIVE_VIEW_SYSTEM);

  public static void main(String[] args) throws Exception {
    HiveActorSystem hiveActorSystem = new HiveActorSystem();
    ActorRef controller = hiveActorSystem.system.actorOf(Props.create(OperationController.class, hiveActorSystem.system,
      new DefaultSupplier<HiveJdbcConnectionDelegate>(HiveJdbcConnectionDelegate.class),
      new DataStorageSupplier(null),
      new Supplier<Optional<HdfsApi>>() {
        @Override
        public Optional<HdfsApi> get() {
          return Optional.absent();
        }
      }),
      "controller");

    Connect connect = new Connect("admin", "", "c6402.ambari.apache.org", 10000, Maps.<String, String>newHashMap());
    HiveJob job = new SyncJob("admin", new String[] {"use default", "show tables"}, null);
//    HiveJob job = new SyncJob("admin", new String[] {"use default"});

    ExecuteJob executeJob = new ExecuteJob(connect, job);

    Inbox inbox = Inbox.create(hiveActorSystem.system);

    inbox.send(controller, executeJob);
    try {

      Object jdbcResult = inbox.receive(Duration.create(1, TimeUnit.MINUTES));

      if (jdbcResult instanceof NoResult) {
        System.out.println("Executed with no result!!!");
      } else if (jdbcResult instanceof ExecutionFailed) {

        ExecutionFailed error = (ExecutionFailed) jdbcResult;
        System.out.println(error.getMessage());
        error.getError().printStackTrace();

      } else if (jdbcResult instanceof ResultSetHolder){
        ResultSetHolder holder = (ResultSetHolder) jdbcResult;
        ActorRef iterator = holder.getIterator();
        while(true) {
          System.out.println("Fetching next results >>>");

          inbox.send(iterator, new Next());
          Object receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));

          if(receive instanceof Result) {
            Result result = (Result) receive;
            List<ResultSetIterator.Row> rows = result.getRows();
            System.out.println("Fetched " + rows.size() + " entries.");
            for(ResultSetIterator.Row row : rows) {
              System.out.println(row);
            }
          }

          if(receive instanceof NoMoreItems) {
            System.out.println("Finished fetching all rows. Exiting...");
            break;
          }

          if(receive instanceof FetchFailed) {
            FetchFailed message = (FetchFailed) receive;
            Throwable exception = message.getError();
            String str = message.getMessage();
            System.out.println(str);
            exception.printStackTrace();
            break;
          }
        }
      }

    } catch(Throwable ex) {
      System.out.println("Coming here!!!");
      ex.printStackTrace();
    }
  }
}
