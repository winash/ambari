package org.apache.ambari.view.hive2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.ambari.view.AmbariStreamProvider;
import org.apache.ambari.view.DataStore;
import org.apache.ambari.view.HttpImpersonator;
import org.apache.ambari.view.ImpersonatorSetting;
import org.apache.ambari.view.ResourceProvider;
import org.apache.ambari.view.SecurityException;
import org.apache.ambari.view.URLConnectionProvider;
import org.apache.ambari.view.URLStreamProvider;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.ViewController;
import org.apache.ambari.view.ViewDefinition;
import org.apache.ambari.view.ViewInstanceDefinition;
import org.apache.ambari.view.cluster.Cluster;
import org.apache.ambari.view.hive2.actor.OperationController;
import org.apache.ambari.view.hive2.actor.ResultSetIterator;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
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
import org.apache.ambari.view.hive2.internal.ConnectionSupplier;
import org.apache.ambari.view.hive2.internal.DataStorageSupplier;
import org.apache.ambari.view.hive2.internal.DefaultSupplier;
import org.apache.ambari.view.hive2.internal.HdfsApiSupplier;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import scala.concurrent.duration.Duration;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by dbhowmick on 5/18/16.
 */
public class HiveActorSystem {
  public static final String HIVE_VIEW_SYSTEM = "HiveViewSystem";
  private final ActorSystem system = ConnectionSystem.getInstance().getActorSystem();

  public static void main(String[] args) throws Exception {
    HiveActorSystem hiveActorSystem = new HiveActorSystem();
    ActorRef controller = hiveActorSystem.system.actorOf(Props.create(OperationController.class, hiveActorSystem.system,
      new ConnectionSupplier(),
      new DataStorageSupplier(),
      new HdfsApiSupplier() {
        @Override
        public Optional<HdfsApi> get(ViewContext context) {
          return Optional.absent();
        }
      }),
      "controller");

    Connect connect = new Connect("admin", "", "c6402.ambari.apache.org", 10000, Maps.<String, String>newHashMap());

    ViewContext context = getViewContext();
    executeSync(hiveActorSystem, controller, connect, context);
    executeASync(hiveActorSystem, controller, connect, context);
  }



  private static void executeASync(HiveActorSystem hiveActorSystem, ActorRef controller, Connect connect, ViewContext context) {
    System.out.println("\n\nExecuting async job...");
    HiveJob job = new AsyncJob("10", "admin", new String[] {"use default", "select * from  geolocation_stage limit 10"}, null, context);
    ExecuteJob executeJob = new ExecuteJob(connect, job);

    Inbox inbox = Inbox.create(hiveActorSystem.system);
    inbox.send(controller, executeJob);

    try {
      Object receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));
      System.out.println(receive);
    } catch (Throwable ex) {
      System.out.println("Timeout.....");
      ex.printStackTrace();
    }
  }

  private static void executeSync(HiveActorSystem hiveActorSystem, ActorRef controller, Connect connect, ViewContext context) {
    HiveJob job = new SyncJob("admin", new String[] {"use default"}, context);

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

  private static ViewContext getViewContext() {
    return new ViewContext() {
      @Override
      public String getUsername() {
        return null;
      }

      @Override
      public String getLoggedinUser() {
        return null;
      }

      @Override
      public void hasPermission(String userName, String permissionName) throws SecurityException {

      }

      @Override
      public String getViewName() {
        return "HIVE";
      }

      @Override
      public ViewDefinition getViewDefinition() {
        return null;
      }

      @Override
      public String getInstanceName() {
        return "TestInstance";
      }

      @Override
      public ViewInstanceDefinition getViewInstanceDefinition() {
        return null;
      }

      @Override
      public Map<String, String> getProperties() {
        return null;
      }

      @Override
      public void putInstanceData(String key, String value) {

      }

      @Override
      public String getInstanceData(String key) {
        return null;
      }

      @Override
      public Map<String, String> getInstanceData() {
        return null;
      }

      @Override
      public void removeInstanceData(String key) {

      }

      @Override
      public String getAmbariProperty(String key) {
        return null;
      }

      @Override
      public ResourceProvider<?> getResourceProvider(String type) {
        return null;
      }

      @Override
      public URLStreamProvider getURLStreamProvider() {
        return null;
      }

      @Override
      public URLConnectionProvider getURLConnectionProvider() {
        return null;
      }

      @Override
      public AmbariStreamProvider getAmbariStreamProvider() {
        return null;
      }

      @Override
      public DataStore getDataStore() {
        return null;
      }

      @Override
      public Collection<ViewDefinition> getViewDefinitions() {
        return null;
      }

      @Override
      public Collection<ViewInstanceDefinition> getViewInstanceDefinitions() {
        return null;
      }

      @Override
      public ViewController getController() {
        return null;
      }

      @Override
      public HttpImpersonator getHttpImpersonator() {
        return null;
      }

      @Override
      public ImpersonatorSetting getImpersonatorSetting() {
        return null;
      }

      @Override
      public Cluster getCluster() {
        return null;
      }
    };
  }
}
