package org.apache.ambari.view.hive.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.utils.ServiceFormattedException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by dbhowmick on 5/20/16.
 */
public class DDLDelegatorImpl implements DDLDelegator {

  public static final String NO_VALUE_MARKER = "NO_VALUE";
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private final ActorRef controller;
  private final ActorSystem system;

  private ViewContext context;

  public DDLDelegatorImpl(ViewContext context, ActorSystem system, ActorRef controller) {
    this.context = context;
    this.system = system;
    this.controller = controller;
  }

  @Override
  public List<String> getDbList(ConnectionConfig config, String like) {
    List<ResultSetIterator.Row> rows = getRowsFromDB(config, new String[] {
      String.format("show databases like '%s'", like)
    });
    return getFirstColumnValues(rows);
  }

  private ImmutableList<String> getFirstColumnValues(List<ResultSetIterator.Row> rows) {
    return FluentIterable.from(rows)
      .transform(new Function<ResultSetIterator.Row, String>() {
        @Override
        public String apply(ResultSetIterator.Row input) {
          String[] values = input.getValues();
          return values.length > 0 ? values[0] : NO_VALUE_MARKER;
        }
      }).toList();
  }

  @Override
  public List<String> getTableList(ConnectionConfig config, String database, String like) {
    List<ResultSetIterator.Row> rows = getRowsFromDB(config, new String[] {
      String.format("use %s", database),
      String.format("show tables like '%s'", like)
    });
    return getFirstColumnValues(rows);
  }

  @Override
  public List<ColumnDescription> getTableDescription(ConnectionConfig config, String database, String table, String like, boolean extended) {
    return null;
  }

  private List<ResultSetIterator.Row> getRowsFromDB(ConnectionConfig config, String[] statements) {
    List<ResultSetIterator.Row> rows = Lists.newArrayList();
    Connect connect = config.createConnectMessage();
    HiveJob job = new SyncJob(config.getUsername(), statements, context);
    ExecuteJob execute = new ExecuteJob(connect, job);

    Inbox inbox = Inbox.create(system);
    inbox.send(controller, execute);
    try {
      Object submitResult = inbox.receive(Duration.create(2, TimeUnit.MINUTES));
      if (submitResult instanceof NoResult) {
        LOG.info("Query returned with no result. Query: '" + getJoinedStatements(statements) + "'");
        return rows;

      } else if (submitResult instanceof ExecutionFailed) {
        ExecutionFailed error = (ExecutionFailed) submitResult;
        LOG.error("Failed to execute statements.{}. user: {}, Query: {}. Exception: {}",
          error.getMessage(), config.getUsername(), getJoinedStatements(statements), error.getError());
        throw new ServiceFormattedException(error.getMessage(), error.getError());

      } else if (submitResult instanceof ResultSetHolder){
        ResultSetHolder holder = (ResultSetHolder) submitResult;
        ActorRef iterator = holder.getIterator();
        while(true) {
          inbox.send(iterator, new Next());
          Object receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));

          if(receive instanceof Result) {
            Result result = (Result) receive;
            rows.addAll(result.getRows());
          }

          if(receive instanceof NoMoreItems) {
            break;
          }

          if(receive instanceof FetchFailed) {
            FetchFailed error = (FetchFailed) receive;
            LOG.error("Failed to fetch results for statements.{}. user: {}, Query: {}. Exception: {}",
              error.getMessage(), config.getUsername(), getJoinedStatements(statements), error.getError());
            throw new ServiceFormattedException(error.getMessage(), error.getError());
          }
        }

      }
    } catch(Throwable ex) {
      String stmts = getJoinedStatements(statements);
      String errorMessage = "Query timed out for user: " + config.getUsername() + ". Query: '" + stmts + "'";
      LOG.error(errorMessage, ex);
      throw new ServiceFormattedException(errorMessage, ex);

    }
    return rows;
  }

  private String getJoinedStatements(String[] statements) {
    return Joiner.on("; ").skipNulls().join(statements);
  }

}
