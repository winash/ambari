package org.apache.ambari.view.hive.client;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Class which gets the complete resultset on the first call to next() and
 * iterates over the returned result. It is a forward-only cursor
 */
public class PersistentCursor implements Iterator<Row>, Iterable<Row> {
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private List<Object[]> rows = Lists.newArrayList();
  private long offset = 0;
  private boolean resultFetched = false;
  private final ViewContext context;
  private final ActorSystem system;
  private final ActorRef controller;

  public PersistentCursor(ViewContext context, ActorSystem system, ActorRef controller) {
    this.context = context;
    this.system = system;
    this.controller = controller;
  }


  @Override
  public Iterator<Row> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return offset < rows.size() - 1;
  }

  @Override
  public Row next() {
    if(!resultFetched) {
      resultFetched = true;

    }
    return null;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Method not supported");
  }

  public long getOffset() {
    return offset;
  }

  private List<Row> getRowsFromDB(ConnectionConfig config, String[] statements) {
    List<Row> rows = Lists.newArrayList();
    Connect connect = config.createConnectMessage();
    HiveJob job = new SyncJob(config.getUsername(), statements, context);
    ExecuteJob execute = new ExecuteJob(connect, job);

    Inbox inbox = Inbox.create(system);
    inbox.send(controller, execute);
    Object submitResult;
    try {
      submitResult = inbox.receive(Duration.create(2, TimeUnit.MINUTES));
    } catch (Throwable ex) {
      String stmts = getJoinedStatements(statements);
      String errorMessage = "Query timed out for user: " + config.getUsername() + ". Query: '" + stmts + "'";
      LOG.error(errorMessage, ex);
      throw new ServiceFormattedException(errorMessage, ex);
    }
    if (submitResult instanceof NoResult) {
      LOG.info("Query returned with no result. Query: '" + getJoinedStatements(statements) + "'");
      return rows;

    } else if (submitResult instanceof ExecutionFailed) {
      ExecutionFailed error = (ExecutionFailed) submitResult;
      LOG.error("Failed to execute statements.{}. user: {}, Query: {}. Exception: {}",
        error.getMessage(), config.getUsername(), getJoinedStatements(statements), error.getError());
      throw new ServiceFormattedException(error.getMessage(), error.getError());

    } else if (submitResult instanceof ResultSetHolder) {
      ResultSetHolder holder = (ResultSetHolder) submitResult;
      ActorRef iterator = holder.getIterator();
      while (true) {
        inbox.send(iterator, new Next());
        Object receive;
        try {
          receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));
        } catch (Throwable ex) {
          String stmts = getJoinedStatements(statements);
          String errorMessage = "Query timed out to fetch results for user: " + config.getUsername() + ". Query: '" + stmts + "'";
          LOG.error(errorMessage, ex);
          throw new ServiceFormattedException(errorMessage, ex);
        }

        if (receive instanceof Result) {
          Result result = (Result) receive;
          rows.addAll(result.getRows());
        }

        if (receive instanceof NoMoreItems) {
          break;
        }

        if (receive instanceof FetchFailed) {
          FetchFailed error = (FetchFailed) receive;
          LOG.error("Failed to fetch results for statements.{}. user: {}, Query: {}. Exception: {}",
            error.getMessage(), config.getUsername(), getJoinedStatements(statements), error.getError());
          throw new ServiceFormattedException(error.getMessage(), error.getError());
        }
      }

    }

    return rows;
  }

  private String getJoinedStatements(String[] statements) {
    return Joiner.on("; ").skipNulls().join(statements);
  }
}
