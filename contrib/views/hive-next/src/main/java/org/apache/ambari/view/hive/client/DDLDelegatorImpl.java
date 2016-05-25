package org.apache.ambari.view.hive.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.utils.ServiceFormattedException;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.GetColumnMetadataJob;
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

import java.util.ArrayList;
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
    Optional<Result> rowsFromDB = getRowsFromDB(config, getDatabaseListStatements(like));
    return rowsFromDB.isPresent() ? getFirstColumnValues(rowsFromDB.get().getRows()) : Lists.<String>newArrayList();
  }

  @Override
  public List<String> getTableList(ConnectionConfig config, String database, String like) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getTableListStatements(database, like));
    return rowsFromDB.isPresent() ? getFirstColumnValues(rowsFromDB.get().getRows()) : Lists.<String>newArrayList();
  }

  @Override
  public List<ColumnDescription> getTableDescription(ConnectionConfig config, String database, String table, String like, boolean extended) {
 Optional<Result> resultOptional = getTableDescription(config, database, table, like);
    List<ColumnDescription> descriptions = new ArrayList<>();
    if(resultOptional.isPresent()) {
      for (Row row : resultOptional.get().getRows()) {
        Object[] values = row.getRow();
        String name = (String) values[3];
        String type = (String) values[5];
        int position = (Integer) values[16];
        descriptions.add(new ColumnDescriptionShort(name, type, position));
      }
    }
    return descriptions;
  }

  @Override
  public Cursor<Row> getDbListCursor(ConnectionConfig config, String like) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getDatabaseListStatements(like));
    if (rowsFromDB.isPresent()) {
      Result result = rowsFromDB.get();
      return new PersistentCursor<>(result.getRows(), result.getColumns());
    } else {
      return new PersistentCursor<>(Lists.<Row>newArrayList(), Lists.<ColumnDescription>newArrayList());
    }
  }

  @Override
  public Cursor<Row> getTableListCursor(ConnectionConfig config, String database, String like) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getTableListStatements(database, like));
    if (rowsFromDB.isPresent()) {
      Result result = rowsFromDB.get();
      return new PersistentCursor<>(result.getRows(), result.getColumns());
    } else {
      return new PersistentCursor<>(Lists.<Row>newArrayList(), Lists.<ColumnDescription>newArrayList());
    }
  }

  @Override
  public Cursor<Row> getTableDescriptionCursor(ConnectionConfig config, String database, String table, String like, boolean extended) {
    Optional<Result> tableDescriptionOptional = getTableDescription(config, database, table, like);
    if(tableDescriptionOptional.isPresent()) {
      Result result = tableDescriptionOptional.get();
      return new PersistentCursor<>(result.getRows(), result.getColumns());
    } else {
      return new PersistentCursor<>(Lists.<Row>newArrayList(), Lists.<ColumnDescription>newArrayList());
    }
  }

  private String[] getDatabaseListStatements(String like) {
    return new String[]{
      String.format("show databases like '%s'", like)
    };
  }

  private String[] getTableListStatements(String database, String like) {
    return new String[]{
      String.format("use %s", database),
      String.format("show tables like '%s'", like)
    };
  }

  private Optional<Result> getRowsFromDB(ConnectionConfig config, String[] statements) {
    Connect connect = config.createConnectMessage();
    HiveJob job = new SyncJob(config.getUsername(), statements, context);
    ExecuteJob execute = new ExecuteJob(connect, job);

    LOG.info("Executing query: {}, for user: {}", getJoinedStatements(statements), job.getUsername());

    return getResultFromDB(execute);
  }

  private Optional<Result> getTableDescription(ConnectionConfig config, String databasePattern, String tablePattern, String columnPattern) {
    Connect connect = config.createConnectMessage();
    HiveJob job = new GetColumnMetadataJob(config.getUsername(), context, databasePattern, tablePattern, columnPattern);
    ExecuteJob execute = new ExecuteJob(connect, job);

    LOG.info("Executing query to fetch the column description for dbPattern: {}, tablePattern: {}, columnPattern: {}, for user: {}",
      databasePattern, tablePattern, columnPattern, job.getUsername());
    return getResultFromDB(execute);
  }

  private Optional<Result> getResultFromDB(ExecuteJob job) {
    List<ColumnDescription> descriptions = null;
    List<Row> rows = Lists.newArrayList();
    Inbox inbox = Inbox.create(system);
    inbox.send(controller, job);
    Object submitResult;
    try {
      submitResult = inbox.receive(Duration.create(2, TimeUnit.MINUTES));
    } catch (Throwable ex) {
      String errorMessage = "Query timed out to fetch table description for user: " + job.getConnect().getUsername();
      LOG.error(errorMessage, ex);
      throw new ServiceFormattedException(errorMessage, ex);
    }

    if (submitResult instanceof NoResult) {
      LOG.info("Query returned with no result.");
      return Optional.absent();

    }

    if (submitResult instanceof ExecutionFailed) {
      ExecutionFailed error = (ExecutionFailed) submitResult;
      LOG.error("Failed to get the table description");
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
          String errorMessage = "Query timed out to fetch results for user: " + job.getConnect().getUsername();
          LOG.error(errorMessage, ex);
          throw new ServiceFormattedException(errorMessage, ex);
        }

        if (receive instanceof Result) {
          Result result = (Result) receive;
          if (descriptions == null) {
            descriptions = result.getColumns();
          }
          result.getRows();
          rows.addAll(result.getRows());
        }

        if (receive instanceof NoMoreItems) {
          break;
        }

        if (receive instanceof FetchFailed) {
          FetchFailed error = (FetchFailed) receive;
          LOG.error("Failed to fetch results ");
          throw new ServiceFormattedException(error.getMessage(), error.getError());
        }
      }

    }
    return Optional.of(new Result(rows, descriptions));
  }

  private String getJoinedStatements(String[] statements) {
    return Joiner.on("; ").skipNulls().join(statements);
  }

  private ImmutableList<String> getFirstColumnValues(List<Row> rows) {
    return FluentIterable.from(rows)
      .transform(new Function<Row, String>() {
        @Override
        public String apply(Row input) {
          Object[] values = input.getRow();
          return values.length > 0 ? (String) values[0] : NO_VALUE_MARKER;
        }
      }).toList();
  }

}
