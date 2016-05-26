package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import com.google.common.collect.Lists;
import org.apache.ambari.view.hive.client.ColumnDescription;
import org.apache.ambari.view.hive.client.ColumnDescriptionShort;
import org.apache.ambari.view.hive.client.Row;
import org.apache.ambari.view.hive.persistence.Storage;
import org.apache.ambari.view.hive.persistence.utils.ItemNotFound;
import org.apache.ambari.view.hive.resources.jobs.viewJobs.Job;
import org.apache.ambari.view.hive.resources.jobs.viewJobs.JobImpl;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.AdvanceCursor;
import org.apache.ambari.view.hive2.actor.message.job.FetchFailed;
import org.apache.ambari.view.hive2.actor.message.job.Next;
import org.apache.ambari.view.hive2.actor.message.job.NoMoreItems;
import org.apache.ambari.view.hive2.actor.message.job.Result;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;

public class ResultSetIterator extends HiveActor {
  private static final int DEFAULT_BATCH_SIZE = 100;
  public static final String NULL = "NULL";

  private final ActorRef parent;
  private final ResultSet resultSet;
  private final int batchSize;

  private static ResultSetMetaData metaData;
  private List<ColumnDescription> columnDescriptions;
  private NumberFormat nf = new DecimalFormat();
  private int columnCount;
  private Storage storage;
  boolean buffered = false;
  Result lastResult;


  private boolean metaDataFetched = false;

  public ResultSetIterator(ActorRef parent, ResultSet resultSet, int batchSize) {
    this.parent = parent;
    this.resultSet = resultSet;
    this.batchSize = batchSize;
  }


  public ResultSetIterator(ActorRef parent, ResultSet resultSet, Storage storage) {
    this(parent, resultSet);
    this.storage = storage;
    this.buffered = true;
  }

  public ResultSetIterator(ActorRef parent, ResultSet resultSet) {
    this(parent, resultSet, DEFAULT_BATCH_SIZE);
  }

  @Override
  void handleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();
    if (message instanceof Next) {
      getNext();
    }
    if(message instanceof AdvanceCursor){
      AdvanceCursor moveCursor = (AdvanceCursor) message;
      String jobid = moveCursor.getJob();
      getNext();

      try {
        JobImpl job = storage.load(JobImpl.class, jobid);
        job.setStatus(Job.JOB_STATE_FINISHED);
        storage.store(JobImpl.class, job);
      } catch (ItemNotFound itemNotFound) {
        //TODO: Handle error
      }
    }

  }

  private void getNext() {
    /**
     * Quick fix for pre buffered result sets
     */
    if(buffered && lastResult != null){
      sender().tell(lastResult, self());
      buffered =false;
      return;
    }
    List<Row> rows = Lists.newArrayList();
    if (!metaDataFetched) {
      try {
        initialize();
      } catch (SQLException ex) {
        sender().tell(new FetchFailed("Failed to get metadata for ResultSet", ex), self());
        // TODO: Tell the parent to clean up
      }
    }
    int index = 0;
    try {
      while (resultSet.next() && index < batchSize) {
        index++;
        rows.add(getRowFromResultSet(resultSet));
      }

      if (index == 0) {
        // We have hit end of resultSet
        sender().tell(new NoMoreItems(), self());
        // TODO: Tell the parent to clean up
      } else {
        Result result = new Result(rows, columnDescriptions);
        lastResult = result;
        sender().tell(result, self());
      }

    } catch (SQLException ex) {
      sender().tell(new FetchFailed("Failed to get metadata for ResultSet", ex), self());
      // TODO: Tell the parent to clean up
    }
  }

  private Row getRowFromResultSet(ResultSet resultSet) throws SQLException {
    Object[] values = new Object[columnCount];
    for(int i = 0; i < columnCount; i++) {
      values[i] = resultSet.getObject(i + 1);
    }
    return new Row(values);
  }

  private void initialize() throws SQLException {
    metaDataFetched = true;
    metaData = resultSet.getMetaData();
    columnCount = metaData.getColumnCount();
    columnDescriptions = Lists.newArrayList();
    for(int i = 1; i <= columnCount; i++) {
      String columnName = metaData.getColumnName(i);
      String typeName = metaData.getColumnTypeName(i);
      ColumnDescription description = new ColumnDescriptionShort(columnName, typeName, i);
      columnDescriptions.add(description);
    }
  }
}
