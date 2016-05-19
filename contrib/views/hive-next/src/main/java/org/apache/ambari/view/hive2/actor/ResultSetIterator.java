package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.google.common.collect.Lists;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.apache.ambari.view.hive2.actor.message.job.FetchFailed;
import org.apache.ambari.view.hive2.actor.message.job.Next;
import org.apache.ambari.view.hive2.actor.message.job.NoMoreItems;
import org.apache.ambari.view.hive2.actor.message.job.Result;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dbhowmick on 5/18/16.
 */
public class ResultSetIterator extends HiveActor {
  private static final int DEFAULT_BATCH_SIZE = 100;
  public static final String NULL = "NULL";

  private final ActorRef parent;
  private final ResultSet resultSet;
  private final int batchSize;

  private static ResultSetMetaData metaData;
  private Row colNames;
  private NumberFormat nf = new DecimalFormat();
  private int columnCount;

  private boolean metaDataFetched = false;

  public ResultSetIterator(ActorRef parent, ResultSet resultSet, int batchSize) {
    this.parent = parent;
    this.resultSet = resultSet;
    this.batchSize = batchSize;


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
  }

  private void getNext() {
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
        rows.add(new Row(columnCount, resultSet));
      }

      if (index == 0) {
        // We have hit end of resultSet
        sender().tell(new NoMoreItems(), self());
        // TODO: Tell the parent to clean up
      } else {
        sender().tell(new Result(rows), self());
      }

    } catch (SQLException ex) {
      sender().tell(new FetchFailed("Failed to get metadata for ResultSet", ex), self());
      // TODO: Tell the parent to clean up
    }
  }

  private void initialize() throws SQLException {
    metaDataFetched = true;
    nf.setRoundingMode(RoundingMode.FLOOR);
    nf.setMinimumFractionDigits(0);
    nf.setMaximumFractionDigits(2);
    metaData = resultSet.getMetaData();
    columnCount = metaData.getColumnCount();
    colNames = new Row(columnCount);
  }


  public class Row {
    String[] values;

    public Row(int size) throws SQLException {
      values = new String[size];
      for (int i = 0; i < size; i++) {
        values[i] = metaData.getColumnLabel(i + 1);
      }
    }


    public Row(int size, ResultSet rs) throws SQLException {
      values = new String[size];
      for (int i = 0; i < size; i++) {
        if (nf != null) {
          Object object = rs.getObject(i + 1);
          if (object == null) {
            values[i] = null;
          } else if (object instanceof Number) {
            values[i] = nf.format(object);
          } else {
            values[i] = object.toString();
          }
        } else {
          values[i] = rs.getString(i + 1);
        }
        values[i] = values[i] == null ? NULL : values[i];

      }

    }

    @Override
    public String toString() {
      return "Row{" +
        "values=" + Arrays.toString(values) +
        '}';
    }
  }

}
