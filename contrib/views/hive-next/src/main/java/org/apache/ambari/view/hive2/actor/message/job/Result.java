package org.apache.ambari.view.hive2.actor.message.job;

import com.google.common.collect.ImmutableList;
import org.apache.ambari.view.hive.client.Row;
import org.apache.ambari.view.hive2.actor.ResultSetIterator;

import java.util.List;

/**
 * Created by dbhowmick on 5/19/16.
 */
public class Result {
  private final Row columns;
  private final List<Row> rows;

  public Result(List<Row> rows, Row columns) {
    this.rows = ImmutableList.copyOf(rows);
    this.columns = columns;
  }

  public List<Row> getRows() {
    return rows;
  }

  public Row getColumns() {
    return columns;
  }
}
