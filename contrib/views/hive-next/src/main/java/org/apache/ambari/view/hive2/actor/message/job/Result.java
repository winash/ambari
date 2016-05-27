package org.apache.ambari.view.hive2.actor.message.job;

import com.google.common.collect.ImmutableList;
import org.apache.ambari.view.hive2.client.ColumnDescription;
import org.apache.ambari.view.hive2.client.Row;

import java.util.List;

/**
 * Created by dbhowmick on 5/19/16.
 */
public class Result {
  private final List<ColumnDescription> columns;
  private final List<Row> rows;

  public Result(List<Row> rows, List<ColumnDescription> columns) {
    this.rows = ImmutableList.copyOf(rows);
    this.columns = columns;
  }

  public List<Row> getRows() {
    return rows;
  }

  public List<ColumnDescription> getColumns() {
    return columns;
  }
}
