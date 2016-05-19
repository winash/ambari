package org.apache.ambari.view.hive2.actor.message.job;

import com.google.common.collect.ImmutableList;
import org.apache.ambari.view.hive2.actor.ResultSetIterator;

import java.util.List;

/**
 * Created by dbhowmick on 5/19/16.
 */
public class Result {
  private final List<ResultSetIterator.Row> rows;

  public Result(List<ResultSetIterator.Row> rows) {
    this.rows = ImmutableList.copyOf(rows);
  }

  public List<ResultSetIterator.Row> getRows() {
    return rows;
  }
}
