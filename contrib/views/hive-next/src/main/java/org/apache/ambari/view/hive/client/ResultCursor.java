package org.apache.ambari.view.hive.client;

import java.util.List;

/**
 * Created by dbhowmick on 5/26/16.
 */
public interface ResultCursor<T> extends Cursor<T>{
  List<ColumnDescription> getDescription();
}
