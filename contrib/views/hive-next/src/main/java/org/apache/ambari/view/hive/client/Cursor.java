package org.apache.ambari.view.hive.client;

import java.util.Iterator;
import java.util.List;

/**
 * Created by dbhowmick on 5/26/16.
 */
public interface Cursor<T, R> extends Iterator<T>, Iterable<T>{
  boolean isResettable();
  void reset();
  int getOffset();
  List<R> getDescription();
}
