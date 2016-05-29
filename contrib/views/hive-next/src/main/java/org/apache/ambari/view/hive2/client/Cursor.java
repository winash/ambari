package org.apache.ambari.view.hive2.client;

import java.util.Iterator;
import java.util.List;

/**
 * Created by dbhowmick on 5/26/16.
 */
public interface Cursor<T, R> extends Iterator<T>, Iterable<T>{
  boolean isResettable();
  void reset();
  int getOffset();
  List<R> getDescriptions();
  void keepAlive();
}
