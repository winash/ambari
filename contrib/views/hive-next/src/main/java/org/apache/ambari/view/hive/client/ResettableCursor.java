package org.apache.ambari.view.hive.client;

/**
 * Created by dbhowmick on 5/26/16.
 */
public interface ResettableCursor {
  void reset();
  int getOffset();
}
