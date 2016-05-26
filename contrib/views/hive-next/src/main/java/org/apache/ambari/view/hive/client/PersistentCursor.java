package org.apache.ambari.view.hive.client;


import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Wrapper over other iterables. Does not block and can be reset to start again from beginning.
 */
public class PersistentCursor<T, R> implements Cursor<T, R>  {
  private List<T> rows = Lists.newArrayList();
  private List<R> columns = Lists.newArrayList();
  private int offset = 0;

  public PersistentCursor(List<T> rows, List<R> columns) {
    this.rows = rows;
    this.columns = columns;
  }


  @Override
  public Iterator<T> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return rows.size() > 0 && offset < rows.size();
  }

  @Override
  public T next() {
    T row = rows.get(offset);
    offset++;
    return row;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Read only cursor. Method not supported");
  }

  @Override
  public boolean isResettable() {
    return true;
  }

  @Override
  public void reset() {
    this.offset = 0;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public List<R> getDescriptions() {
    return columns;
  }
}
