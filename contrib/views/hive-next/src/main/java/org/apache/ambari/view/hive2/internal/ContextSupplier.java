package org.apache.ambari.view.hive2.internal;

import org.apache.ambari.view.ViewContext;

/**
 * A class that can supply objects of same type.
 * @param <T>
 */
public interface ContextSupplier<T> {
  /**
   * Retrieves an instance of appropriate type. The returned object could be a new instance
   * or an exiting instance. No guarantee on that.
   * @param context View Context to be used to create the instance
   * @return instance of appropriateType
   */
  T get(ViewContext context);
}
