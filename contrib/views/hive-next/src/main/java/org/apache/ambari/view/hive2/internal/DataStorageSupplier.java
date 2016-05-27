package org.apache.ambari.view.hive2.internal;

import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.persistence.DataStoreStorage;
import org.apache.ambari.view.hive2.persistence.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A supplier for data storage
 * Duplicated to keep the API uniform
 */
public class DataStorageSupplier implements ContextSupplier<Storage> {

  protected final Logger LOG =
    LoggerFactory.getLogger(getClass());

  @Override
  public Storage get(ViewContext context) {
    LOG.debug("Creating storage instance for Viewname: {}, Instance Name: {}", context.getViewName(), context.getInstanceName());
    return new DataStoreStorage(context);
  }
}
