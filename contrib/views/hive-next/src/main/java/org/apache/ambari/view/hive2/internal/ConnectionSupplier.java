package org.apache.ambari.view.hive2.internal;

import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.ConnectionDelegate;
import org.apache.ambari.view.hive2.HiveJdbcConnectionDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionSupplier implements ContextSupplier<ConnectionDelegate> {

  protected final Logger LOG =
    LoggerFactory.getLogger(getClass());

  @Override
  public ConnectionDelegate get(ViewContext context) {
    LOG.debug("Creating Connection delegate instance for Viewname: {}, Instance Name: {}", context.getViewName(), context.getInstanceName());
    return new HiveJdbcConnectionDelegate();
  }
}
