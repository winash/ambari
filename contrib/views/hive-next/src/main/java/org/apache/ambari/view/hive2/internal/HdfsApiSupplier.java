package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.ambari.view.utils.hdfs.HdfsApiException;
import org.apache.ambari.view.utils.hdfs.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsApiSupplier implements ContextSupplier<Optional<HdfsApi>> {

  protected final Logger LOG =
    LoggerFactory.getLogger(getClass());

  @Override
  public Optional<HdfsApi> get(ViewContext context) {
    try {
      LOG.debug("Creating HDFSApi instance for Viewname: {}, Instance Name: {}", context.getViewName(), context.getInstanceName());
      return Optional.of(HdfsUtil.connectToHDFSApi(context));
    } catch (HdfsApiException e) {
      LOG.error("Cannot get the HDFS API", e);
    }
    return Optional.absent();
  }
}
