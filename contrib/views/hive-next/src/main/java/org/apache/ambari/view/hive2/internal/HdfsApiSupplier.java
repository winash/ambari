package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.ambari.view.utils.hdfs.HdfsApiException;
import org.apache.ambari.view.utils.hdfs.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HdfsApiSupplier implements ContextSupplier<Optional<HdfsApi>> {

  protected final Logger LOG =
    LoggerFactory.getLogger(getClass());

  private static final Map<String, HdfsApi> hdfsApiMap = new ConcurrentHashMap<>();
  private final Object lock = new Object();

  @Override
  public Optional<HdfsApi> get(ViewContext context) {
    try {
      if(!hdfsApiMap.containsKey(context.getInstanceName())) {
        synchronized (lock) {
          if(!hdfsApiMap.containsKey(context.getInstanceName())) {
            LOG.debug("Creating HDFSApi instance for Viewname: {}, Instance Name: {}", context.getViewName(), context.getInstanceName());
            HdfsApi api = HdfsUtil.connectToHDFSApi(context);
            hdfsApiMap.put(context.getInstanceName(), api);
            return Optional.of(api);
          }
        }
      }
      return Optional.of(hdfsApiMap.get(context.getInstanceName()));
    } catch (HdfsApiException e) {
      LOG.error("Cannot get the HDFS API", e);
      return Optional.absent();
    }
  }
}
