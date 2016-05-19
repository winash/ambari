package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.ambari.view.utils.hdfs.HdfsApiException;
import org.apache.ambari.view.utils.hdfs.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsApiSupplier implements Supplier<Optional<HdfsApi>> {

    protected final Logger LOG =
            LoggerFactory.getLogger(getClass());

    private final ViewContext context;

    public HdfsApiSupplier(ViewContext viewContext) {
        this.context = viewContext;
    }

    /**
     * Retrieves an instance of the appropriate type. The returned object may or
     * may not be a new instance, depending on the implementation.
     *
     * @return an instance of the appropriate type
     */
    @Override
    public Optional<HdfsApi> get() {
        try {
            return Optional.of(HdfsUtil.connectToHDFSApi(context));
        } catch (HdfsApiException e) {
            LOG.error("Cannot get the HDFS API");
        }
        return Optional.absent();
    }
}
