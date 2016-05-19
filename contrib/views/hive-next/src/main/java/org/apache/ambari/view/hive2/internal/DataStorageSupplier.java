package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Supplier;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.persistence.DataStoreStorage;
import org.apache.ambari.view.hive.persistence.LocalKeyValueStorage;
import org.apache.ambari.view.hive.persistence.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A supplier for data storage
 * Duplicated to keep the API uniform
 */
public class DataStorageSupplier implements Supplier<Storage> {

    private ViewContext context;
    protected final Logger LOG =
            LoggerFactory.getLogger(getClass());


    public DataStorageSupplier(ViewContext viewContext) {
        this.context = viewContext;
    }

    /**
     * Retrieves an instance of the appropriate type. The returned object may or
     * may not be a new instance, depending on the implementation.
     *
     * @return an instance of the appropriate type
     */
    @Override
    public Storage get() {
        return new DataStoreStorage(context);
    }
}
