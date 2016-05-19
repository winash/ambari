package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Supplier;

import java.util.List;

/**
 * Create instances of classes
 * for which no constructors have been specified
 * @param <T>
 */
public class DefaultSupplier<T> implements Supplier<T>{

    private Class<T> clazz;
    T instance;

    public DefaultSupplier(T instance) {
        this.instance = instance;
    }

    public DefaultSupplier(Class<T> clazz) throws IllegalAccessException, InstantiationException {
        this.clazz = clazz;
    }

    /**
     * Get the instance
     * @return
     */
    @Override
    public T get() {
        if(clazz != null){
            try {
                return clazz.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }
        } else {
            return instance;
        }
    }
}
