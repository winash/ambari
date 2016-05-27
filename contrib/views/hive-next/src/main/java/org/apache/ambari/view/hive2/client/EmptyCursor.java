package org.apache.ambari.view.hive2.client;

import com.beust.jcommander.internal.Lists;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by arajeev on 27/05/16.
 */
public class EmptyCursor implements Cursor<Row, ColumnDescription> {

    private List<Row> rows = Lists.newArrayList();
    private List<ColumnDescription> desc = Lists.newArrayList();


    @Override
    public boolean isResettable() {
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public int getOffset() {
        return 0;
    }

    @Override
    public List<ColumnDescription> getDescriptions() {
        return desc;
    }

    /**
     * Returns an iterator over a set of elements of type T.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<Row> iterator() {
        return rows.iterator();
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        return false;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Row next() {
        throw new NotImplementedException();
    }

    /**
     * Removes from the underlying collection the last element returned
     * by this iterator (optional operation).  This method can be called
     * only once per call to {@link #next}.  The behavior of an iterator
     * is unspecified if the underlying collection is modified while the
     * iteration is in progress in any way other than by calling this
     * method.
     *
     * @throws UnsupportedOperationException if the {@code remove}
     *                                       operation is not supported by this iterator
     * @throws IllegalStateException         if the {@code next} method has not
     *                                       yet been called, or the {@code remove} method has already
     *                                       been called after the last call to the {@code next}
     *                                       method
     */
    @Override
    public void remove() {
        throw new NotImplementedException();
    }
}
