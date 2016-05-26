package org.apache.ambari.view.hive.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import com.google.common.collect.Lists;
import org.apache.ambari.view.hive.utils.ServiceFormattedException;
import org.apache.ambari.view.hive2.actor.message.job.FetchFailed;
import org.apache.ambari.view.hive2.actor.message.job.Next;
import org.apache.ambari.view.hive2.actor.message.job.NoMoreItems;
import org.apache.ambari.view.hive2.actor.message.job.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper over iterator actor and blocks to fetch Rows and ColumnDescription whenever there is no more Rows to be
 * returned.
 */
public class NonPersistentCursor implements Cursor<Row, ColumnDescription> {
  private final Logger LOG = LoggerFactory.getLogger(getClass());
  private static long DEFAULT_WAIT_TIMEOUT = 5000L;

  private final ActorSystem system;
  private final ActorRef actorRef;
  private final long readTimeOut;
  private final Queue<Row> rows = Lists.newLinkedList();
  private final List<ColumnDescription> descriptions = Lists.newLinkedList();
  private int offSet = 0;
  private boolean endReached = false;

  public NonPersistentCursor(ActorSystem system, ActorRef actorRef, long readTimeOut) {
    this.system = system;
    this.actorRef = actorRef;
    this.readTimeOut = readTimeOut;
  }

  public NonPersistentCursor(ActorSystem system, ActorRef actorRef) {
    this(system, actorRef, DEFAULT_WAIT_TIMEOUT);
  }

  @Override
  public boolean isResettable() {
    return false;
  }

  @Override
  public void reset() {
    // Do nothing
  }

  @Override
  public int getOffset() {
    return offSet;
  }

  @Override
  public List<ColumnDescription> getDescriptions() {
    fetchIfNeeded();
    return descriptions;
  }

  @Override
  public Iterator<Row> iterator() {
    return null;
  }

  @Override
  public boolean hasNext() {
    fetchIfNeeded();
    return !endReached;
  }

  @Override
  public Row next() {
    fetchIfNeeded();
    offSet++;
    return rows.poll();
  }

  @Override
  public void remove() {
    throw new RuntimeException("Read only cursor. Method not supported");
  }

  private void fetchIfNeeded() {
    if (endReached || rows.size() > 0) return;
    getNextRows();
  }

  private void getNextRows() {
    Inbox inbox = Inbox.create(system);
    inbox.send(actorRef, new Next());
    Object receive;
    try {
      receive = inbox.receive(Duration.create(readTimeOut, TimeUnit.MILLISECONDS));
    } catch (Throwable ex) {
      String errorMessage = "Result fetch timed out";
      LOG.error(errorMessage, ex);
      throw new ServiceFormattedException(errorMessage, ex);
    }

    if (receive instanceof Result) {
      Result result = (Result) receive;
      if (descriptions.isEmpty()) {
        descriptions.addAll(result.getColumns());
      }
      result.getRows();
      rows.addAll(result.getRows());
    }

    if (receive instanceof NoMoreItems) {
      endReached = true;
    }

    if (receive instanceof FetchFailed) {
      FetchFailed error = (FetchFailed) receive;
      LOG.error("Failed to fetch results ");
      throw new ServiceFormattedException(error.getMessage(), error.getError());
    }
  }
}
