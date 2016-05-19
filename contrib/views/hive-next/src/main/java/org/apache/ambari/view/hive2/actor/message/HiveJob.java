package org.apache.ambari.view.hive2.actor.message;

import com.google.common.collect.ImmutableList;
import org.apache.ambari.view.ViewContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public abstract class HiveJob {

  private final String username;
  private final String[] statements;
  private final Type type;
  private final ViewContext viewContext;

  public HiveJob(Type type, String[] statements, String username,ViewContext viewContext) {
    this.type = type;
    this.username = username;
    this.statements = statements;
    this.viewContext = viewContext;
  }

  public String getUsername() {
    return username;
  }

  public Collection<String> getStatements() {
    return Arrays.asList(statements);
  }


  public Type getType() {
    return type;
  }

  /**
   * Get the statements to be executed synchronously
   *
   * @return
   */
  public Collection<String> getSyncStatements() {
    if (!(statements.length > 1))
      return Collections.emptyList();
    else
      return ImmutableList.copyOf(Arrays.copyOfRange(statements, 0, statements.length - 2));
  }

  /**
   * Get the statement to be executed asynchronously
   *
   * @return async statement
   */
  public String getAsyncStatement() {
    return statements[statements.length - 1];
  }

  public ViewContext getViewContext() {
    return viewContext;
  }


  public enum Type {
    SYNC,
    ASYNC
  }

}
