package org.apache.ambari.view.hive2.actor.message;

import com.google.common.collect.ImmutableList;
import org.apache.ambari.view.ViewContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by dbhowmick on 5/25/16.
 */
public class DDLJob extends HiveJob {

  private final String[] statements;

  public DDLJob(Type type, String[] statements, String username, ViewContext viewContext) {
    super(type, username, viewContext);
    this.statements = statements;
  }

  public Collection<String> getStatements() {
    return Arrays.asList(statements);
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
}
