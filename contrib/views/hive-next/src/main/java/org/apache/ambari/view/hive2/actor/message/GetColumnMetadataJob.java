package org.apache.ambari.view.hive2.actor.message;

import org.apache.ambari.view.ViewContext;

/**
 * Created by dbhowmick on 5/25/16.
 */
public class GetColumnMetadataJob extends HiveJob {
  private final String schemaPattern;
  private final String tablePattern;
  private final String columnPattern;
  public GetColumnMetadataJob(String username, ViewContext viewContext,
                              String schemaPattern, String tablePattern, String columnPattern) {
    super(Type.SYNC, username, viewContext);
    this.schemaPattern = schemaPattern;
    this.tablePattern = tablePattern;
    this.columnPattern = columnPattern;
  }

  public GetColumnMetadataJob(String username, ViewContext viewContext,
                              String tablePattern, String columnPattern) {
    this(username, viewContext, "*", tablePattern, columnPattern);
  }

  public GetColumnMetadataJob(String username, ViewContext viewContext,
                              String columnPattern) {
    this(username, viewContext, "*", "*", columnPattern);
  }

  public GetColumnMetadataJob(String username, ViewContext viewContext) {
    this(username, viewContext, "*", "*", "*");
  }

  public String getSchemaPattern() {
    return schemaPattern;
  }

  public String getTablePattern() {
    return tablePattern;
  }

  public String getColumnPattern() {
    return columnPattern;
  }
}
