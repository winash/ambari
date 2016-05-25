package org.apache.ambari.view.hive.client;

import java.util.List;

/**
 * Created by dbhowmick on 5/20/16.
 */
public interface DDLDelegator {

  List<String> getDbList(ConnectionConfig config, String like);

  List<String> getTableList(ConnectionConfig config, String database, String like);

  List<ColumnDescription> getTableDescription(ConnectionConfig config, String database, String table, String like, boolean extended);

  Cursor<Row> getDbListCursor(ConnectionConfig config, String like);

  Cursor<Row> getTableListCursor(ConnectionConfig config, String database, String like);

  Cursor<Row> getTableDescriptionCursor(ConnectionConfig config, String database, String table, String like, boolean extended);
}
