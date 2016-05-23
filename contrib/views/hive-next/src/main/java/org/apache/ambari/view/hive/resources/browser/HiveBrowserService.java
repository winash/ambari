/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ambari.view.hive.resources.browser;

import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.ViewResourceHandler;
import org.apache.ambari.view.hive.client.ConnectionConfig;
import org.apache.ambari.view.hive.client.DDLDelegator;
import org.apache.ambari.view.hive.client.DDLDelegatorImpl;
import org.apache.ambari.view.hive.utils.BadRequestFormattedException;
import org.apache.ambari.view.hive.utils.ServiceFormattedException;
import org.apache.ambari.view.hive2.ConnectionSystem;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Database access resource
 */
public class HiveBrowserService {
  @Inject
  ViewResourceHandler handler;
  @Inject
  protected ViewContext context;

  protected final static Logger LOG =
    LoggerFactory.getLogger(HiveBrowserService.class);

  private static final long EXPIRING_TIME = 10 * 60 * 1000;  // 10 minutes
  /*private static Map<String, Cursor> resultsCache;
  private UserLocalConnection connectionLocal = new UserLocalConnection();

  public static Map<String, Cursor> getResultsCache() {
    if (resultsCache == null) {
      PassiveExpiringMap<String, Cursor> resultsCacheExpiringMap =
          new PassiveExpiringMap<String, Cursor>(EXPIRING_TIME);
      resultsCache = Collections.synchronizedMap(resultsCacheExpiringMap);
    }
    return resultsCache;
  }
  */

  /**
   * Returns list of databases
   */
  @GET
  @Path("database")
  @Produces(MediaType.APPLICATION_JSON)
  public Response databases(@QueryParam("like") String like,
                            @QueryParam("first") String fromBeginning,
                            @QueryParam("count") Integer count,
                            @QueryParam("columns") final String requestedColumns) {
    if (like == null)
      like = "*";
    else
      like = "*" + like + "*";
    JSONObject response = new JSONObject();
    DDLDelegator delegator = new DDLDelegatorImpl(context, ConnectionSystem.getInstance().getActorSystem(), ConnectionSystem.getInstance().getOperationController());
    List<String> databases = delegator.getDbList(getHiveConnectionConfig(), like);
    response.put("databases", databases);
    return Response.ok(response).build();

  }

  /**
   * Returns list of databases
   */
  @GET
  @Path("database.page")
  @Produces(MediaType.APPLICATION_JSON)
  public Response databasesPaginated(@QueryParam("like") String like,
                                     @QueryParam("first") String fromBeginning,
                                     @QueryParam("count") Integer count,
                                     @QueryParam("searchId") String searchId,
                                     @QueryParam("format") String format,
                                     @QueryParam("columns") final String requestedColumns) {
    if (like == null)
      like = "*";
    else
      like = "*" + like + "*";
    String curl = null;
    try {
      final String finalLike = like;
      return null;
      /*return ResultsPaginationController.getInstance(context)
          .request("databases", searchId, false, fromBeginning, count, format,
                  new Callable<Cursor>() {
                    @Override
                    public Cursor call() throws Exception {
                      TSessionHandle session = connectionLocal.get(context).getOrCreateSessionByTag("DDL");
                      return connectionLocal.get(context).ddl().getDBListCursor(session, finalLike);
                    }
                  }).build();*/


      //TODO: New implementation
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (IllegalArgumentException ex) {
      throw new BadRequestFormattedException(ex.getMessage(), ex);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex, curl);
    }
  }

  /**
   * Returns list of databases
   */
  @GET
  @Path("database/{db}/table")
  @Produces(MediaType.APPLICATION_JSON)
  public Response tablesInDatabase(@PathParam("db") String db,
                                   @QueryParam("like") String like,
                                   @QueryParam("first") String fromBeginning,
                                   @QueryParam("count") Integer count,
                                   @QueryParam("columns") final String requestedColumns) {
    if (like == null)
      like = "*";
    else
      like = "*" + like + "*";

    JSONObject response = new JSONObject();
    DDLDelegator delegator = new DDLDelegatorImpl(context, ConnectionSystem.getInstance().getActorSystem(), ConnectionSystem.getInstance().getOperationController());
    List<String> tables = delegator.getTableList(getHiveConnectionConfig(), db, like);
    response.put("tables", tables);
    response.put("database", db);
    return Response.ok(response).build();

  }

  /**
   * Returns list of databases
   */
  @GET
  @Path("database/{db}/table.page")
  @Produces(MediaType.APPLICATION_JSON)
  public Response tablesInDatabasePaginated(@PathParam("db") final String db,
                                            @QueryParam("like") String like,
                                            @QueryParam("first") String fromBeginning,
                                            @QueryParam("count") Integer count,
                                            @QueryParam("searchId") String searchId,
                                            @QueryParam("format") String format,
                                            @QueryParam("columns") final String requestedColumns) {
    if (like == null)
      like = "*";
    else
      like = "*" + like + "*";
    String curl = null;
    try {
      final String finalLike = like;
      /*return ResultsPaginationController.getInstance(context)
          .request(db + ":tables", searchId, false, fromBeginning, count, format,
                  new Callable<Cursor>() {
                    @Override
                    public Cursor call() throws Exception {
                      TSessionHandle session = connectionLocal.get(context).getOrCreateSessionByTag("DDL");
                      Cursor cursor = connectionLocal.get(context).ddl().getTableListCursor(session, db, finalLike);
                      cursor.selectColumns(requestedColumns);
                      return cursor;
                    }
                  }).build();*/
      return null;

      //TODO: New implementation

    } catch (WebApplicationException ex) {
      throw ex;
    } catch (IllegalArgumentException ex) {
      throw new BadRequestFormattedException(ex.getMessage(), ex);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex, curl);
    }
  }

  /**
   * Returns list of databases
   */
  @GET
  @Path("database/{db}/table/{table}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response describeTable(@PathParam("db") String db,
                                @PathParam("table") String table,
                                @QueryParam("like") String like,
                                @QueryParam("columns") String requestedColumns,
                                @QueryParam("extended") String extended) {
    boolean extendedTableDescription = (extended != null && extended.equals("true"));
    String curl = null;
    try {
      JSONObject response = new JSONObject();
      //TSessionHandle session = connectionLocal.get(context).getOrCreateSessionByTag("DDL");
      //List<ColumnDescription> columnDescriptions = connectionLocal.get(context).ddl()
      //    .getTableDescription(session, db, table, like, extendedTableDescription);
      //response.put("columns", columnDescriptions);
      response.put("database", db);
      response.put("table", table);

      //TODO: New implementation

      return Response.ok(response).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (IllegalArgumentException ex) {
      throw new BadRequestFormattedException(ex.getMessage(), ex);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex, curl);
    }
  }

  /**
   * Returns list of databases
   */
  @GET
  @Path("database/{db}/table/{table}.page")
  @Produces(MediaType.APPLICATION_JSON)
  public Response describeTablePaginated(@PathParam("db") final String db,
                                         @PathParam("table") final String table,
                                         @QueryParam("like") final String like,
                                         @QueryParam("first") String fromBeginning,
                                         @QueryParam("searchId") String searchId,
                                         @QueryParam("count") Integer count,
                                         @QueryParam("format") String format,
                                         @QueryParam("columns") final String requestedColumns) {
    String curl = null;
    try {
      /*return ResultsPaginationController.getInstance(context)
          .request(db + ":tables:" + table + ":columns", searchId, false, fromBeginning, count, format,
              new Callable<Cursor>() {
                @Override
                public Cursor call() throws Exception {
                  TSessionHandle session = connectionLocal.get(context).getOrCreateSessionByTag("DDL");
                  Cursor cursor = connectionLocal.get(context).ddl().
                      getTableDescriptionCursor(session, db, table, like);
                  cursor.selectColumns(requestedColumns);
                  return cursor;
                }
              }).build();*/
      return null;

      //TODO: New implementation

    } catch (WebApplicationException ex) {
      throw ex;
    } catch (IllegalArgumentException ex) {
      throw new BadRequestFormattedException(ex.getMessage(), ex);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex, curl);
    }
  }


  private ConnectionConfig getHiveConnectionConfig() {
    return new ConnectionConfig.ConnectionConfigBuilder()
      .withUsername("admin")
      .withPassword("")
      .withHost("c6402.ambari.apache.org")
      .withPort(10000)
      .build();
  }
}
