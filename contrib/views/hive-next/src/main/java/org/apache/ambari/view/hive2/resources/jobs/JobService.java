/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ambari.view.hive2.resources.jobs;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Optional;
import org.apache.ambari.view.ViewResourceHandler;
import org.apache.ambari.view.hive2.BaseService;
import org.apache.ambari.view.hive2.backgroundjobs.BackgroundJobController;
import org.apache.ambari.view.hive2.client.AsyncJobRunner;
import org.apache.ambari.view.hive2.client.AsyncJobRunnerImpl;
import org.apache.ambari.view.hive2.client.ColumnDescription;
import org.apache.ambari.view.hive2.client.Cursor;
import org.apache.ambari.view.hive2.client.EmptyCursor;
import org.apache.ambari.view.hive2.client.HiveAuthCredentials;
import org.apache.ambari.view.hive2.client.HiveClientException;
import org.apache.ambari.view.hive2.client.NonPersistentCursor;
import org.apache.ambari.view.hive2.client.Row;
import org.apache.ambari.view.hive2.client.UserLocalHiveAuthCredentials;
import org.apache.ambari.view.hive2.persistence.utils.ItemNotFound;
import org.apache.ambari.view.hive2.resources.jobs.atsJobs.IATSParser;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.Job;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.JobController;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.JobImpl;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.JobResourceManager;
import org.apache.ambari.view.hive2.utils.MisconfigurationFormattedException;
import org.apache.ambari.view.hive2.utils.NotFoundFormattedException;
import org.apache.ambari.view.hive2.utils.ServiceFormattedException;
import org.apache.ambari.view.hive2.utils.SharedObjectsFactory;
import org.apache.ambari.view.hive2.ConnectionSystem;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Servlet for queries
 * API:
 * GET /:id
 *      read job
 * POST /
 *      create new job
 *      Required: title, queryFile
 * GET /
 *      get all Jobs of current user
 */
public class JobService extends BaseService {
  @Inject
  ViewResourceHandler handler;

  private JobResourceManager resourceManager;
  private IOperationHandleResourceManager opHandleResourceManager;
  //private UserLocalConnection connectionLocal = new UserLocalConnection();

  protected final static Logger LOG =
      LoggerFactory.getLogger(JobService.class);
  private Aggregator aggregator;

  protected synchronized JobResourceManager getResourceManager() {
    if (resourceManager == null) {
      SharedObjectsFactory connectionsFactory = getSharedObjectsFactory();
      resourceManager = new JobResourceManager(connectionsFactory, context);
    }
    return resourceManager;
  }

  protected IOperationHandleResourceManager getOperationHandleResourceManager() {
    if (opHandleResourceManager == null) {
      opHandleResourceManager = new OperationHandleResourceManager(getSharedObjectsFactory());
    }
    return opHandleResourceManager;
  }

  protected Aggregator getAggregator() {
    if (aggregator == null) {
      IATSParser atsParser = getSharedObjectsFactory().getATSParser();
      aggregator = new Aggregator(getResourceManager(), getOperationHandleResourceManager(), atsParser);
    }
    return aggregator;
  }

  protected void setAggregator(Aggregator aggregator) {
    this.aggregator = aggregator;
  }

  /**
   * Get single item
   */
  @GET
  @Path("{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOne(@PathParam("jobId") String jobId) {
    try {
      JobController jobController = getResourceManager().readController(jobId);

      JSONObject jsonJob = jsonObjectFromJob(jobController);

      return Response.ok(jsonJob).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (ItemNotFound itemNotFound) {
      throw new NotFoundFormattedException(itemNotFound.getMessage(), itemNotFound);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  private JSONObject jsonObjectFromJob(JobController jobController) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    Job hiveJob = jobController.getJobPOJO();

    Job mergedJob;
    try {
      mergedJob = getAggregator().readATSJob(hiveJob);
    } catch (ItemNotFound itemNotFound) {
      throw new ServiceFormattedException("E010 ExecuteJob not found", itemNotFound);
    }
    Map createdJobMap = PropertyUtils.describe(mergedJob);
    createdJobMap.remove("class"); // no need to show Bean class on client

    JSONObject jobJson = new JSONObject();
    jobJson.put("job", createdJobMap);
    return jobJson;
  }

  /**
   * Get job results in csv format
   */
  @GET
  @Path("{jobId}/results/csv")
  @Produces("text/csv")
  public Response getResultsCSV(@PathParam("jobId") String jobId,
                                @Context HttpServletResponse response,
                                @QueryParam("fileName") String fileName,
                                @QueryParam("columns") final String requestedColumns) {
    try {

      final String username = context.getUsername();

      ConnectionSystem system = ConnectionSystem.getInstance();
      final AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(system.getOperationController(), system.getActorSystem());

      Optional<NonPersistentCursor> cursorOptional = asyncJobRunner.resetAndGetCursor(jobId, username);

      if(!cursorOptional.isPresent()){
        throw new Exception("Download failed");
      }

      final NonPersistentCursor resultSet = cursorOptional.get();


      StreamingOutput stream = new StreamingOutput() {
        @Override
        public void write(OutputStream os) throws IOException, WebApplicationException {
          Writer writer = new BufferedWriter(new OutputStreamWriter(os));
          CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
          try {

            List<ColumnDescription> descriptions = resultSet.getDescriptions();
            List<String> headers = Lists.newArrayList();
            for (ColumnDescription description : descriptions) {
              headers.add(description.getName());
            }

            csvPrinter.printRecord(headers.toArray());

            while (resultSet.hasNext()) {
              csvPrinter.printRecord(resultSet.next().getRow());
              writer.flush();
            }
          } finally {
            writer.close();
          }
        }
      };

      if (fileName == null || fileName.isEmpty()) {
        fileName = "results.csv";
      }

      return Response.ok(stream).
          header("Content-Disposition", String.format("attachment; filename=\"%s\"", fileName)).
          build();


    } catch (WebApplicationException ex) {
      throw ex;
    }  catch (Throwable ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Get job results in csv format
   */
  @GET
  @Path("{jobId}/results/csv/saveToHDFS")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getResultsToHDFS(@PathParam("jobId") String jobId,
                                   @QueryParam("commence") String commence,
                                   @QueryParam("file") final String targetFile,
                                   @QueryParam("stop") final String stop,
                                   @QueryParam("columns") final String requestedColumns,
                                   @Context HttpServletResponse response) {
    try {

      final JobController jobController = getResourceManager().readController(jobId);
      final String username = context.getUsername();

      ConnectionSystem system = ConnectionSystem.getInstance();
      final AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(system.getOperationController(), system.getActorSystem());

      Optional<NonPersistentCursor> cursorOptional = asyncJobRunner.resetAndGetCursor(jobId, username);

      if(!cursorOptional.isPresent()){
        throw new Exception("Download failed");
      }

      final NonPersistentCursor resultSet = cursorOptional.get();


      String backgroundJobId = "csv" + String.valueOf(jobController.getJob().getId());
      if (commence != null && commence.equals("true")) {
        if (targetFile == null)
          throw new MisconfigurationFormattedException("targetFile should not be empty");
        BackgroundJobController.getInstance(context).startJob(String.valueOf(backgroundJobId), new Runnable() {
          @Override
          public void run() {

            try {

              FSDataOutputStream stream = getSharedObjectsFactory().getHdfsApi().create(targetFile, true);
              Writer writer = new BufferedWriter(new OutputStreamWriter(stream));
              CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
              try {
                while (resultSet.hasNext() && !Thread.currentThread().isInterrupted()) {
                  csvPrinter.printRecord(resultSet.next().getRow());
                  writer.flush();
                }
              } finally {
                writer.close();
              }
              stream.close();

            } catch (IOException e) {
              throw new ServiceFormattedException("F010 Could not write CSV to HDFS for job#" + jobController.getJob().getId(), e);
            } catch (InterruptedException e) {
              throw new ServiceFormattedException("F010 Could not write CSV to HDFS for job#" + jobController.getJob().getId(), e);
            }
          }
        });
      }

      if (stop != null && stop.equals("true")) {
        BackgroundJobController.getInstance(context).interrupt(backgroundJobId);
      }

      JSONObject object = new JSONObject();
      object.put("stopped", BackgroundJobController.getInstance(context).isInterrupted(backgroundJobId));
      object.put("jobId", jobController.getJob().getId());
      object.put("backgroundJobId", backgroundJobId);
      object.put("operationType", "CSV2HDFS");
      object.put("status", BackgroundJobController.getInstance(context).state(backgroundJobId).toString());

      return Response.ok(object).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (ItemNotFound itemNotFound) {
      throw new NotFoundFormattedException(itemNotFound.getMessage(), itemNotFound);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }


  @Path("{jobId}/status")
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response fetchJobStatus(@PathParam("jobId") String jobId) throws ItemNotFound, HiveClientException, NoOperationStatusSetException {
    JobController jobController = getResourceManager().readController(jobId);
    //String jobStatus = jobController.getStatus().status;
    String jobStatus = "";
    //TODO: New implementation

    LOG.info("jobStatus : {} for jobId : {}",jobStatus, jobId);

    JSONObject jsonObject = new JSONObject();
    jsonObject.put("jobStatus", jobStatus);
    jsonObject.put("jobId", jobId);

    return Response.ok(jsonObject).build();
  }

  /**
   * Get next results page
   */
  @GET
  @Path("{jobId}/results")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getResults(@PathParam("jobId") final String jobId,
                             @QueryParam("first") String fromBeginning,
                             @QueryParam("count") Integer count,
                             @QueryParam("searchId") String searchId,
                             @QueryParam("format") String format,
                             @QueryParam("columns") final String requestedColumns) {
    try {

      final String username = context.getUsername();

      ConnectionSystem system = ConnectionSystem.getInstance();
      final AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(system.getOperationController(), system.getActorSystem());

      return ResultsPaginationController.getInstance(context)
              .request(jobId, searchId, true, fromBeginning, count, format,requestedColumns,
                      new Callable<Cursor< Row, ColumnDescription >>() {
                        @Override
                        public Cursor call() throws Exception {
                          Optional<NonPersistentCursor> cursor = asyncJobRunner.getCursor(jobId, username);
                          if(cursor.isPresent())
                          return cursor.get();
                          else
                            return new EmptyCursor();
                        }
                      }).build();

    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Renew expiration time for results
   */
  @GET
  @Path("{jobId}/results/keepAlive")
  public Response keepAliveResults(@PathParam("jobId") String jobId,
                             @QueryParam("first") String fromBeginning,
                             @QueryParam("count") Integer count) {
    try {
      if (!ResultsPaginationController.getInstance(context).keepAlive(jobId, ResultsPaginationController.DEFAULT_SEARCH_ID)) {
        throw new NotFoundFormattedException("Results already expired", null);
      }
      return Response.ok().build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Get progress info
   */
  @GET
  @Path("{jobId}/progress")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getProgress(@PathParam("jobId") String jobId) {
    try {
      final JobController jobController = getResourceManager().readController(jobId);

      ProgressRetriever.Progress progress = new ProgressRetriever(jobController.getJob(), getSharedObjectsFactory()).
          getProgress();

      return Response.ok(progress).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (ItemNotFound itemNotFound) {
      throw new NotFoundFormattedException(itemNotFound.getMessage(), itemNotFound);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Delete single item
   */
  @DELETE
  @Path("{id}")
  public Response delete(@PathParam("id") String id,
                         @QueryParam("remove") final String remove) {
    try {
      JobController jobController;
      try {
        jobController = getResourceManager().readController(id);
      } catch (ItemNotFound itemNotFound) {
        throw new NotFoundFormattedException(itemNotFound.getMessage(), itemNotFound);
      }
      jobController.cancel();
      if (remove != null && remove.compareTo("true") == 0) {
        getResourceManager().delete(id);
      }
      return Response.status(204).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (ItemNotFound itemNotFound) {
      throw new NotFoundFormattedException(itemNotFound.getMessage(), itemNotFound);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Get all Jobs
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getList() {
    try {
      LOG.debug("Getting all job");
      List<Job> allJobs = getAggregator().readAll(context.getUsername());
      for(Job job : allJobs) {
        job.setSessionTag(null);
      }

      JSONObject object = new JSONObject();
      object.put("jobs", allJobs);
      return Response.ok(object).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Create job
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response create(JobRequest request, @Context HttpServletResponse response,
                         @Context UriInfo ui) {
    try {
      Map jobInfo = PropertyUtils.describe(request.job);
      Job job = new JobImpl(jobInfo);


      getResourceManager().create(job);

      JobController createdJobController = getResourceManager().readController(job.getId());
      createdJobController.submit();
      getResourceManager().saveIfModified(createdJobController);

      response.setHeader("Location",
          String.format("%s/%s", ui.getAbsolutePath().toString(), job.getId()));

      JSONObject jobObject = jsonObjectFromJob(createdJobController);

      return Response.ok(jobObject).status(201).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (ItemNotFound itemNotFound) {
      throw new NotFoundFormattedException(itemNotFound.getMessage(), itemNotFound);
    } catch (Throwable ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Set password and connect to Hive
   */
  @POST
  @Path("auth")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setupPassword(AuthRequest request) {
    try {
      HiveAuthCredentials authCredentials = new HiveAuthCredentials();
      authCredentials.setPassword(request.password);
      new UserLocalHiveAuthCredentials().set(authCredentials, context);

      //connectionLocal.remove(context);  // force reconnect on next get
      //connectionLocal.get(context);
      return Response.ok().status(200).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Remove connection credentials
   */
  @DELETE
  @Path("auth")
  public Response removePassword() {
    try {
      new UserLocalHiveAuthCredentials().remove(context);
      //connectionLocal.remove(context);  // force reconnect on next get
      return Response.ok().status(200).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }


  /**
   * Invalidate session
   */
  @DELETE
  @Path("sessions/{sessionTag}")
  public Response invalidateSession(@PathParam("sessionTag") String sessionTag) {
    try {
      //Connection connection = connectionLocal.get(context);
      //connection.invalidateSessionByTag(sessionTag);
      return Response.ok().build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Session status
   */
  @GET
  @Path("sessions/{sessionTag}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response sessionStatus(@PathParam("sessionTag") String sessionTag) {
    try {
      //Connection connection = connectionLocal.get(context);

      JSONObject session = new JSONObject();
      session.put("sessionTag", sessionTag);
      try {
        //connection.getSessionByTag(sessionTag);
        session.put("actual", true);
      } catch (Exception /*HiveClientException*/ ex) {
        session.put("actual", false);
      }

      //TODO: New implementation

      JSONObject status = new JSONObject();
      status.put("session", session);
      return Response.ok(status).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Wrapper object for json mapping
   */
  public static class JobRequest {
    public JobImpl job;
  }

  /**
   * Wrapper for authentication json mapping
   */
  public static class AuthRequest {
    public String password;
  }
}
