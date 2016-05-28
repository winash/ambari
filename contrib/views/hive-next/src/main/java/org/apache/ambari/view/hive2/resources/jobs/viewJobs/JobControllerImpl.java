/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ambari.view.hive2.resources.jobs.viewJobs;

import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.client.AsyncJobRunner;
import org.apache.ambari.view.hive2.client.AsyncJobRunnerImpl;
import org.apache.ambari.view.hive2.client.ConnectionConfig;
import org.apache.ambari.view.hive2.client.HiveClientRuntimeException;
import org.apache.ambari.view.hive2.persistence.utils.ItemNotFound;
import org.apache.ambari.view.hive2.resources.jobs.ModifyNotificationDelegate;
import org.apache.ambari.view.hive2.resources.jobs.ModifyNotificationInvocationHandler;
import org.apache.ambari.view.hive2.resources.jobs.atsJobs.IATSParser;
import org.apache.ambari.view.hive2.resources.savedQueries.SavedQuery;
import org.apache.ambari.view.hive2.resources.savedQueries.SavedQueryResourceManager;
import org.apache.ambari.view.hive2.utils.BadRequestFormattedException;
import org.apache.ambari.view.hive2.utils.FilePaginator;
import org.apache.ambari.view.hive2.utils.MisconfigurationFormattedException;
import org.apache.ambari.view.hive2.utils.ServiceFormattedException;
import org.apache.ambari.view.hive2.ConnectionFactory;
import org.apache.ambari.view.hive2.ConnectionSystem;
import org.apache.ambari.view.hive2.actor.message.AsyncJob;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.internal.AsyncExecutionSuccess;
import org.apache.ambari.view.hive2.internal.Either;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.ambari.view.utils.hdfs.HdfsApiException;
import org.apache.ambari.view.utils.hdfs.HdfsUtil;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JobControllerImpl implements JobController, ModifyNotificationDelegate {
    private final static Logger LOG =
            LoggerFactory.getLogger(JobControllerImpl.class);

    private ViewContext context;
    private HdfsApi hdfsApi;
    private Job jobUnproxied;
    private Job job;
    private boolean modified;

    //private OperationHandleControllerFactory opHandleControllerFactory;
    private SavedQueryResourceManager savedQueryResourceManager;
    private IATSParser atsParser;

    /**
     * JobController constructor
     * Warning: Create JobControllers ONLY using JobControllerFactory!
     */
    public JobControllerImpl(ViewContext context, Job job,
                             //OperationHandleControllerFactory opHandleControllerFactory,
                             SavedQueryResourceManager savedQueryResourceManager,
                             IATSParser atsParser,
                             HdfsApi hdfsApi) {
        this.context = context;
        setJobPOJO(job);
        //this.opHandleControllerFactory = opHandleControllerFactory;
        this.savedQueryResourceManager = savedQueryResourceManager;
        this.atsParser = atsParser;
        this.hdfsApi = hdfsApi;

        //UserLocalConnection connectionLocal = new UserLocalConnection();
        //this.hiveConnection = new ConnectionController(opHandleControllerFactory, connectionLocal.get(context));
    }

    public String getQueryForJob() {
        FilePaginator paginator = new FilePaginator(job.getQueryFile(), hdfsApi);
        String query;
        try {
            query = paginator.readPage(0);  //warning - reading only 0 page restricts size of query to 1MB
        } catch (IOException e) {
            throw new ServiceFormattedException("F030 Error when reading file " + job.getQueryFile(), e);
        } catch (InterruptedException e) {
            throw new ServiceFormattedException("F030 Error when reading file " + job.getQueryFile(), e);
        }
        return query;
    }

    private static final String DEFAULT_DB = "default";

    public String getJobDatabase() {
        if (job.getDataBase() != null) {
            return job.getDataBase();
        } else {
            return DEFAULT_DB;
        }
    }

  /*@Override
  public OperationHandleController.OperationStatus getStatus() throws ItemNotFound, HiveClientException, NoOperationStatusSetException {
    OperationHandleController handle = opHandleControllerFactory.getHandleForJob(job);
    return handle.getOperationStatus();
  }*/

    @Override
    public void submit() throws Throwable {
        String jobDatabase = getJobDatabase();
        String query = getQueryForJob();
        ConnectionSystem system = ConnectionSystem.getInstance();
        AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(system.getOperationController(), system.getActorSystem());
        // create async Job
        //
        AsyncJob asyncJob = new AsyncJob(job.getId(), context.getUsername(), getStatements(jobDatabase, query), job.getLogFile(), context);
        Either<AsyncExecutionSuccess, AsyncExecutionFailed> submitJob = asyncJobRunner.submitJob(getHiveConnectionConfig(), asyncJob, job);
        if (submitJob.isLeft()) {
            // The job ran successfully
            LOG.info("The job " + asyncJob + " was completed successfully");
        } else {
            // the Job failed
            LOG.info("The job " + asyncJob + " was not submitted", submitJob.getRight().getError());
            // fail the Job
            job.setStatus(Job.JOB_STATE_ERROR);
            throw submitJob.getRight().getError();
        }

    }

    private String[] getStatements(String jobDatabase, String query) {
        String[] split = query.split("\\r?\\n");
        String[] strings = {"use " + jobDatabase};
        String[] both = (String[])ArrayUtils.addAll(strings,split);
        return both;
    }

    private void setupHiveBeforeQueryExecute() {
        String database = getJobDatabase();
        //hiveConnection.selectDatabase(getSession(), database);
        //TODO: New implementation
    }

  /*private TSessionHandle getSession() {
    try {
      if (job.getSessionTag() != null) {
        return hiveConnection.getSessionByTag(getJob().getSessionTag());
      }
    } catch (HiveClientException ignore) {
      LOG.debug("Stale sessionTag was provided, new session will be opened");
    }

    String tag = hiveConnection.openSession();
    job.setSessionTag(tag);
    try {
      return hiveConnection.getSessionByTag(tag);
    } catch (HiveClientException e) {
      throw new HiveClientFormattedException(e);
    }
  }*/

    @Override
    public void cancel() throws ItemNotFound {
        //OperationHandleController handle = opHandleControllerFactory.getHandleForJob(job);
        //handle.cancel();
    }

    @Override
    public void update() {
        updateOperationStatus();
        updateOperationLogs();

        updateJobDuration();
    }

    public void updateOperationStatus() {
        try {

            //OperationHandleController handle = opHandleControllerFactory.getHandleForJob(job);
            //OperationHandleController.OperationStatus status = handle.getOperationStatus();
      /*job.setStatus(status.status);
      job.setStatusMessage(status.message);
      job.setSqlState(status.sqlState);*/
            LOG.debug("Status of job#" + job.getId() + " is " + job.getStatus());

        } catch (Exception /*NoOperationStatusSetException*/ e) {
            LOG.info("Operation state is not set for job#" + job.getId());

        } /*catch (HiveErrorStatusException e) {
      LOG.debug("Error updating status for job#" + job.getId() + ": " + e.getMessage());
      job.setStatus(ExecuteJob.JOB_STATE_UNKNOWN);

    } catch (HiveClientException e) {
      throw new HiveClientFormattedException(e);

    } catch (ItemNotFound itemNotFound) {
      LOG.debug("No TOperationHandle for job#" + job.getId() + ", can't update status");
    }*/
    }

    public void updateOperationLogs() {
        try {
            //OperationHandleController handle = opHandleControllerFactory.getHandleForJob(job);
            //String logs = handle.getLogs();

            //LogParser info = LogParser.parseLog(logs);
            //LogParser.AppId app = info.getLastAppInList();
      /*if (app != null) {
        job.setApplicationId(app.getIdentifier());
      }*/

            String logFilePath = job.getLogFile();
            //HdfsUtil.putStringToFile(hdfsApi, logFilePath, logs);

        } catch (HiveClientRuntimeException ex) {
            LOG.error("Error while fetching logs: " + ex.getMessage());
        } /*catch (ItemNotFound itemNotFound) {
      LOG.debug("No TOperationHandle for job#" + job.getId() + ", can't read logs");
    } catch (HdfsApiException e) {
      throw new ServiceFormattedException(e);
    }*/
    }

    public boolean isJobEnded() {
        String status = job.getStatus();
        return status.equals(Job.JOB_STATE_FINISHED) || status.equals(Job.JOB_STATE_CANCELED) ||
                status.equals(Job.JOB_STATE_CLOSED) || status.equals(Job.JOB_STATE_ERROR) ||
                status.equals(Job.JOB_STATE_UNKNOWN); // Unknown is not finished, but polling makes no sense
    }

    @Override
    public Job getJob() {
        return job;
    }

    /**
     * Use carefully. Returns unproxied bean object
     * @return unproxied bean object
     */
    @Override
    public Job getJobPOJO() {
        return jobUnproxied;
    }

    public void setJobPOJO(Job jobPOJO) {
        Job jobModifyNotificationProxy = (Job) Proxy.newProxyInstance(jobPOJO.getClass().getClassLoader(),
                new Class[]{Job.class},
                new ModifyNotificationInvocationHandler(jobPOJO, this));
        this.job = jobModifyNotificationProxy;

        this.jobUnproxied = jobPOJO;
    }

  /*@Override
  public Cursor getResults() throws ItemNotFound {
    OperationHandleController handle = opHandleControllerFactory.getHandleForJob(job);
    return handle.getResults();
  }*/

    @Override
    public boolean hasResults() throws ItemNotFound {
        //OperationHandleController handle = opHandleControllerFactory.getHandleForJob(job);
        //return handle.hasResults();
        return false;
    }

    @Override
    public void afterCreation() {
        setupStatusDirIfNotPresent();
        setupQueryFileIfNotPresent();
        setupLogFileIfNotPresent();

        setCreationDate();
    }

    public void setupLogFileIfNotPresent() {
        if (job.getLogFile() == null || job.getLogFile().isEmpty()) {
            setupLogFile();
        }
    }

    public void setupQueryFileIfNotPresent() {
        if (job.getQueryFile() == null || job.getQueryFile().isEmpty()) {
            setupQueryFile();
        }
    }

    public void setupStatusDirIfNotPresent() {
        if (job.getStatusDir() == null || job.getStatusDir().isEmpty()) {
            setupStatusDir();
        }
    }

    private static final long MillisInSecond = 1000L;

    public void updateJobDuration() {
        job.setDuration(System.currentTimeMillis() / MillisInSecond - job.getDateSubmitted());
    }

    public void setCreationDate() {
        job.setDateSubmitted(System.currentTimeMillis() / MillisInSecond);
    }


    private void setupLogFile() {
        LOG.debug("Creating log file for job#" + job.getId());

        String logFile = job.getStatusDir() + "/" + "logs";
        try {
            HdfsUtil.putStringToFile(hdfsApi, logFile, "");
        } catch (HdfsApiException e) {
            throw new ServiceFormattedException(e);
        }

        job.setLogFile(logFile);
        LOG.debug("Log file for job#" + job.getId() + ": " + logFile);
    }

    private void setupStatusDir() {
        String newDirPrefix = makeStatusDirectoryPrefix();
        String newDir = null;
        try {
            newDir = HdfsUtil.findUnallocatedFileName(hdfsApi, newDirPrefix, "");
        } catch (HdfsApiException e) {
            throw new ServiceFormattedException(e);
        }

        job.setStatusDir(newDir);
        LOG.debug("Status dir for job#" + job.getId() + ": " + newDir);
    }

    private String makeStatusDirectoryPrefix() {
        String userScriptsPath = context.getProperties().get("jobs.dir");

        if (userScriptsPath == null) { // TODO: move check to initialization code
            String msg = "jobs.dir is not configured!";
            LOG.error(msg);
            throw new MisconfigurationFormattedException("jobs.dir");
        }

        String normalizedName = String.format("hive-job-%s", job.getId());
        String timestamp = new SimpleDateFormat("yyyy-MM-dd_hh-mm").format(new Date());
        return String.format(userScriptsPath +
                "/%s-%s", normalizedName, timestamp);
    }

    private void setupQueryFile() {
        String statusDir = job.getStatusDir();
        assert statusDir != null : "setupStatusDir() should be called first";

        String jobQueryFilePath = statusDir + "/" + "query.hql";

        try {

            if (job.getForcedContent() != null) {

                HdfsUtil.putStringToFile(hdfsApi, jobQueryFilePath, job.getForcedContent());
                job.setForcedContent("");  // prevent forcedContent to be written to DB

            } else if (job.getQueryId() != null) {

                String savedQueryFile = getRelatedSavedQueryFile();
                hdfsApi.copy(savedQueryFile, jobQueryFilePath);
                job.setQueryFile(jobQueryFilePath);

            } else {

                throw new BadRequestFormattedException("queryId or forcedContent should be passed!", null);

            }

        } catch (IOException e) {
            throw new ServiceFormattedException("F040 Error when creating file " + jobQueryFilePath, e);
        } catch (InterruptedException e) {
            throw new ServiceFormattedException("F040 Error when creating file " + jobQueryFilePath, e);
        } catch (HdfsApiException e) {
            throw new ServiceFormattedException(e);
        }
        job.setQueryFile(jobQueryFilePath);

        LOG.debug("Query file for job#" + job.getId() + ": " + jobQueryFilePath);
    }


    private ConnectionConfig getHiveConnectionConfig() {
        return ConnectionFactory.create(context);
    }

    private String getRelatedSavedQueryFile() {
        SavedQuery savedQuery;
        try {
            savedQuery = savedQueryResourceManager.read(job.getQueryId());
        } catch (ItemNotFound itemNotFound) {
            throw new BadRequestFormattedException("queryId not found!", itemNotFound);
        }
        return savedQuery.getQueryFile();
    }

    @Override
    public boolean onModification(Object object) {
        setModified(true);
        return true;
    }

    @Override
    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    @Override
    public void clearModified() {
        setModified(false);
    }
}
