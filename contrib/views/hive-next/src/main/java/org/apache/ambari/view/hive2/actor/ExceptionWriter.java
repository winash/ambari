/*
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

package org.apache.ambari.view.hive2.actor;

import akka.actor.UntypedActor;
import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.persistence.Storage;
import org.apache.ambari.view.hive2.persistence.utils.ItemNotFound;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.Job;
import org.apache.ambari.view.hive2.resources.jobs.viewJobs.JobImpl;
import org.apache.ambari.view.hive2.actor.message.job.AsyncExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.ambari.view.utils.hdfs.HdfsApiException;
import org.apache.ambari.view.utils.hdfs.HdfsUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionWriter extends UntypedActor {

  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  private final HdfsApi hdfsApi;
  private final Storage storage;

  public ExceptionWriter(HdfsApi hdfsApi, Storage storage) {
    this.hdfsApi = hdfsApi;
    this.storage = storage;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof AsyncExecutionFailed) {
      AsyncExecutionFailed exception = (AsyncExecutionFailed) message;
      updateError(exception.getJobId(), exception.getMessage(), exception.getError());
    } else if (message instanceof ExecutionFailed) {
      ExecutionFailed exception = (ExecutionFailed) message;
      writeError(exception.getMessage(), exception.getError());
    }
  }

  private void writeError(String message, Throwable error) {
    LOG.error(message, error);
  }

  private void updateError(String jobId, String message, Throwable error) {
    Optional<JobImpl> jobOptional = getJob(jobId);
    if (!jobOptional.isPresent()) {
      LOG.error("Failed to get Job info from database. Job id: {}. Failed to write error logs with message: {}. Exception: {}", jobId, message, error);
      return;
    }
    JobImpl job = jobOptional.get();
    String logFile = job.getLogFile();
    updateLogFile(logFile, message, error);
    job.setStatus(Job.JOB_STATE_ERROR);
    storage.store(JobImpl.class, job);
  }

  private void updateLogFile(String logFile, String message, Throwable error) {
    String errorString = getErrorString(message, error);
    try {
      HdfsUtil.putStringToFile(hdfsApi, logFile, errorString);
    } catch (HdfsApiException e) {
      LOG.error("Failed to update logfile {} in HDFS. Falling back to exception logging in next log. Error: {}", logFile, e);
      LOG.error("Fallback: {}", errorString);
    }
  }

  private String getErrorString(String message, Throwable error) {
    StringBuilder builder = new StringBuilder();
    builder.append(message);
    builder.append("\n");
    builder.append(ExceptionUtils.getStackTrace(error));
    return builder.toString();
  }

  private Optional<JobImpl> getJob(String id) {
    try {
      return Optional.of(storage.load(JobImpl.class, id));
    } catch (ItemNotFound itemNotFound) {
      return Optional.absent();
    }
  }
}
