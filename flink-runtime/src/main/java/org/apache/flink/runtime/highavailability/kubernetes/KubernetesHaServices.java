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

package org.apache.flink.runtime.highavailability.kubernetes;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.KubernetesCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.KubernetesUtils;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link HighAvailabilityServices} using Kubernetes.
 */
public class KubernetesHaServices implements HighAvailabilityServices {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesHaServices.class);

	private static final String RESOURCE_MANAGER_LEADER_PATH = ".resource.manager.lock";

	private static final String DISPATCHER_LEADER_PATH = ".dispatcher.lock";

	private static final String JOB_MANAGER_LEADER_PATH = ".job.manager.lock";

	private static final String REST_SERVER_LEADER_PATH = ".rest.server.lock";

	/** The Kubernetes apiContext to use. */
	private final KubernetesApiContext apiContext;

	/** The executor to run Kubernetes callbacks on. */
	private final Executor executor;

	/** The runtime configuration. */
	private final Configuration configuration;

	/** The Kubernetes based running jobs registry. */
	private final RunningJobsRegistry runningJobsRegistry;

	/** Store for arbitrary blobs. */
	private final BlobStoreService blobStoreService;

	public KubernetesHaServices(
			KubernetesApiContext apiContext,
			Executor executor,
			Configuration configuration,
			BlobStoreService blobStoreService) {
		this.apiContext = checkNotNull(apiContext);
		this.executor = checkNotNull(executor);
		this.configuration = checkNotNull(configuration);
		this.runningJobsRegistry = new KubernetesRunningJobsRegistry(apiContext, configuration);
		this.blobStoreService = checkNotNull(blobStoreService);
	}

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, RESOURCE_MANAGER_LEADER_PATH);
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		return KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, DISPATCHER_LEADER_PATH);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		return KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, getPathForJobManager(jobID));
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return getJobManagerLeaderRetriever(jobID);
	}

	@Override
	public LeaderRetrievalService getWebMonitorLeaderRetriever() {
		return KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, REST_SERVER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return KubernetesUtils.createLeaderElectionService(apiContext, configuration, RESOURCE_MANAGER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		return KubernetesUtils.createLeaderElectionService(apiContext, configuration, DISPATCHER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		return KubernetesUtils.createLeaderElectionService(apiContext, configuration, getPathForJobManager(jobID));
	}

	@Override
	public LeaderElectionService getWebMonitorLeaderElectionService() {
		return KubernetesUtils.createLeaderElectionService(apiContext, configuration, REST_SERVER_LEADER_PATH);
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return new KubernetesCheckpointRecoveryFactory(apiContext, configuration, executor);
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		return KubernetesUtils.createSubmittedJobGraphStore(apiContext, configuration);
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
		return runningJobsRegistry;
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		return blobStoreService;
	}

	// ------------------------------------------------------------------------
	//  Shutdown
	// ------------------------------------------------------------------------

	@Override
	public void close() throws Exception {
		Throwable exception = null;

		try {
			blobStoreService.close();
		} catch (Throwable t) {
			exception = t;
		}

		// TODO: how to close kubernetes api ?

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close the KubernetesHaServices.");
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		LOG.info("Close and clean up all data for KubernetesHaServices.");

		Throwable exception = null;

		try {
			blobStoreService.closeAndCleanupAllData();
		} catch (Throwable t) {
			exception = t;
		}

		try {
			KubernetesUtils.cleanUpConfigMaps(apiContext);
		} catch (Throwable t) {
			exception = ExceptionUtils.firstOrSuppressed(t, exception);
		}

		// TODO: how to close kubernetes api ?

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close and clean up all data of KubernetesHaServices.");
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static String getPathForJobManager(final JobID jobID) {
		return "." + KubernetesUtils.concatPath(jobID.toString(), JOB_MANAGER_LEADER_PATH);
	}
}
