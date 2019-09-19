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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.kubernetes.KubernetesStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.util.KubernetesUtils;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.util.FlinkException;

import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.util.Watch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SubmittedJobGraph} instances for JobManagers running in {@link HighAvailabilityMode#KUBERNETES}.
 */
public class KubernetesSubmittedJobGraphStore implements SubmittedJobGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesSubmittedJobGraphStore.class);

	private static final String JOB_GRAPHS_CONFIG_MAP_NAME = "jobgraphs";

	/** Lock to synchronize with the {@link SubmittedJobGraphListener}. */
	private final Object cacheLock = new Object();

	private final KubernetesApiContext apiContext;

	/** The set of IDs of all added job graphs. */
	private final Set<JobID> addedJobGraphs = new HashSet<>();

	/** Completed checkpoints in Kubernetes. */
	private final KubernetesStateHandleStore<SubmittedJobGraph> jobGraphsInKubernetes;

	/** The external listener to be notified on races. */
	private SubmittedJobGraphListener jobGraphListener;

	/** Flag indicating whether this instance is running. */
	private volatile boolean isRunning;

	private Thread watchThread;

	private Watch<V1ConfigMap> watch;

	public KubernetesSubmittedJobGraphStore(
		KubernetesApiContext apiContext,
		String currentJobsPath,
		RetrievableStateStorageHelper<SubmittedJobGraph> stateStorage) {

		this.apiContext = checkNotNull(apiContext).usingConfigMapNamePrefix(
			KubernetesUtils.concatPath(apiContext.getNamespace(), currentJobsPath));
		this.jobGraphsInKubernetes = new KubernetesStateHandleStore<>(
			this.apiContext, stateStorage, JOB_GRAPHS_CONFIG_MAP_NAME);
	}

	@Override
	public void start(SubmittedJobGraphListener jobGraphListener) throws Exception {
		synchronized (cacheLock) {
			if (!isRunning) {
				this.jobGraphListener = jobGraphListener;
				isRunning = true;

				// TODO: unify thread group ?
				watchThread = new Thread(new WatchRunnable());
				watchThread.start();
			}
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (cacheLock) {
			if (!isRunning) {
				return;
			}
			jobGraphListener = null;
			isRunning = false;
		}
		if (watch != null) {
			try {
				watch.close();
			} catch (Exception e) {
			}
		}
		if (watchThread != null) {
			try {
				watchThread.interrupt();
				watchThread.join();
			} catch (InterruptedException e) {
			}
		}
	}

	@Nullable
	@Override
	public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		final String path = getPathForJob(jobId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Recovering job graph {} from {}.{} {}.",
				jobId, apiContext.getConfigMapNamePrefix(), JOB_GRAPHS_CONFIG_MAP_NAME, path);
		}

		synchronized (cacheLock) {
			verifyIsRunning();

			RetrievableStateHandle<SubmittedJobGraph> jobGraphRetrievableStateHandle;
			try {
				jobGraphRetrievableStateHandle = jobGraphsInKubernetes.get(path);
				if (jobGraphRetrievableStateHandle == null) {
					return null;
				}
			} catch (Exception e) {
				throw new FlinkException("Could not retrieve the submitted job graph state handle " +
					"for " + path + " from the submitted job graph store.", e);
			}

			SubmittedJobGraph jobGraph;
			try {
				jobGraph = jobGraphRetrievableStateHandle.retrieveState();
			} catch (ClassNotFoundException cnfe) {
				throw new FlinkException("Could not retrieve submitted JobGraph from state handle under " + path +
					". This indicates that you are trying to recover from state written by an " +
					"older Flink version which is not compatible. Try cleaning the state handle store.", cnfe);
			} catch (IOException ioe) {
				throw new FlinkException("Could not retrieve submitted JobGraph from state handle under " + path +
					". This indicates that the retrieved state handle is broken. Try cleaning the state handle " +
					"store.", ioe);
			}

			addedJobGraphs.add(jobGraph.getJobId());

			LOG.info("Recovered {}.", jobGraph);

			return jobGraph;
		}
	}

	@Override
	public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");
		String path = getPathForJob(jobGraph.getJobId());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Add job graph {} to {}.{} {}.",
				jobGraph.getJobId(), apiContext.getConfigMapNamePrefix(), JOB_GRAPHS_CONFIG_MAP_NAME, path);
		}

		boolean success = false;
		while (!success) {
			synchronized (cacheLock) {
				verifyIsRunning();
				if (!jobGraphsInKubernetes.getAllKeysForState().contains(path) || (addedJobGraphs.contains(jobGraph.getJobId()))) {
					try {
						jobGraphsInKubernetes.add(path, jobGraph);

						addedJobGraphs.add(jobGraph.getJobId());

						success = true;
					} catch (Exception e) {
						LOG.warn("Caught exception while adding job graph to Kubernetes, retry again. Exception: ", e);
					}
				} else {
					throw new IllegalStateException("Oh, no. Trying to update a graph you didn't " +
						"#getAllSubmittedJobGraphs() or #putJobGraph() yourself before.");
				}
			}
		}

		LOG.info("Added jobGraph {} to Kubernetes.", jobGraph);
	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		String path = getPathForJob(jobId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Removing job graph {} from {}.{} {}.",
				jobId, apiContext.getConfigMapNamePrefix(), JOB_GRAPHS_CONFIG_MAP_NAME, path);
		}

		synchronized (cacheLock) {
			if (addedJobGraphs.contains(jobId)) {
				jobGraphsInKubernetes.removeAndDiscardState(path);

				addedJobGraphs.remove(jobId);
			}
		}

		LOG.info("Removed job graph {} from ZooKeeper.", jobId);
	}

	@Override
	public void releaseJobGraph(JobID jobId) throws Exception {
		// TODO: not supported yet
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		Collection<String> paths;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Retrieving all stored job ids from Kubernetes in {}.{}",
				apiContext.getConfigMapNamePrefix(), JOB_GRAPHS_CONFIG_MAP_NAME);
		}

		try {
			paths = jobGraphsInKubernetes.getAllKeysForState();
		} catch (Exception e) {
			throw new Exception("Failed to retrieve entry paths from KubernetesStateHandleStore.", e);
		}

		List<JobID> jobIds = new ArrayList<>(paths.size());

		for (String path : paths) {
			try {
				jobIds.add(jobIdfromPath(path));
			} catch (Exception exception) {
				LOG.warn("Could not parse job id from {}. This indicates a malformed path.", path, exception);
			}
		}

		return jobIds;
	}

	/**
	 * Verifies that the state is running.
	 */
	private void verifyIsRunning() {
		checkState(isRunning, "Not running. Forgot to call start()?");
	}

	/**
	 * Returns the JobID as a String (with leading slash).
	 */
	public static String getPathForJob(JobID jobId) {
		checkNotNull(jobId, "Job ID");
		return String.format("%s", jobId);
	}

	/**
	 * Returns the JobID from the given path in ZooKeeper.
	 *
	 * @param path in ZooKeeper
	 * @return JobID associated with the given path
	 */
	public static JobID jobIdfromPath(final String path) {
		return JobID.fromHexString(path);
	}

	private class WatchRunnable implements Runnable {
		@Override
		public void run() {
			while (isRunning) {
				try {
					watch = KubernetesUtils.createConfigMapWatch(apiContext, JOB_GRAPHS_CONFIG_MAP_NAME);
					for (Watch.Response<V1ConfigMap> response : watch) {
						switch (response.type) {
							case "ADDED":
							case "MODIFIED":
								handleConfigMapChanged(response.type);
								break;
							case "DELETED":
								LOG.warn("ConfigMap [{}.{}] has been deleted.",
									apiContext.getConfigMapNamePrefix(), JOB_GRAPHS_CONFIG_MAP_NAME);
								handleConfigMapChanged(response.type);
								break;
							default:
								LOG.warn("Got unknown response type [" + response.type + "] while watching ConfigMap ["
									+ JOB_GRAPHS_CONFIG_MAP_NAME + "]");
						}
					}
				} catch (Exception e) {
					LOG.warn("Caught Exception during watching ConfigMap [" + JOB_GRAPHS_CONFIG_MAP_NAME + "], exception: ", e);
				}
			}
		}
	}

	private void handleConfigMapChanged(String responseType) throws Exception {
		LOG.info("SubmittedJobGraph ConfigMap [{}] has been {}.", apiContext.getConfigMapNamePrefix(), responseType);
		synchronized (cacheLock) {
			Collection<JobID> newJobIDs = getJobIds();
			Set<JobID> jobIDsToAdd = new HashSet<>();
			Set<JobID> jobIDsToRemove = new HashSet<>(addedJobGraphs);

			newJobIDs.forEach(jobID -> {
				if (!jobIDsToRemove.remove(jobID)) {
					jobIDsToAdd.add(jobID);
				}
			});

			for (JobID jobId : jobIDsToRemove) {
				try {
					if (jobGraphListener != null && addedJobGraphs.contains(jobId)) {
						try {
							jobGraphListener.onRemovedJobGraph(jobId);
						} catch (Throwable t) {
							LOG.error("Error in callback", t);
						}
					}

					break;
				} catch (Exception e) {
					LOG.error("Error in SubmittedJobGraphsPathCacheListener", e);
				}
			}

			for (JobID jobId : jobIDsToAdd) {
				try {
					if (jobGraphListener != null && !addedJobGraphs.contains(jobId)) {
						try {
							// Whoa! This has been added by someone else. Or we were fast
							// to remove it (false positive).
							jobGraphListener.onAddedJobGraph(jobId);
						} catch (Throwable t) {
							LOG.error("Error in callback", t);
						}
					}
				} catch (Exception e) {
					LOG.error("Error in SubmittedJobGraphsPathCacheListener", e);
				}
			}
		}
	}
}
