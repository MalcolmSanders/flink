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
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.util.KubernetesUtils;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.models.V1ConfigMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Kubernetes based registry for running jobs, highly available.
 */
public class KubernetesRunningJobsRegistry implements RunningJobsRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesRunningJobsRegistry.class);

	private static final Charset ENCODING = Charset.forName("utf-8");

	private final KubernetesApiContext apiContext;

	private final String runningJobPath;

	public KubernetesRunningJobsRegistry(KubernetesApiContext apiContext,  final Configuration configuration) {
		this.apiContext = checkNotNull(apiContext);
		String rawPath =  KubernetesUtils.normalizePath(
			configuration.getString(KubernetesHighAvailabilityOptions.HA_KUBERNETES_RUNNING_JOB_REGISTRY_PATH));
		this.runningJobPath = rawPath.charAt(0) == '.' ? rawPath.substring(1) : rawPath;
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			writeEnumToConfigMap(jobID, JobSchedulingStatus.RUNNING);
		}
		catch (Exception e) {
			throw new IOException("Failed to set RUNNING state in ZooKeeper for job " + jobID, e);
		}
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			writeEnumToConfigMap(jobID, JobSchedulingStatus.DONE);
		}
		catch (Exception e) {
			throw new IOException("Failed to set DONE state in ZooKeeper for job " + jobID, e);
		}
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			final String configMapName = createConfigMapName(jobID);
			final byte[] data = KubernetesUtils.getBinaryDataFromConfigMap(apiContext, configMapName);
			if (data != null) {
				try {
					final String name = new String(data, ENCODING);
					return JobSchedulingStatus.valueOf(name);
				} catch (IllegalArgumentException e) {
					throw new IOException("Found corrupt data in ConfigMap: " +
						Arrays.toString(data) + " is no valid job status");
				}
			}

			// nothing found, yet, must be in status 'PENDING'
			return JobSchedulingStatus.PENDING;
		} catch (Exception e) {
			throw new IOException("Get finished state from ConfigMap fail for job " + jobID.toString(), e);
		}
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			final String configMapName = createConfigMapName(jobID);
			KubernetesUtils.deleteConfigMap(apiContext, configMapName);
		} catch (Exception e) {
			throw new IOException("Failed to clear job state from ConfigMap for job " + jobID, e);
		}
	}

	private String createConfigMapName(JobID jobID) {
		return KubernetesUtils.concatPath(runningJobPath, jobID.toString());
	}

	private void writeEnumToConfigMap(JobID jobID, JobSchedulingStatus status) {
		final String configMapName = createConfigMapName(jobID);
		V1ConfigMap configMap = KubernetesUtils.buildConfigMapWithBinaryData(apiContext, configMapName,
			status.name().getBytes(ENCODING));
		// TODO: what if this action cannot be fulfilled ?
		boolean success = false;
		while (!success) {
			try {
				KubernetesUtils.createConfigMapOrUpdateBinaryData(apiContext, configMap);
				success = true;
			} catch (ApiException e) {
				LOG.error("Cannot write JobSchedulingStatus to k8s, status: " + status + ", exception: "
					+ e.getResponseBody());
			}
		}
	}
}
