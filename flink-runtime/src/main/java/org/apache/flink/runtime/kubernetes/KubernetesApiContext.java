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

package org.apache.flink.runtime.kubernetes;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1OwnerReference;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A helper class for Kubernetes client and its related configurations.
 */
public class KubernetesApiContext {

	private final ApiClient apiClient;

	private final CoreV1Api coreV1Api;

	private final String namespace;

	private final String configMapNamePrefix;

	private final Duration leaderElectionLeaseDuration;

	private final Duration leaderElectionRenewDeadline;

	private final Duration leaderElectionRetryPeriod;

	private final V1OwnerReference ownerReference;

	public KubernetesApiContext(ApiClient apiClient, CoreV1Api coreV1Api, String namespace, String configMapNamePrefix,
		Duration leaderElectionLeaseDuration, Duration leaderElectionRenewDeadline, Duration leaderElectionRetryPeriod,
		V1OwnerReference ownerReference) {
		this.apiClient = checkNotNull(apiClient);
		this.coreV1Api = checkNotNull(coreV1Api);
		this.namespace = checkNotNull(namespace);
		this.configMapNamePrefix = checkNotNull(configMapNamePrefix);
		this.leaderElectionLeaseDuration = checkNotNull(leaderElectionLeaseDuration);
		this.leaderElectionRenewDeadline = leaderElectionRenewDeadline; // TODO: seems this configuration is useless
		this.leaderElectionRetryPeriod = checkNotNull(leaderElectionRetryPeriod);
		this.ownerReference = checkNotNull(ownerReference);
	}

	public ApiClient getApiClient() {
		return apiClient;
	}

	public CoreV1Api getCoreV1Api() {
		return coreV1Api;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getConfigMapNamePrefix() {
		return configMapNamePrefix;
	}

	public Duration getLeaderElectionLeaseDuration() {
		return leaderElectionLeaseDuration;
	}

	public Duration getLeaderElectionRenewDeadline() {
		return leaderElectionRenewDeadline;
	}

	public Duration getLeaderElectionRetryPeriod() {
		return leaderElectionRetryPeriod;
	}

	public V1OwnerReference getOwnerReference() {
		return ownerReference;
	}

	/**
	 * Spawns a facade of {@link KubernetesApiContext} using another prefix.
	 * @param newConfigMapNamePrefix
	 * @return
	 */
	public KubernetesApiContext usingConfigMapNamePrefix(String newConfigMapNamePrefix) {
		return new KubernetesApiContext(
			getApiClient(),
			getCoreV1Api(),
			getNamespace(),
			newConfigMapNamePrefix,
			getLeaderElectionLeaseDuration(),
			getLeaderElectionRenewDeadline(),
			getLeaderElectionRetryPeriod(),
			getOwnerReference());
	}
}
