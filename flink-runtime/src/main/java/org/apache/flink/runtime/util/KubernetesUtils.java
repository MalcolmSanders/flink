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

package org.apache.flink.runtime.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.KubernetesCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.KubernetesCompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.kubernetes.KubernetesHighAvailabilityOptions;
import org.apache.flink.runtime.jobmanager.KubernetesSubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.kubernetes.ConfigMapLock;
import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.leaderelection.KubernetesLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.KubernetesLeaderRetrievalService;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.reflect.TypeToken;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ConfigMapBuilder;
import io.kubernetes.client.models.V1OwnerReference;
import io.kubernetes.client.models.V1Status;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class containing helper functions to interact with Kubernetes.
 */
public class KubernetesUtils {

	public static final String API_VERSION = "v1";

	public static final String CONFIG_MAP_KIND = "ConfigMap";

	private static final String DEFAULT_CONFIG_MAP_KEY = "DefaultConfigMapKey";

	public static final String CONFIG_MAP_LABEL_KEY = "owner";

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

	private static final Pattern pathPattern =
		Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");

	public static KubernetesApiContext createKubernetesApiContext(Configuration configuration) throws Exception {
		Preconditions.checkNotNull(configuration, "configuration");

		String root = normalizePath(configuration.getValue(KubernetesHighAvailabilityOptions.HA_KUBERNETES_ROOT));
		String clusterPath = normalizePath(configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID));
		String rootWithClusterPath = concatPath(root, clusterPath);
		LOG.info("Using '{}' as Kubernetes ConfigMap name prefix.", rootWithClusterPath);

		String namespace = checkNotNull(configuration.getString(KubernetesHighAvailabilityOptions.KUBERNETES_NAMESPACE));

		String ownerReferenceKind = checkNotNull(configuration.getString(
			KubernetesHighAvailabilityOptions.KUBERNETES_OWNER_REFERENCE_KIND));
		String ownerReferenceName = checkNotNull(configuration.getString(
			KubernetesHighAvailabilityOptions.KUBERNETES_OWNER_REFERENCE_NAME));
		String ownerReferenceUid = checkNotNull(configuration.getString(
			KubernetesHighAvailabilityOptions.KUBERNETES_OWNER_REFERENCE_UID));

		V1OwnerReference ownerReference = new V1OwnerReference()
			.apiVersion(API_VERSION).kind(ownerReferenceKind)
			.name(ownerReferenceName).uid(ownerReferenceUid).blockOwnerDeletion(Boolean.TRUE);

		Duration leaderElectionLeaseDuration = Duration.ofMillis(configuration.getLong(
			KubernetesHighAvailabilityOptions.HA_KUBERNETES_LEADER_ELECTION_LEASE_DURATION_IN_MS));
		Duration leaderElectionRenewDeadline = Duration.ofMillis(configuration.getLong(
			KubernetesHighAvailabilityOptions.HA_KUBERNETES_LEADER_ELECTION_RENEW_DEADLINE_IN_MS));
		Duration leaderElectionRetryPeriod = Duration.ofMillis(configuration.getLong(
			KubernetesHighAvailabilityOptions.HA_KUBERNETES_LEADER_ELECTION_RETRY_PERIOD_IN_MS));

		ApiClient client = Config.defaultClient();
		client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
		io.kubernetes.client.Configuration.setDefaultApiClient(client);
		CoreV1Api api = new CoreV1Api();

		return new KubernetesApiContext(client, api, namespace, rootWithClusterPath,
			leaderElectionLeaseDuration, leaderElectionRenewDeadline, leaderElectionRetryPeriod, ownerReference);
	}

	public static CompletedCheckpointStore createCompletedCheckpointStore(
		KubernetesApiContext apiContext,
		Configuration configuration,
		JobID jobId,
		int maxNumberOfCheckpointsToRetain,
		Executor executor) throws Exception {

		checkNotNull(configuration, "Configuration");

		String checkpointsPath = concatPath(
			normalizePath(configuration.getString(KubernetesHighAvailabilityOptions.HA_KUBERNETES_CHECKPOINTS_PATH)),
			KubernetesSubmittedJobGraphStore.getPathForJob(jobId));

		RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage =
			ZooKeeperUtils.createFileSystemStateStorage(configuration, "completedCheckpoint");

		final KubernetesCompletedCheckpointStore kubernetesCompletedCheckpointStore =
			new KubernetesCompletedCheckpointStore(
				maxNumberOfCheckpointsToRetain,
				apiContext,
				checkpointsPath,
				stateStorage,
				executor);

		LOG.info("Initialized {} in '{}'.", KubernetesCompletedCheckpointStore.class.getSimpleName(), checkpointsPath);
		return kubernetesCompletedCheckpointStore;
	}

	public static KubernetesCheckpointIDCounter createCheckpointIDCounter(
		KubernetesApiContext apiContext,
		Configuration configuration,
		JobID jobId) {

		String checkpointIdCounterPath = concatPath(
			normalizePath(configuration.getString(KubernetesHighAvailabilityOptions.HA_KUBERNETES_CHECKPOINT_COUNTER_PATH)),
			KubernetesSubmittedJobGraphStore.getPathForJob(jobId));

		return new KubernetesCheckpointIDCounter(apiContext, checkpointIdCounterPath);
	}

	public static KubernetesLeaderRetrievalService createLeaderRetrievalService(
		final KubernetesApiContext apiContext,
		final Configuration configuration,
		final String pathSuffix) {

		String leaderPath = concatPath(
			normalizePath(configuration.getString(KubernetesHighAvailabilityOptions.HA_KUBERNETES_LEADER_PATH)),
			pathSuffix);
		return new KubernetesLeaderRetrievalService(apiContext, leaderPath);
	}

	public static KubernetesLeaderElectionService createLeaderElectionService(
		final KubernetesApiContext apiContext,
		final Configuration configuration,
		final String pathSuffix) {

		final String latchPath = concatPath(
			normalizePath(configuration.getString(KubernetesHighAvailabilityOptions.HA_KUBERNETES_LATCH_PATH)),
			pathSuffix);
		final String leaderPath = concatPath(
			normalizePath(configuration.getString(KubernetesHighAvailabilityOptions.HA_KUBERNETES_LEADER_PATH)),
			pathSuffix);

		return new KubernetesLeaderElectionService(apiContext, latchPath, leaderPath);
	}

	public static KubernetesSubmittedJobGraphStore createSubmittedJobGraphStore(
		KubernetesApiContext apiContext,
		Configuration configuration) throws Exception {

		checkNotNull(configuration, "Configuration");

		RetrievableStateStorageHelper<SubmittedJobGraph> stateStorage =
			ZooKeeperUtils.createFileSystemStateStorage(configuration, "submittedJobGraph");

		// Kubernetes submitted jobs root dir
		String kubernetesSubmittedJobsPath =
			normalizePath(configuration.getString(KubernetesHighAvailabilityOptions.HA_KUBERNETES_JOBGRAPHS_PATH));

		return new KubernetesSubmittedJobGraphStore(apiContext, kubernetesSubmittedJobsPath, stateStorage);
	}

	/**
	 * Validates path name since k8s only accepts path in the format of DNS-1123 subdomain.
	 * @param path
	 * @return
	 */
	public static String checkPath(String path) {
		checkNotNull(path);
		if (pathPattern.matcher(path).matches()) {
			return path;
		}
		throw new IllegalArgumentException("The path should in the format of DNS-1123 subdomain, " + path);
	}

	public static String normalizePath(String path) {
		checkNotNull(path);
		if (pathPattern.matcher(path).matches()) {
			return path;
		}

		// Accepted characters are (1) 0-9 (2) a-z (3) '.' .
		String[] parts = path.toLowerCase().replace('/', '.').replace("-", "")
			.replace("_", "").split("\\.");
		StringBuilder stringBuilder = new StringBuilder();
		for (String part : parts) {
			if (part != null) {
				if (stringBuilder.length() > 0) {
					stringBuilder.append('.');
				}
				stringBuilder.append(part);
			}
		}
		String normalizedPath = stringBuilder.toString();
		if (pathPattern.matcher(normalizedPath).matches()) {
			return normalizedPath;
		} else {
			throw  new IllegalArgumentException("Failed to normalize path, original path "
				+ path + ", normalized path:" + normalizedPath);
		}
	}

	public static String concatPath(String prefix, String suffix) {
		checkNotNull(prefix);
		checkNotNull(suffix);

		String str;
		if (prefix.isEmpty() || suffix.isEmpty() || (prefix.endsWith(".") ^ suffix.startsWith("."))) {
			str = prefix + suffix;
		} else if (prefix.endsWith(".")) {
			str = prefix + suffix.substring(1);
		} else {
			str = prefix + "." + suffix;
		}
		return checkPath(str);
	}

	public static LeaderElector createLeaderElector(KubernetesApiContext apiContext, String leaderPath) {
		ConfigMapLock lock = new ConfigMapLock(apiContext.getCoreV1Api(), apiContext.getNamespace(),
			KubernetesUtils.concatPath(apiContext.getConfigMapNamePrefix(), leaderPath), UUID.randomUUID().toString(),
			apiContext.getOwnerReference(),
			Collections.singletonMap(CONFIG_MAP_LABEL_KEY, apiContext.getOwnerReference().getUid()));
		LeaderElectionConfig leaderElectionConfig = new LeaderElectionConfig(lock,
			apiContext.getLeaderElectionLeaseDuration(), apiContext.getLeaderElectionRenewDeadline(),
			apiContext.getLeaderElectionRetryPeriod());
		return new LeaderElector(leaderElectionConfig);
	}

	public static Watch createConfigMapWatch(
		KubernetesApiContext apiContext, String configMapName) throws ApiException {

		return Watch.createWatch(
			apiContext.getApiClient(),
			apiContext.getCoreV1Api().listNamespacedConfigMapCall(
				apiContext.getNamespace(),
				null,
				null,
				"metadata.name=" + KubernetesUtils.concatPath(apiContext.getConfigMapNamePrefix(), configMapName),
				null,
				100,
				null,
				null,
				Boolean.TRUE,
				null, // Don't really need this callback
				null // Don't really need this callback
			),
			new TypeToken<Watch.Response<V1ConfigMap>>() {}.getType());
	}

	public static V1ConfigMap buildConfigMapWithBinaryData(
		KubernetesApiContext apiContext,
		String name,
		byte[] data,
		V1OwnerReference ownerReference,
		String resourceVersion) {

		return (new V1ConfigMapBuilder())
			.withApiVersion(API_VERSION)
			.withKind(CONFIG_MAP_KIND)
			.withNewMetadata()
			.withNamespace(apiContext.getNamespace())
			.withName(KubernetesUtils.concatPath(apiContext.getConfigMapNamePrefix(), name))
			.withOwnerReferences(ownerReference != null ? Collections.singletonList(ownerReference) : Collections.<V1OwnerReference>emptyList())
			.withResourceVersion(resourceVersion)
			.withLabels(Collections.singletonMap(CONFIG_MAP_LABEL_KEY, apiContext.getOwnerReference().getUid()))
			.endMetadata()
			.withBinaryData(Collections.singletonMap(DEFAULT_CONFIG_MAP_KEY, data))
			.build();
	}

	public static V1ConfigMap buildEphemeralConfigMapWithBinaryData(
		KubernetesApiContext apiContext, String name, byte[] data) {
		return buildConfigMapWithBinaryData(apiContext, name, data, apiContext.getOwnerReference(), null);
	}

	public static V1ConfigMap buildConfigMapWithBinaryData(
		KubernetesApiContext apiContext, String name, byte[] data) {
		return buildConfigMapWithBinaryData(apiContext, name, data, null, null);
	}

	public static V1ConfigMap buildConfigMapWithBinaryData(
		KubernetesApiContext apiContext, String name, byte[] data, String resourceVersion) {
		return buildConfigMapWithBinaryData(apiContext, name, data, null, resourceVersion);
	}

	public static Map<String, byte[]> getAllBinaryDataFromConfigMap(
		KubernetesApiContext apiContext, String name) throws ApiException {

		V1ConfigMap configMap = getConfigMap(apiContext, name);
		return configMap != null ? configMap.getBinaryData() : null;
	}

	public static byte[] getBinaryDataFromConfigMap(V1ConfigMap configMap) {
		if (configMap == null) {
			return null;
		}
		Map<String, byte[]> allBinaryData = configMap.getBinaryData();
		return allBinaryData != null ? allBinaryData.getOrDefault(DEFAULT_CONFIG_MAP_KEY, null) : null;
	}

	public static byte[] getBinaryDataFromConfigMap(
		KubernetesApiContext apiContext, String name, String key) throws ApiException {
		Map<String, byte[]> allBinaryData = getAllBinaryDataFromConfigMap(apiContext, name);
		return allBinaryData != null ? allBinaryData.getOrDefault(key, null) : null;
	}

	public static byte[] getBinaryDataFromConfigMap(KubernetesApiContext apiContext, String name) throws ApiException {
		return getBinaryDataFromConfigMap(apiContext, name, DEFAULT_CONFIG_MAP_KEY);
	}

	public static V1ConfigMap createConfigMap(KubernetesApiContext apiContext, V1ConfigMap configMap) throws ApiException {
		return apiContext.getCoreV1Api().createNamespacedConfigMap(
			apiContext.getNamespace(), configMap, null, null, null);
	}

	public static V1ConfigMap replaceConfigMap(KubernetesApiContext apiContext, V1ConfigMap configMap) throws ApiException {
		return apiContext.getCoreV1Api().replaceNamespacedConfigMap(
			configMap.getMetadata().getName(), apiContext.getNamespace(), configMap, null, null, null);
	}

	public static V1ConfigMap getConfigMap(KubernetesApiContext apiContext, String name) throws ApiException {
		return getConfigMap(apiContext, name, Boolean.TRUE);
	}

	public static V1ConfigMap getConfigMap(KubernetesApiContext apiContext, String name, Boolean withPrefix) throws ApiException {
		try {
			return apiContext.getCoreV1Api().readNamespacedConfigMap(
				withPrefix ? KubernetesUtils.concatPath(apiContext.getConfigMapNamePrefix(), name) : name,
				apiContext.getNamespace(),
				null,
				Boolean.TRUE,
				Boolean.TRUE);
		} catch (ApiException e) {
			String response = e.getResponseBody();
			if (response.contains("NotFound")) {
				// Such ConfigMap doesn't exist.
				return null;
			} else {
				throw e;
			}
		}

		/* Other way to get ConfigMap, which is heavier than readNamespacedConfigMap. */
//		V1ConfigMapList configMapList = apiContext.getCoreV1Api().listNamespacedConfigMap(
//			apiContext.getNamespace(),
//			null,
//			null,
//			"metadata.name=" + KubernetesUtils.concatPath(apiContext.getConfigMapNamePrefix(), name),
//			null,
//			100,
//			null,
//			null,
//			Boolean.FALSE);
//		if (configMapList.getItems().size() > 0) {
//			return configMapList.getItems().get(0);
//		} else {
//			return null;
//		}
	}

	public static V1ConfigMap createConfigMapOrUpdateBinaryData(
		KubernetesApiContext apiContext, V1ConfigMap configMap) throws ApiException {

		V1ConfigMap currConfigMap = getConfigMap(apiContext, configMap.getMetadata().getName(), Boolean.FALSE);
		if (currConfigMap != null) {
			configMap.setMetadata(currConfigMap.getMetadata());
			return apiContext.getCoreV1Api().replaceNamespacedConfigMap(configMap.getMetadata().getName(),
				apiContext.getNamespace(), configMap, null, null, null);
		} else {
			return apiContext.getCoreV1Api().createNamespacedConfigMap(
				apiContext.getNamespace(), configMap, null, null, null);
		}
	}

	public static void createConfigMapOrAddKeyValue(
		KubernetesApiContext apiContext, String name, String key, byte[] value) throws ApiException {

		V1ConfigMap currConfigMap = getConfigMap(apiContext, name);
		if (currConfigMap != null) {
			Map<String, byte[]> data = currConfigMap.getBinaryData() != null ? currConfigMap.getBinaryData() : new HashMap<>();
			data.put(key, value);
			V1ConfigMap configMap = (new V1ConfigMapBuilder())
				.withApiVersion(API_VERSION)
				.withKind(CONFIG_MAP_KIND)
				.withMetadata(currConfigMap.getMetadata())
				.withBinaryData(data)
				.build();
			apiContext.getCoreV1Api().replaceNamespacedConfigMap(configMap.getMetadata().getName(),
				apiContext.getNamespace(), configMap, null, null, null);
		} else {
			V1ConfigMap configMap = (new V1ConfigMapBuilder())
				.withApiVersion(API_VERSION)
				.withKind(CONFIG_MAP_KIND)
				.withNewMetadata()
				.withNamespace(apiContext.getNamespace())
				.withName(KubernetesUtils.concatPath(apiContext.getConfigMapNamePrefix(), name))
				.withLabels(Collections.singletonMap(CONFIG_MAP_LABEL_KEY, apiContext.getOwnerReference().getUid()))
				.endMetadata()
				.withBinaryData(Collections.singletonMap(key, value))
				.build();
			apiContext.getCoreV1Api().createNamespacedConfigMap(
				apiContext.getNamespace(), configMap, null, null, null);
		}
	}

	public static void updateConfigMapToRemoveKey(
		KubernetesApiContext apiContext, String name, String key) throws ApiException {

		V1ConfigMap currConfigMap = getConfigMap(apiContext, name);
		if (currConfigMap != null) {
			Map<String, byte[]> data = currConfigMap.getBinaryData() != null ? currConfigMap.getBinaryData() : new HashMap<>();
			data.remove(key);
			V1ConfigMap configMap = (new V1ConfigMapBuilder())
				.withApiVersion(API_VERSION)
				.withKind(CONFIG_MAP_KIND)
				.withMetadata(currConfigMap.getMetadata())
				.withBinaryData(data)
				.build();
			apiContext.getCoreV1Api().replaceNamespacedConfigMap(configMap.getMetadata().getName(),
				apiContext.getNamespace(), configMap, null, null, null);
		}
	}

	public static void deleteConfigMap(KubernetesApiContext apiContext, String name) throws ApiException {
		String configMapName = KubernetesUtils.concatPath(apiContext.getConfigMapNamePrefix(), name);
		try {
			V1Status status = apiContext.getCoreV1Api().deleteNamespacedConfigMap(
				configMapName, apiContext.getNamespace(), null, null, null, 0, Boolean.FALSE, null);
			if (!"Success".equals(status.getStatus())) {
				throw new ApiException("Fail to delete ConfigMap [" + configMapName + "], status " + status.getStatus() +
					", message: " + status.getMessage());
			}
		} catch (ApiException e) {
			String response = e.getResponseBody();
			if (response.contains("NotFound")) {
				return;
			} else {
				throw e;
			}
		}
	}

	public static void cleanUpConfigMaps(KubernetesApiContext apiContext) throws ApiException {
		// V1Status.getStatus gets null, so don't check
		apiContext.getCoreV1Api().deleteCollectionNamespacedConfigMap(
			apiContext.getNamespace(),
			null,
			null,
			null,
			CONFIG_MAP_LABEL_KEY + "=" + apiContext.getOwnerReference().getUid(),
			100,
			null,
			0, // TODO: need timeout setting ?
			Boolean.FALSE
		);
	}
}
