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

import org.apache.flink.runtime.util.KubernetesUtils;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ConfigMapBuilder;
import io.kubernetes.client.models.V1ConfigMapList;
import io.kubernetes.client.models.V1Namespace;
import io.kubernetes.client.models.V1NamespaceBuilder;
import io.kubernetes.client.models.V1NamespaceList;
import io.kubernetes.client.models.V1OwnerReference;
import io.kubernetes.client.models.V1OwnerReferenceBuilder;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Test Utils for Kubernetes related cases.
 */
public class KubernetesTestUtils {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesTestUtils.class);

	private static final String NAMESPACE_PREFIX = "flink-it-case-ns-";

	private static final String OWNER_PREFIX = "owner-";

	private static final String CONFIG_MAP_PREFIX = "config-map-prefix-";

	public static boolean isKubernetesAvailable() throws Exception {
		ApiClient client = Config.defaultClient();
		client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
		Configuration.setDefaultApiClient(client);
		CoreV1Api api = new CoreV1Api();

		try {
			V1NamespaceList namespaceList = api.listNamespace(null, null, null, null,
				1, null, 0, Boolean.FALSE);
			// double check since a valid kubernetes cluster should have at least one namespace.
			return namespaceList.getItems().size() > 0;
		} catch (Exception e) {
			return false;
		}
	}

	public static KubernetesApiContext createApiContext() throws Exception {
		Random rand = new Random();
		ApiClient client = Config.defaultClient();
		client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
		Configuration.setDefaultApiClient(client);
		CoreV1Api api = new CoreV1Api();
		String namespace = NAMESPACE_PREFIX + rand.nextLong();
		String ownerName = OWNER_PREFIX + rand.nextLong();
		String configMapPrefix = CONFIG_MAP_PREFIX + rand.nextLong();

		try {
			// ensure namespace doesn't exist.
			api.deleteNamespace(namespace, null, null, null, 0, Boolean.FALSE, null);
		} catch (ApiException e) {
		}

		V1Namespace namespaceBody = (new V1NamespaceBuilder())
			.withApiVersion("v1")
			.withKind("Namespace")
			.withNewMetadata()
			.withName(namespace)
			.endMetadata()
			.build();

		api.createNamespace(namespaceBody, null, null, null);

		try {
			// ensures owner config map doesn't exist.
			api.deleteNamespacedConfigMap(ownerName, namespace, null, null, null, 0,
				Boolean.FALSE, null);
		} catch (ApiException e) {
		}

		// creates config map and gets its uid.
		V1ConfigMap ownerConfigMap = (new V1ConfigMapBuilder())
			.withApiVersion(KubernetesUtils.API_VERSION)
			.withKind(KubernetesUtils.CONFIG_MAP_KIND)
			.withNewMetadata()
			.withName(ownerName)
			.withNamespace(namespace)
			.endMetadata()
			.withData(Collections.singletonMap("key", "value"))
			.build();
		V1ConfigMap response = api.createNamespacedConfigMap(namespace, ownerConfigMap, null, null, null);

		// constructs its owner reference.
		V1OwnerReference ownerReference = new V1OwnerReferenceBuilder()
			.withApiVersion(KubernetesUtils.API_VERSION)
			.withKind(KubernetesUtils.CONFIG_MAP_KIND)
			.withName(ownerName)
			.withUid(response.getMetadata().getUid())
			.build();

		return new KubernetesApiContext(client, api, namespace, configMapPrefix,
			Duration.ofMillis(500), null, Duration.ofMillis(250), ownerReference);
	}

	public static void deleteAllConfigMaps(KubernetesApiContext apiContext) {
		try {
			V1ConfigMapList configMapList = apiContext.getCoreV1Api().listNamespacedConfigMap(
				apiContext.getNamespace(), null, null, null, null, 100,
				null, 0, Boolean.FALSE);
			for (V1ConfigMap configMap : configMapList.getItems()) {
				if (configMap.getMetadata().getName().startsWith(CONFIG_MAP_PREFIX)) {
					apiContext.getCoreV1Api().deleteNamespacedConfigMap(configMap.getMetadata().getName(),
						apiContext.getNamespace(), null, null, null, 0, Boolean.FALSE, null);
				}
			}
		} catch (Exception e) {
			LOG.warn("Failed to delete all config maps, namespace: " + apiContext.getNamespace() + ", exception: ", e);
		}
	}

	public static void clearEnvironment(KubernetesApiContext apiContext) {
		try {
			apiContext.getCoreV1Api().deleteNamespace(apiContext.getNamespace(), null, null, null,
				0, Boolean.FALSE, null);
		} catch (Exception e) {
		}
	}
}
