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

import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.extended.leaderelection.LeaderElectionRecord;
import io.kubernetes.client.extended.leaderelection.Lock;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1OwnerReference;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link io.kubernetes.client.extended.leaderelection.Lock} based on
 * {@link io.kubernetes.client.extended.leaderelection.resourcelock.ConfigMapLock}.
 * This implementation enables setting {@link V1OwnerReference} and labels during lock creation.
 * See https://github.com/kubernetes-client/java/issues/704 for detailed discussion.
 */
public class ConfigMapLock implements Lock {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigMapLock.class);

	private static final String LeaderElectionRecordAnnotationKey = "control-plane.alpha.kubernetes.io/leader";

	// Namespace and name describes the configMap object
	// that the LeaderElector will attempt to lead.
	private final String namespace;
	private final String name;
	private final String identity;
	private final V1OwnerReference ownerReference;
	private final Map<String, String> labels;

	private CoreV1Api coreV1Client;

	private AtomicReference<V1ConfigMap> configMapRefer = new AtomicReference<>(null);

	public ConfigMapLock(CoreV1Api api, String namespace, String name, String identity,
		V1OwnerReference ownerReference) {
		this(api, namespace, name, identity, ownerReference, Collections.emptyMap());
	}

	public ConfigMapLock(CoreV1Api api, String namespace, String name, String identity,
		V1OwnerReference ownerReference, Map<String, String> labels) {
		this.coreV1Client = checkNotNull(api);
		this.namespace = checkNotNull(namespace);
		this.name = checkNotNull(name);
		this.identity = checkNotNull(identity);
		this.ownerReference = checkNotNull(ownerReference);
		this.labels = checkNotNull(labels);
	}

	@Override
	public LeaderElectionRecord get() throws ApiException {
		V1ConfigMap configMap = coreV1Client.readNamespacedConfigMap(name, namespace, null, null, null);
		Map<String, String> annotations = configMap.getMetadata().getAnnotations();
		if (annotations == null || annotations.isEmpty()) {
			configMap.getMetadata().setAnnotations(new HashMap<>());
		}

		String recordRawStringContent =
			configMap.getMetadata().getAnnotations().get(LeaderElectionRecordAnnotationKey);
		if (StringUtils.isEmpty(recordRawStringContent)) {
			return new LeaderElectionRecord();
		}
		LeaderElectionRecord record =
			coreV1Client
				.getApiClient()
				.getJSON()
				.deserialize(recordRawStringContent, LeaderElectionRecord.class);
		configMapRefer.set(configMap);
		return record;
	}

	@Override
	public boolean create(LeaderElectionRecord record) {
		try {
			V1ConfigMap configMap = new V1ConfigMap();
			V1ObjectMeta objectMeta = new V1ObjectMeta();
			objectMeta.setName(name);
			objectMeta.setNamespace(namespace);
			objectMeta.setOwnerReferences(Collections.singletonList(ownerReference));
			objectMeta.setLabels(labels);
			Map<String, String> annotations = new HashMap<>();
			annotations.put(
				LeaderElectionRecordAnnotationKey,
				coreV1Client.getApiClient().getJSON().serialize(record));
			objectMeta.setAnnotations(annotations);
			configMap.setMetadata(objectMeta);
			V1ConfigMap createdConfigMap =
				coreV1Client.createNamespacedConfigMap(namespace, configMap, null, null, null);
			configMapRefer.set(createdConfigMap);
			return true;
		} catch (Throwable t) {
			LOG.error("failed to create leader election record as {}", t.getMessage());
			return false;
		}
	}

	@Override
	public boolean update(LeaderElectionRecord record) {
		try {
			V1ConfigMap configMap = configMapRefer.get();
			configMap
				.getMetadata()
				.putAnnotationsItem(
					LeaderElectionRecordAnnotationKey,
					coreV1Client.getApiClient().getJSON().serialize(record));
			V1ConfigMap replacedConfigMap =
				coreV1Client.replaceNamespacedConfigMap(name, namespace, configMap, null, null, null);
			configMapRefer.set(replacedConfigMap);
			return true;
		} catch (Throwable t) {
			LOG.error("failed to update leader election record as {}", t.getMessage());
			return false;
		}
	}

	@Override
	public String identity() {
		return identity;
	}

	@Override
	public String describe() {
		return namespace + "/" + name;
	}
}
