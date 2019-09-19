/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.kubernetes;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.util.KubernetesUtils;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import io.kubernetes.client.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the
 * returned state handle to Kubernetes.
 */
public class KubernetesStateHandleStore<T extends Serializable> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateHandleStore.class);

	private final KubernetesApiContext apiContext;

	private final RetrievableStateStorageHelper<T> storage;

	private final String configMapName;

	public KubernetesStateHandleStore(
		KubernetesApiContext apiContext,
		RetrievableStateStorageHelper<T> storage,
		String configMapName) {

		this.apiContext = checkNotNull(apiContext);
		this.storage = checkNotNull(storage);
		this.configMapName = KubernetesUtils.checkPath(configMapName);
	}

	public RetrievableStateHandle<T> add(String key, T state) throws Exception{
		checkNotNull(key, "Key for state in ConfigMap's binary data");
		checkNotNull(state, "State");

		RetrievableStateHandle<T> stateHandle = storage.store(state);

		boolean success = false;

		try {
			// Serialize the state handle. This writes the state to the backend.
			byte[] serializedStoreHandle = InstantiationUtil.serializeObject(stateHandle);
			KubernetesUtils.createConfigMapOrAddKeyValue(apiContext, configMapName, key, serializedStoreHandle);
			success = true;
			return stateHandle;
		} catch (Exception e) {
			throw new FlinkRuntimeException("Fail to create ConfigMap or add key value", e);
		} finally {
			if (!success) {
				// Cleanup the state handle if it was not written to kubernetes.
				if (stateHandle != null) {
					stateHandle.discardState();
				}
			}
		}
	}

	public RetrievableStateHandle<T> get(String key) throws Exception {
		checkNotNull(key, "Key for state in ConfigMap's binary data");

		byte[] data;
		try {
			data = KubernetesUtils.getBinaryDataFromConfigMap(apiContext, configMapName, key);
		} catch (ApiException e) {
			throw new Exception("Failed to retrieve state handle data under " + key + " from kubernetes.", e);
		}

		if (data == null) {
			throw new Exception("Key [" + key + "] for state doesn't exist.");
		}

		try {
			RetrievableStateHandle<T> stateHandle = InstantiationUtil.deserializeObject(
				data,
				Thread.currentThread().getContextClassLoader());
			return stateHandle;
		} catch (IOException | ClassNotFoundException e) {
			throw new IOException("Failed to deserialize state handle from ZooKeeper data from " + key + '.', e);
		}
	}

	public boolean removeAndDiscardState(String key) throws Exception {
		checkNotNull(key, "Key for state in ConfigMap's binary data");

		RetrievableStateHandle<T> stateHandle = null;
		try {
			stateHandle = get(key);
		} catch (Exception e) {
			LOG.warn("Could not retrieve the state handle for key [" + key + "].", e);
		}

		try {
			KubernetesUtils.updateConfigMapToRemoveKey(apiContext, configMapName, key);
		} catch (Exception e) {
			LOG.warn("Fail to remove key [" + key + "] from ConfigMap [" + configMapName + "].", e);
			return false;
		}

		if (stateHandle != null) {
			stateHandle.discardState();
		}

		return true;
	}

	public Collection<String> getAllKeysForState() throws Exception {
		try {
			Map<String, byte[]> allData = KubernetesUtils.getAllBinaryDataFromConfigMap(apiContext, configMapName);
			return allData != null ? allData.keySet() : Collections.EMPTY_SET;
		} catch (ApiException e) {
			throw new Exception("Failed to retrieve state handle data from " + apiContext.getConfigMapNamePrefix()
				+ "." + configMapName + " from kubernetes.", e);
		}
	}

	public List<Tuple2<RetrievableStateHandle<T>, String>> getAll() throws Exception {
		final List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();

		Map<String, byte[]> allData = null;
		try {
			allData = KubernetesUtils.getAllBinaryDataFromConfigMap(apiContext, configMapName);
		} catch (ApiException e) {
			throw new Exception("Failed to retrieve state handle data from " + apiContext.getConfigMapNamePrefix()
				+ "." + configMapName + " from kubernetes.", e);
		}

		if (allData == null) {
			return stateHandles;
		}

		for (Map.Entry<String, byte[]> entry : allData.entrySet()) {
			try {
				RetrievableStateHandle<T> stateHandle = InstantiationUtil.deserializeObject(
					entry.getValue(),
					Thread.currentThread().getContextClassLoader());
				stateHandles.add(new Tuple2<>(stateHandle, entry.getKey()));
			} catch (IOException | ClassNotFoundException e) {
				LOG.warn("Failed to deserialize state handle from Kubernetes from [" +
					apiContext.getConfigMapNamePrefix() + "." + configMapName + ". Ignore this key: " +
					entry.getKey() + '.', e);
			}
		}

		return stateHandles;
	}

	public void deleteConfigMap() throws ApiException {
		LOG.info("Delete ConfigMap {}.{} from Kubernetes.", apiContext.getConfigMapNamePrefix(), configMapName);
		KubernetesUtils.deleteConfigMap(apiContext, configMapName);
	}
}
