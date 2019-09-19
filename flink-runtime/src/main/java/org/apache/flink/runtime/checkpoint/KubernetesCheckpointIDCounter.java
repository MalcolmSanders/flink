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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.util.KubernetesUtils;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.models.V1ConfigMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointIDCounter} instances for JobManagers running in {@link HighAvailabilityMode#KUBERNETES}.
 */
public class KubernetesCheckpointIDCounter implements CheckpointIDCounter {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesCheckpointIDCounter.class);

	private final KubernetesApiContext apiContext;

	private final String counterPath;

	private AtomicBoolean running;

	private final Object lock = new Object();

	private long lastCount;

	private String lastResourceVersion;

	public KubernetesCheckpointIDCounter(KubernetesApiContext apiContext, String counterPath) {
		this.apiContext = checkNotNull(apiContext);
		this.counterPath = checkNotNull(counterPath);
		this.running = new AtomicBoolean(false);
		this.lastCount = 1L;
		this.lastResourceVersion = "";
	}

	@Override
	public void start() throws Exception {
		running.compareAndSet(false, true);

		while (running.get()) {
			try {
				synchronized (lock) {
					V1ConfigMap prevConfigMap = KubernetesUtils.getConfigMap(apiContext, counterPath);
					if (prevConfigMap == null) {
						// Create ConfigMap for the first time.
						V1ConfigMap configMap = KubernetesUtils.buildConfigMapWithBinaryData(apiContext, counterPath, toBytes(lastCount));
						KubernetesUtils.createConfigMap(apiContext, configMap);
					} else {
						byte[] data = KubernetesUtils.getBinaryDataFromConfigMap(prevConfigMap);
						if (data == null) {
							throw new IllegalStateException("Data is corrupted in ConfigMap for KubernetesCheckpointIDCounter");
						}
						this.lastCount = fromBytes(data);
						this.lastResourceVersion = prevConfigMap.getMetadata().getResourceVersion();
					}
				}
				break;
			} catch (ApiException e) {
				LOG.warn("Failed to initialize KubernetesCheckpointIDCounter, retry again, exception: ", e);
			}
		}
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		running.compareAndSet(true, false);

		if (jobStatus.isGloballyTerminalState()) {
			LOG.info("Removing ConfigMap {} from Kubernetes", counterPath);
			try {
				KubernetesUtils.deleteConfigMap(apiContext, counterPath);
			} catch (Exception e) {
				LOG.warn("Failed to remove ConfigMap {}, exception: ", counterPath, e);
			}
		}
	}

	@Override
	public long getAndIncrement() throws Exception {
		while (running.get()) {
			try {
				synchronized (lock) {
					long nextCount = lastCount + 1;
					V1ConfigMap newConfigMap = KubernetesUtils.buildConfigMapWithBinaryData(
						apiContext, counterPath, toBytes(nextCount), lastResourceVersion);
					V1ConfigMap actualConfigMap = KubernetesUtils.replaceConfigMap(apiContext, newConfigMap);
					// Double check.
					byte[] data = KubernetesUtils.getBinaryDataFromConfigMap(actualConfigMap);
					if (data == null) {
						throw new IllegalStateException("Data is corrupted in ConfigMap for KubernetesCheckpointIDCounter");
					}
					long actualNextCount = fromBytes(data);
					if (nextCount != actualNextCount) {
						// Should never enter this branch.
						throw new IllegalStateException("Failed to update KubernetesCheckpointIDCounter, "
							+ "expected count: " + nextCount + ", actual count: " + actualNextCount);
					}
					lastCount = nextCount;
					lastResourceVersion = actualConfigMap.getMetadata().getResourceVersion();
					return nextCount - 1;
				}
			} catch (ApiException e) {
				// Probably get "Conflict" exception in case of race condition, retrieve the latest count and try again.
				LOG.warn("Failed to getAndIncrement KubernetesCheckpointIDCounter, retry again, exception: ", e);
				get();
			}
		}
		throw new IllegalStateException("Failed to increment KubernetesCheckpointIDCounter until shutdown.");
	}

	@Override
	public long get() {
		while (running.get()) {
			try {
				synchronized (lock) {
					V1ConfigMap currConfigMap = KubernetesUtils.getConfigMap(apiContext, counterPath);
					if (currConfigMap == null) {
						throw new IllegalStateException("Fail to retrieve KubernetesCheckpointIDCounter from ConfigMap");
					} else {
						byte[] data = KubernetesUtils.getBinaryDataFromConfigMap(currConfigMap);
						if (data == null) {
							throw new IllegalStateException("Data is corrupted in ConfigMap for KubernetesCheckpointIDCounter");
						}
						lastCount = fromBytes(data);
						lastResourceVersion = currConfigMap.getMetadata().getResourceVersion();
						return lastCount;
					}
				}
			} catch (ApiException e) {
				LOG.warn("Failed to retrieve KubernetesCheckpointIDCounter, retry again, exception: ", e);
			}
		}
		throw new IllegalStateException("Failed to retrieve KubernetesCheckpointIDCounter until shutdown.");
	}

	@Override
	public void setCount(long newId) throws Exception {
		while (running.get()) {
			try {
				synchronized (lock) {
					V1ConfigMap newConfigMap = KubernetesUtils.buildConfigMapWithBinaryData(
						apiContext, counterPath, toBytes(newId));
					V1ConfigMap actualConfigMap = KubernetesUtils.createConfigMapOrUpdateBinaryData(
						apiContext, newConfigMap);
					// Double check.
					byte[] data = KubernetesUtils.getBinaryDataFromConfigMap(actualConfigMap);
					if (data == null) {
						throw new IllegalStateException("Data is corrupted in ConfigMap for KubernetesCheckpointIDCounter");
					}
					long actualNextCount = fromBytes(data);
					if (newId != actualNextCount) {
						// Should never enter this branch.
						throw new IllegalStateException("Failed to update KubernetesCheckpointIDCounter, "
							+ "expected count: " + newId + ", actual count: " + actualNextCount);
					}
					lastCount = newId;
					lastResourceVersion = actualConfigMap.getMetadata().getResourceVersion();
					return;
				}
			} catch (ApiException e) {
				LOG.warn("Failed to setCount for KubernetesCheckpointIDCounter, retry again, exception: ", e);
			}
		}
	}

	static byte[] toBytes(long value) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putLong(value);
		return bytes;
	}

	private static long fromBytes(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getLong();
	}
}
