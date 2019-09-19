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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.util.KubernetesUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.util.Watch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Objects;
import java.util.UUID;

/**
 * TODO: add doc.
 */
public class KubernetesLeaderRetrievalService implements LeaderRetrievalService {
	private static final Logger LOG = LoggerFactory.getLogger(KubernetesLeaderRetrievalService.class);

	private final KubernetesApiContext apiContext;

	private final String retrievalPath;

	private Watch<V1ConfigMap> watch;

	/** Listener which will be notified about leader changes. */
	private volatile LeaderRetrievalListener leaderListener;

	private final Object lock = new Object();

	private volatile boolean running;

	private Thread watchThread;

	private String lastLeaderAddress;

	private UUID lastLeaderSessionID;

	public KubernetesLeaderRetrievalService(KubernetesApiContext apiContext, String retrievalPath) {
		this.apiContext = Preconditions.checkNotNull(apiContext);
		this.retrievalPath = KubernetesUtils.checkPath(retrievalPath);

		this.leaderListener = null;
		this.lastLeaderAddress = null;
		this.lastLeaderSessionID = null;
		this.running = false;
	}

	@Override
	public void start(LeaderRetrievalListener listener) throws Exception {
		Preconditions.checkNotNull(listener, "Listener must not be null.");
		Preconditions.checkState(leaderListener == null, "KubernetesLeaderRetrievalService can " +
			"only be started once.");

		LOG.info("Starting KubernetesLeaderRetrievalService {}.", retrievalPath);

		synchronized (lock) {
			running = true;
			leaderListener = listener;
			// TODO: unify thread group ?
			watchThread = new Thread(new WatchRunnable());
			watchThread.start();
		}
	}

	@Override
	public void stop() throws Exception {
		LOG.info("Stopping KubernetesLeaderRetrievalService {}.", retrievalPath);

		synchronized (lock) {
			if (!running) {
				return;
			}
			running = false;
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

	private void handleConfigMapChanged(String responseType) {
		synchronized (lock) {
			if (running) {
				try {
					LOG.debug("Leader ConfigMap [{}] has been {}.", apiContext.getConfigMapNamePrefix(), responseType);

					String leaderAddress;
					UUID leaderSessionID;

					byte[] data = KubernetesUtils.getBinaryDataFromConfigMap(apiContext, retrievalPath);
					if (data == null || data.length == 0) {
						leaderAddress = null;
						leaderSessionID = null;
					} else {
						ByteArrayInputStream bais = new ByteArrayInputStream(data);
						ObjectInputStream ois = new ObjectInputStream(bais);

						leaderAddress = ois.readUTF();
						leaderSessionID = (UUID) ois.readObject();
					}

					if (!(Objects.equals(leaderAddress, lastLeaderAddress) &&
						Objects.equals(leaderSessionID, lastLeaderSessionID))) {
						LOG.debug(
							"New leader information: Leader={}, session ID={}.",
							leaderAddress,
							leaderSessionID);

						lastLeaderAddress = leaderAddress;
						lastLeaderSessionID = leaderSessionID;
						leaderListener.notifyLeaderAddress(leaderAddress, leaderSessionID);
					}
				} catch (Exception e) {
					leaderListener.handleError(new Exception("Could not handle ConfigMap added or modified event.", e));
				}
			} else {
				LOG.debug("Ignoring node change notification since the service has already been stopped.");
			}
		}
	}

	private class WatchRunnable implements Runnable {
		@Override
		public void run() {
			while (running) {
				try {
					watch = KubernetesUtils.createConfigMapWatch(apiContext, retrievalPath);
					for (Watch.Response<V1ConfigMap> response : watch) {
						switch (response.type) {
							case "ADDED":
							case "MODIFIED":
							case "DELETED":
								handleConfigMapChanged(response.type);
								break;
							default:
								leaderListener.handleError(
									new FlinkException("Unhandled error in KubernetesLeaderRetrievalService, "
										+ "ConfigMap watch response: " + response.type));
						}
					}
				} catch (ApiException e) {
					LOG.error("Caught Exception during watching ConfigMap [" + retrievalPath + "], exception: ", e);
				}
			}
		}
	}

}
