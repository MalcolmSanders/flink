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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.leaderretrieval.KubernetesLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.KubernetesUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.models.V1ConfigMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Leader election service for multiple JobManager. The leading JobManager is elected using
 * {@link LeaderElector}. The current leader's address as well as its leader session ID is published via
 * {@link V1ConfigMap}.
 */
public class KubernetesLeaderElectionService implements LeaderElectionService, LeaderRetrievalListener {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesLeaderElectionService.class);

	private final Object lock = new Object();

	private final KubernetesApiContext apiContext;

	private final LeaderElector leaderElector;

	private final AtomicBoolean hasLeadership;

	private final LeaderRetrievalService retrievalService;

	private final String latchPath;

	private final String leaderPath;

	private volatile UUID issuedLeaderSessionID;

	private volatile UUID confirmedLeaderSessionID;

	/** The leader contender which applies for leadership. */
	private volatile LeaderContender leaderContender;

	private volatile boolean running;

	private Thread leaderElectionThread;

	public KubernetesLeaderElectionService(KubernetesApiContext apiContext, String latchPath, String leaderPath) {
		this.apiContext = Preconditions.checkNotNull(apiContext);
		this.latchPath = KubernetesUtils.checkPath(latchPath);
		this.leaderPath = KubernetesUtils.checkPath(leaderPath);

		this.retrievalService = new KubernetesLeaderRetrievalService(apiContext, this.leaderPath);
		this.leaderElector = KubernetesUtils.createLeaderElector(apiContext, this.latchPath);
		this.hasLeadership = new AtomicBoolean(false);

		this.issuedLeaderSessionID = null;
		this.confirmedLeaderSessionID = null;
		this.leaderContender = null;

		this.running = false;
	}

	/**
	 * Returns the current leader session ID or null, if the contender is not the leader.
	 *
	 * @return The last leader session ID or null, if the contender is not the leader
	 */
	public UUID getLeaderSessionID() {
		return confirmedLeaderSessionID;
	}

	@Override
	public void start(LeaderContender contender) throws Exception {
		Preconditions.checkNotNull(contender, "Contender must not be null.");
		Preconditions.checkState(leaderContender == null, "Contender was already set.");

		LOG.info("Starting KubernetesLeaderElectionService {}.", this);

		synchronized (lock) {
			running = true;

			leaderContender = contender;

			leaderElectionThread = new Thread(new LeaderElectionRunnable());
			leaderElectionThread.start();

			retrievalService.start(this);
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (lock) {
			if (!running) {
				return;
			}

			running = false;
			confirmedLeaderSessionID = null;
			issuedLeaderSessionID = null;
		}

		LOG.info("Stopping KubernetesLeaderElectionService {}.", this);

		Exception exception = null;

		try {
			retrievalService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			leaderElectionThread.interrupt();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			// TODO: need to throw this exception ?
			LOG.error("Could not properly stop the KubernetesLeaderElectionService, exception:", exception);
		}
	}

	@Override
	public void confirmLeaderSessionID(UUID leaderSessionID) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(
				"Confirm leader session ID {} for leader {}.",
				leaderSessionID,
				leaderContender.getAddress());
		}

		Preconditions.checkNotNull(leaderSessionID);

		if (hasLeadership.get()) {
			// check if this is an old confirmation call
			synchronized (lock) {
				if (running) {
					if (leaderSessionID.equals(this.issuedLeaderSessionID)) {
						confirmedLeaderSessionID = leaderSessionID;
						writeLeaderInformation(confirmedLeaderSessionID);
					}
				} else {
					LOG.debug("Ignoring the leader session Id {} confirmation, since the " +
						"KubernetesLeaderElectionService has already been stopped.", leaderSessionID);
				}
			}
		} else {
			LOG.warn("The leader session ID {} was confirmed even though the " +
				"corresponding JobManager was not elected as the leader.", leaderSessionID);
		}
	}

	@Override
	public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
		return hasLeadership.get() && leaderSessionId.equals(issuedLeaderSessionID);
	}

	@Override
	public void notifyLeaderAddress(@Nullable String leaderAddress, @Nullable UUID leaderSessionID) {
		try {
			// leaderSessionID is null if the leader contender has not yet confirmed the session ID
			if (hasLeadership.get()) {
				synchronized (lock) {
					if (running) {
						if (LOG.isDebugEnabled()) {
							LOG.debug(
								"Leader node changed while {} is the leader with session ID {}.",
								leaderContender.getAddress(),
								confirmedLeaderSessionID);
						}

						if (confirmedLeaderSessionID != null) {

							if (leaderAddress == null || leaderSessionID == null) {
								if (LOG.isDebugEnabled()) {
									LOG.debug(
										"Writing leader information into empty node by {}.",
										leaderContender.getAddress());
								}
								writeLeaderInformation(confirmedLeaderSessionID);
							} else if (!leaderAddress.equals(this.leaderContender.getAddress()) ||
								(leaderSessionID == null || !leaderSessionID.equals(confirmedLeaderSessionID))) {
								// the data field does not correspond to the expected leader information
								if (LOG.isDebugEnabled()) {
									LOG.debug("Correcting leader information by {}.", leaderContender.getAddress());
								}
								writeLeaderInformation(confirmedLeaderSessionID);
							}
						}
					} else {
						LOG.debug("Ignoring node change notification since the service has already been stopped.");
					}
				}
			}
		} catch (Exception e) {
			leaderContender.handleError(new Exception("Could not handle node changed event.", e));
		}
	}

	@Override
	public void handleError(Exception exception) {
		leaderContender.handleError(new FlinkException("Unhandled error in KubernetesLeaderElectionService", exception));
	}

	private class LeaderElectionRunnable implements Runnable {
		@Override
		public void run() {
			try {
				while (running) {
					leaderElector.run(
						new Runnable() {
							@Override
							public void run() {
								isLeader();
							}
						},
						new Runnable() {
							@Override
							public void run() {
								notLeader();
							}
						});
					if (running) {
						Thread.sleep(apiContext.getLeaderElectionRetryPeriod().toMillis());
					}
				}
			} catch (InterruptedException e) {
				//
			}
		}
	}

	public void isLeader() {
		synchronized (lock) {
			if (running) {
				if (!hasLeadership.compareAndSet(false, true)) {
					LOG.debug("Leadership has already been granted.");
				}

				issuedLeaderSessionID = UUID.randomUUID();
				confirmedLeaderSessionID = null;

				if (LOG.isDebugEnabled()) {
					LOG.debug(
						"Grant leadership to contender {} with session ID {}.",
						leaderContender.getAddress(),
						issuedLeaderSessionID);
				}

				leaderContender.grantLeadership(issuedLeaderSessionID);
			} else {
				LOG.debug("Ignoring the grant leadership notification since the service has " +
					"already been stopped.");
			}
		}
	}

	public void notLeader() {
		synchronized (lock) {
			if (running) {
				if (!hasLeadership.compareAndSet(true, false)) {
					LOG.debug("Ignoring the revoke leadership notification since it hasn't got leadership.");
				}

				issuedLeaderSessionID = null;
				confirmedLeaderSessionID = null;

				if (LOG.isDebugEnabled()) {
					LOG.debug("Revoke leadership of {}.", leaderContender.getAddress());
				}

				leaderContender.revokeLeadership();
			} else {
				LOG.debug("Ignoring the revoke leadership notification since the service " +
					"has already been stopped.");
			}
		}
	}

	/**
	 * Writes the current leader's address as well the given leader session ID to ZooKeeper.
	 *
	 * @param leaderSessionID Leader session ID which is written to ZooKeeper
	 */
	protected void writeLeaderInformation(UUID leaderSessionID) {
		// this method does not have to be synchronized because the curator framework client
		// is thread-safe
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"Write leader information: Leader={}, session ID={}.",
					leaderContender.getAddress(),
					leaderSessionID);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(leaderContender.getAddress());
			oos.writeObject(leaderSessionID);

			oos.close();

			V1ConfigMap configMap = KubernetesUtils.buildEphemeralConfigMapWithBinaryData(
				apiContext, leaderPath, baos.toByteArray());

			boolean dataWritten = false;

			while (running && !dataWritten && hasLeadership.get()) {
				try {
					KubernetesUtils.createConfigMapOrUpdateBinaryData(apiContext, configMap);
					dataWritten = true;
				} catch (ApiException e) {
					LOG.error("Got exception probably due to race condition, retry again. Exception: ", e);
				}
			}

			if (dataWritten && LOG.isDebugEnabled()) {
				LOG.debug(
					"Successfully wrote leader information: Leader={}, session ID={}.",
					leaderContender.getAddress(),
					leaderSessionID);
			}
		} catch (Exception e) {
			leaderContender.handleError(
				new Exception("Could not write leader address and leader session ID to Kubernetes.", e));
		}
	}
}
