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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.kubernetes.KubernetesTestUtils;
import org.apache.flink.runtime.leaderretrieval.KubernetesLeaderRetrievalService;
import org.apache.flink.runtime.util.KubernetesUtils;
import org.apache.flink.util.TestLogger;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.models.V1ConfigMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@link KubernetesLeaderElectionService} and the {@link KubernetesLeaderRetrievalService}.
 */
public class KubernetesLeaderElectionTest extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(KubernetesLeaderElectionTest.class);

	private static final String LEADER_PATH = "leaderpath";

	private static KubernetesApiContext apiContext;

	private Configuration configuration;

	private static final String TEST_URL = "akka//user/jobmanager";

	private static final long timeout = 200L * 1000L;

	@Before
	public void before() throws Exception {
		Boolean isKubernetesAvailable = KubernetesTestUtils.isKubernetesAvailable();
		assumeTrue("Kubernetes environment is not available", isKubernetesAvailable);
		if (isKubernetesAvailable) {
			apiContext = KubernetesTestUtils.createApiContext();
		}

		configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_MODE, "kubernetes");
	}

	@After
	public void after() throws IOException {
		if (apiContext != null) {
			KubernetesTestUtils.clearEnvironment(apiContext);
			apiContext = null;
		}
	}

	/**
	 * Tests that the KubernetesLeaderElection/RetrievalService return both the correct URL.
	 */
	@Test
	public void testKubernetesLeaderElectionRetrieval() throws Exception {
		KubernetesLeaderElectionService leaderElectionService = null;
		KubernetesLeaderRetrievalService leaderRetrievalService = null;

		try {
			leaderElectionService = KubernetesUtils.createLeaderElectionService(apiContext, configuration, LEADER_PATH);
			leaderRetrievalService = KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, LEADER_PATH);

			TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
			TestingListener listener = new TestingListener();

			leaderElectionService.start(contender);
			leaderRetrievalService.start(listener);

			contender.waitForLeader(timeout);

			assertTrue(contender.isLeader());
			assertEquals(leaderElectionService.getLeaderSessionID(), contender.getLeaderSessionID());

			listener.waitForNewLeader(timeout);

			assertEquals(TEST_URL, listener.getAddress());
			assertEquals(leaderElectionService.getLeaderSessionID(), listener.getLeaderSessionID());

		} finally {
			if (leaderElectionService != null) {
				leaderElectionService.stop();
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}
		}
	}

	/**
	 * Tests repeatedly the reelection of still available LeaderContender. After a contender has
	 * been elected as the leader, it is removed. This forces the KubernetesLeaderElectionService
	 * to elect a new leader.
	 */
	@Test
	public void testKubernetesReelection() throws Exception {
		Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5L));

		int num = 10;

		KubernetesLeaderElectionService[] leaderElectionService = new KubernetesLeaderElectionService[num];
		TestingContender[] contenders = new TestingContender[num];
		KubernetesLeaderRetrievalService leaderRetrievalService = null;

		TestingListener listener = new TestingListener();

		try {
			leaderRetrievalService = KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, LEADER_PATH);

			LOG.debug("Start leader retrieval service for the TestingListener.");

			leaderRetrievalService.start(listener);

			for (int i = 0; i < num; i++) {
				leaderElectionService[i] = KubernetesUtils.createLeaderElectionService(apiContext, configuration, LEADER_PATH);
				contenders[i] = new TestingContender(TEST_URL + "_" + i, leaderElectionService[i]);

				LOG.debug("Start leader election service for contender #{}.", i);

				leaderElectionService[i].start(contenders[i]);
			}

			String pattern = TEST_URL + "_" + "(\\d+)";
			Pattern regex = Pattern.compile(pattern);

			int numberSeenLeaders = 0;

			while (deadline.hasTimeLeft() && numberSeenLeaders < num) {
				LOG.debug("Wait for new leader #{}.", numberSeenLeaders);
				String address = listener.waitForNewLeader(deadline.timeLeft().toMillis());

				Matcher m = regex.matcher(address);

				if (m.find()) {
					int index = Integer.parseInt(m.group(1));

					TestingContender contender = contenders[index];

					// check that the retrieval service has retrieved the correct leader
					if (address.equals(contender.getAddress()) && listener.getLeaderSessionID().equals(contender.getLeaderSessionID())) {
						// kill the election service of the leader
						LOG.debug("Stop leader election service of contender #{}.", numberSeenLeaders);
						leaderElectionService[index].stop();
						leaderElectionService[index] = null;

						numberSeenLeaders++;
					}
				} else {
					fail("Did not find the leader's index.");
				}
			}

			assertFalse("Did not complete the leader reelection in time.", deadline.isOverdue());
			assertEquals(num, numberSeenLeaders);

		} finally {
			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			for (KubernetesLeaderElectionService electionService : leaderElectionService) {
				if (electionService != null) {
					electionService.stop();
				}
			}
		}
	}

	/**
	 * Tests the repeated reelection of {@link LeaderContender} once the current leader dies.
	 * Furthermore, it tests that new LeaderElectionServices can be started later on and that they
	 * successfully register at Kubernetes and take part in the leader election.
	 */
	@Test
	public void testKubernetesReelectionWithReplacement() throws Exception {
		int num = 3;
		int numTries = 30;

		KubernetesLeaderElectionService[] leaderElectionService = new KubernetesLeaderElectionService[num];
		TestingContender[] contenders = new TestingContender[num];
		KubernetesLeaderRetrievalService leaderRetrievalService = null;

		TestingListener listener = new TestingListener();

		try {
			leaderRetrievalService = KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, LEADER_PATH);

			leaderRetrievalService.start(listener);

			for (int i = 0; i < num; i++) {
				leaderElectionService[i] = KubernetesUtils.createLeaderElectionService(apiContext, configuration, LEADER_PATH);
				contenders[i] = new TestingContender(TEST_URL + "_" + i + "_0", leaderElectionService[i]);

				leaderElectionService[i].start(contenders[i]);
			}

			String pattern = TEST_URL + "_" + "(\\d+)" + "_" + "(\\d+)";
			Pattern regex = Pattern.compile(pattern);

			for (int i = 0; i < numTries; i++) {
				listener.waitForNewLeader(timeout);

				String address = listener.getAddress();

				Matcher m = regex.matcher(address);

				if (m.find()) {
					int index = Integer.parseInt(m.group(1));
					int lastTry = Integer.parseInt(m.group(2));

					assertEquals(listener.getLeaderSessionID(), contenders[index].getLeaderSessionID());

					// stop leader election service = revoke leadership
					leaderElectionService[index].stop();
					// create new leader election service which takes part in the leader election
					leaderElectionService[index] = KubernetesUtils.createLeaderElectionService(apiContext, configuration, LEADER_PATH);
					contenders[index] = new TestingContender(
						TEST_URL + "_" + index + "_" + (lastTry + 1),
						leaderElectionService[index]);

					leaderElectionService[index].start(contenders[index]);
				} else {
					throw new Exception("Did not find the leader's index.");
				}
			}

		} finally {
			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			for (KubernetesLeaderElectionService electionService : leaderElectionService) {
				if (electionService != null) {
					electionService.stop();
				}
			}
		}
	}

	/**
	 * Tests that the current leader is notified when his leader connection information in Kubernetes
	 * are overwritten. The leader must re-establish the correct leader connection information in
	 * Kubernetes.
	 */
	@Test
	public void testMultipleLeaders() throws Exception {
		final String FAULTY_CONTENDER_URL = "faultyContender";
		final String leaderPath = "leader";

		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_LEADER_PATH, leaderPath);

		KubernetesLeaderElectionService leaderElectionService = null;
		KubernetesLeaderRetrievalService leaderRetrievalService = null;
		KubernetesLeaderRetrievalService leaderRetrievalService2 = null;
		TestingListener listener = new TestingListener();
		TestingListener listener2 = new TestingListener();
		TestingContender contender;

		try {
			leaderElectionService = KubernetesUtils.createLeaderElectionService(apiContext, configuration, LEADER_PATH);
			leaderRetrievalService = KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, LEADER_PATH);
			leaderRetrievalService2 = KubernetesUtils.createLeaderRetrievalService(apiContext, configuration, LEADER_PATH);

			contender = new TestingContender(TEST_URL, leaderElectionService);

			leaderElectionService.start(contender);
			leaderRetrievalService.start(listener);

			listener.waitForNewLeader(timeout);

			assertEquals(listener.getLeaderSessionID(), contender.getLeaderSessionID());
			assertEquals(TEST_URL, listener.getAddress());

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(FAULTY_CONTENDER_URL);
			oos.writeObject(null);

			oos.close();

			// overwrite the current leader address, the leader should notice that and correct it
			boolean dataWritten = false;

			while (!dataWritten) {
				KubernetesUtils.deleteConfigMap(apiContext, leaderPath);

				try {
					V1ConfigMap configMap = KubernetesUtils.buildConfigMapWithBinaryData(
						apiContext, leaderPath, baos.toByteArray());
					KubernetesUtils.createConfigMap(apiContext, configMap);

					dataWritten = true;
				} catch (ApiException e) {
					// this can happen if the leader election service was faster
				}
			}

			leaderRetrievalService2.start(listener2);

			listener2.waitForNewLeader(timeout);

			if (FAULTY_CONTENDER_URL.equals(listener2.getAddress())) {
				listener2.waitForNewLeader(timeout);
			}

			assertEquals(listener2.getLeaderSessionID(), contender.getLeaderSessionID());
			assertEquals(listener2.getAddress(), contender.getAddress());

		} finally {
			if (leaderElectionService != null) {
				leaderElectionService.stop();
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			if (leaderRetrievalService2 != null) {
				leaderRetrievalService2.stop();
			}
		}
	}

	// TODO
	/**
	 *  Test that errors in the {@link LeaderElectionService} are correctly forwarded to the
	 *  {@link LeaderContender}.
	 */
	@Test
	public void testExceptionForwarding() throws Exception {
//		KubernetesLeaderElectionService leaderElectionService = null;
//		KubernetesLeaderRetrievalService leaderRetrievalService = null;
//		TestingListener listener = new TestingListener();
//		TestingContender testingContender;
//
//		CuratorFramework client;
//		final CreateBuilder mockCreateBuilder = mock(CreateBuilder.class, Mockito.RETURNS_DEEP_STUBS);
//		final Exception testException = new Exception("Test exception");
//
//		try {
//			client = spy(KubernetesUtils.startCuratorFramework(configuration));
//
//			Answer<CreateBuilder> answer = new Answer<CreateBuilder>() {
//				private int counter = 0;
//
//				@Override
//				public CreateBuilder answer(InvocationOnMock invocation) throws Throwable {
//					counter++;
//
//					// at first we have to create the leader latch, there it mustn't fail yet
//					if (counter < 2) {
//						return (CreateBuilder) invocation.callRealMethod();
//					} else {
//						return mockCreateBuilder;
//					}
//				}
//			};
//
//			doAnswer(answer).when(client).create();
//
//			when(
//				mockCreateBuilder
//					.creatingParentsIfNeeded()
//					.withMode(Matchers.any(CreateMode.class))
//					.forPath(anyString(), any(byte[].class))).thenThrow(testException);
//
//			leaderElectionService = new KubernetesLeaderElectionService(client, "/latch", "/leader");
//			leaderRetrievalService = KubernetesUtils.createLeaderRetrievalService(client, configuration);
//
//			testingContender = new TestingContender(TEST_URL, leaderElectionService);
//
//			leaderElectionService.start(testingContender);
//			leaderRetrievalService.start(listener);
//
//			testingContender.waitForError(timeout);
//
//			assertNotNull(testingContender.getError());
//			assertEquals(testException, testingContender.getError().getCause());
//		} finally {
//			if (leaderElectionService != null) {
//				leaderElectionService.stop();
//			}
//
//			if (leaderRetrievalService != null) {
//				leaderRetrievalService.stop();
//			}
//		}
	}

	// TODO
	/**
	 * Tests that there is no information left in the Kubernetes cluster after the Kubernetes client
	 * has terminated. In other words, checks that the KubernetesLeaderElection service uses
	 * ephemeral nodes.
	 */
	@Test
	public void testEphemeralKubernetesNodes() throws Exception {
//		KubernetesLeaderElectionService leaderElectionService;
//		KubernetesLeaderRetrievalService leaderRetrievalService = null;
//		TestingContender testingContender;
//		TestingListener listener;
//
//		CuratorFramework client = null;
//		CuratorFramework client2 = null;
//		NodeCache cache = null;
//
//		try {
//			client = KubernetesUtils.startCuratorFramework(configuration);
//			client2 = KubernetesUtils.startCuratorFramework(configuration);
//
//			leaderElectionService = KubernetesUtils.createLeaderElectionService(client, configuration);
//			leaderRetrievalService = KubernetesUtils.createLeaderRetrievalService(client2, configuration);
//			testingContender = new TestingContender(TEST_URL, leaderElectionService);
//			listener = new TestingListener();
//
//			final String leaderPath = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_LEADER_PATH);
//			cache = new NodeCache(client2, leaderPath);
//
//			ExistsCacheListener existsListener = new ExistsCacheListener(cache);
//			DeletedCacheListener deletedCacheListener = new DeletedCacheListener(cache);
//
//			cache.getListenable().addListener(existsListener);
//			cache.start();
//
//			leaderElectionService.start(testingContender);
//
//			testingContender.waitForLeader(timeout);
//
//			Future<Boolean> existsFuture = existsListener.nodeExists();
//
//			existsFuture.get(timeout, TimeUnit.MILLISECONDS);
//
//			cache.getListenable().addListener(deletedCacheListener);
//
//			leaderElectionService.stop();
//
//			// now stop the underlying client
//			client.close();
//
//			Future<Boolean> deletedFuture = deletedCacheListener.nodeDeleted();
//
//			// make sure that the leader node has been deleted
//			deletedFuture.get(timeout, TimeUnit.MILLISECONDS);
//
//			leaderRetrievalService.start(listener);
//
//			try {
//				listener.waitForNewLeader(1000L);
//
//				fail("TimeoutException was expected because there is no leader registered and " +
//					"thus there shouldn't be any leader information in Kubernetes.");
//			} catch (TimeoutException e) {
//				//that was expected
//			}
//		} finally {
//			if(leaderRetrievalService != null) {
//				leaderRetrievalService.stop();
//			}
//
//			if (cache != null) {
//				cache.close();
//			}
//
//			if (client2 != null) {
//				client2.close();
//			}
//		}
	}

//	private static class ExistsCacheListener implements NodeCacheListener {
//
//		final CompletableFuture<Boolean> existsPromise = new CompletableFuture<>();
//
//		final NodeCache cache;
//
//		public ExistsCacheListener(final NodeCache cache) {
//			this.cache = cache;
//		}
//
//		public Future<Boolean> nodeExists() {
//			return existsPromise;
//		}
//
//		@Override
//		public void nodeChanged() throws Exception {
//			ChildData data = cache.getCurrentData();
//
//			if (data != null && !existsPromise.isDone()) {
//				existsPromise.complete(true);
//				cache.getListenable().removeListener(this);
//			}
//		}
//	}
//
//	private static class DeletedCacheListener implements NodeCacheListener {
//
//		final CompletableFuture<Boolean> deletedPromise = new CompletableFuture<>();
//
//		final NodeCache cache;
//
//		public DeletedCacheListener(final NodeCache cache) {
//			this.cache = cache;
//		}
//
//		public Future<Boolean> nodeDeleted() {
//			return deletedPromise;
//		}
//
//		@Override
//		public void nodeChanged() throws Exception {
//			ChildData data = cache.getCurrentData();
//
//			if (data == null && !deletedPromise.isDone()) {
//				deletedPromise.complete(true);
//				cache.getListenable().removeListener(this);
//			}
//		}
//	}
}
