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

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.kubernetes.KubernetesApiContext;
import org.apache.flink.runtime.kubernetes.KubernetesTestUtils;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.util.KubernetesUtils;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;

import io.kubernetes.client.ApiException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for basic {@link CompletedCheckpointStore} contract and Kubernetes state handling.
 */
public class KubernetesCompletedCheckpointStoreITCase extends CompletedCheckpointStoreTest {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesCompletedCheckpointStoreITCase.class);

	private static KubernetesApiContext apiContext;

	private static final String CHECKPOINT_PATH = "checkpoints";

	@BeforeClass
	public static void beforeClass() throws Exception {
		Boolean isKubernetesAvailable = KubernetesTestUtils.isKubernetesAvailable();
		assumeTrue("Kubernetes environment is not available", isKubernetesAvailable);
		if (isKubernetesAvailable) {
			apiContext = KubernetesTestUtils.createApiContext();
		}
	}

	@AfterClass
	public static void afterClass() {
		if (apiContext != null) {
			KubernetesTestUtils.clearEnvironment(apiContext);
			apiContext = null;
		}
	}

	@Before
	public void before() {
		if (apiContext != null) {
			KubernetesTestUtils.deleteAllConfigMaps(apiContext);
		}
	}

	@Override
	protected CompletedCheckpointStore createCompletedCheckpoints(int maxNumberOfCheckpointsToRetain) throws Exception {
		return new KubernetesCompletedCheckpointStore(maxNumberOfCheckpointsToRetain, apiContext, CHECKPOINT_PATH,
			new TestingRetrievableStateStorageHelper<>(), Executors.directExecutor());
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Tests that older checkpoints are not cleaned up right away when recovering. Only after
	 * another checkpointed has been completed the old checkpoints exceeding the number of
	 * checkpoints to retain will be removed.
	 */
	@Test
	public void testRecover() throws Exception {

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		CompletedCheckpointStore checkpoints = createCompletedCheckpoints(3);

		TestCompletedCheckpoint[] expected = new TestCompletedCheckpoint[]{
			createCheckpoint(0, sharedStateRegistry),
			createCheckpoint(1, sharedStateRegistry),
			createCheckpoint(2, sharedStateRegistry)
		};

		// Add multiple checkpoints
		checkpoints.addCheckpoint(expected[0]);
		checkpoints.addCheckpoint(expected[1]);
		checkpoints.addCheckpoint(expected[2]);

		verifyCheckpointRegistered(expected[0].getOperatorStates().values(), sharedStateRegistry);
		verifyCheckpointRegistered(expected[1].getOperatorStates().values(), sharedStateRegistry);
		verifyCheckpointRegistered(expected[2].getOperatorStates().values(), sharedStateRegistry);

		// All three should be in ZK
		assertEquals(3, getCompletedCheckpointSizeFromKubernetes());
		assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());

		// Recover
		sharedStateRegistry.close();
		sharedStateRegistry = new SharedStateRegistry();
		checkpoints.recover();

		assertEquals(3, getCompletedCheckpointSizeFromKubernetes());
		assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());
		assertEquals(expected[2], checkpoints.getLatestCheckpoint(false));

		List<CompletedCheckpoint> expectedCheckpoints = new ArrayList<>(3);
		expectedCheckpoints.add(expected[1]);
		expectedCheckpoints.add(expected[2]);
		expectedCheckpoints.add(createCheckpoint(3, sharedStateRegistry));

		checkpoints.addCheckpoint(expectedCheckpoints.get(2));

		List<CompletedCheckpoint> actualCheckpoints = checkpoints.getAllCheckpoints();

		assertEquals(expectedCheckpoints, actualCheckpoints);

		for (CompletedCheckpoint actualCheckpoint : actualCheckpoints) {
			verifyCheckpointRegistered(actualCheckpoint.getOperatorStates().values(), sharedStateRegistry);
		}
	}

	/**
	 * Tests that shutdown discards all checkpoints.
	 */
	@Test
	public void testShutdownDiscardsCheckpoints() throws Exception {

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		CompletedCheckpointStore store = createCompletedCheckpoints(1);
		TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);

		store.addCheckpoint(checkpoint);
		assertEquals(1, store.getNumberOfRetainedCheckpoints());
		assertTrue(existCompletedCheckpointInKubernetes(checkpoint.getCheckpointID()));

		store.shutdown(JobStatus.FINISHED);
		assertEquals(0, store.getNumberOfRetainedCheckpoints());
		assertFalse(existCompletedCheckpointInKubernetes(checkpoint.getCheckpointID()));

		sharedStateRegistry.close();
		store.recover();

		assertEquals(0, store.getNumberOfRetainedCheckpoints());
	}

	/**
	 * Tests that suspends keeps all checkpoints (so that they can be recovered
	 * later by the Kubernetes store). Furthermore, suspending a job should release
	 * all locks.
	 */
	@Test
	public void testSuspendKeepsCheckpoints() throws Exception {

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		CompletedCheckpointStore store = createCompletedCheckpoints(1);
		TestCompletedCheckpoint checkpoint = createCheckpoint(0, sharedStateRegistry);

		store.addCheckpoint(checkpoint);
		assertEquals(1, store.getNumberOfRetainedCheckpoints());
		assertTrue(existCompletedCheckpointInKubernetes(checkpoint.getCheckpointID()));

		store.shutdown(JobStatus.SUSPENDED);

		assertEquals(0, store.getNumberOfRetainedCheckpoints());

		assertTrue(existCompletedCheckpointInKubernetes(checkpoint.getCheckpointID()));

		// Recover again
		sharedStateRegistry.close();
		store.recover();

		CompletedCheckpoint recovered = store.getLatestCheckpoint(false);
		assertEquals(checkpoint, recovered);
	}

	/**
	 * FLINK-6284.
	 *
	 * <p>Tests that the latest recovered checkpoint is the one with the highest checkpoint id
	 */
	@Test
	public void testLatestCheckpointRecovery() throws Exception {
		final int numCheckpoints = 3;
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		CompletedCheckpointStore checkpointStore = createCompletedCheckpoints(numCheckpoints);
		List<CompletedCheckpoint> checkpoints = new ArrayList<>(numCheckpoints);

		checkpoints.add(createCheckpoint(9, sharedStateRegistry));
		checkpoints.add(createCheckpoint(10, sharedStateRegistry));
		checkpoints.add(createCheckpoint(11, sharedStateRegistry));

		for (CompletedCheckpoint checkpoint : checkpoints) {
			checkpointStore.addCheckpoint(checkpoint);
		}

		sharedStateRegistry.close();
		checkpointStore.recover();

		CompletedCheckpoint latestCheckpoint = checkpointStore.getLatestCheckpoint(false);

		assertEquals(checkpoints.get(checkpoints.size() - 1), latestCheckpoint);
	}

	/**
	 * FLINK-6612
	 *
	 * <p>Checks that a concurrent checkpoint completion won't discard a checkpoint which has been
	 * recovered by a different completed checkpoint store.
	 */
	@Test
	public void testConcurrentCheckpointOperations() throws Exception {
		final int numberOfCheckpoints = 1;
		final long waitingTimeout = 50L;

		CompletedCheckpointStore kubernetesCheckpointStore1 = createCompletedCheckpoints(numberOfCheckpoints);
		CompletedCheckpointStore kubernetesCheckpointStore2 = createCompletedCheckpoints(numberOfCheckpoints);

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		TestCompletedCheckpoint completedCheckpoint = createCheckpoint(1, sharedStateRegistry);

		// complete the first checkpoint
		kubernetesCheckpointStore1.addCheckpoint(completedCheckpoint);

		// recover the checkpoint by a different checkpoint store
		sharedStateRegistry.close();
		sharedStateRegistry = new SharedStateRegistry();
		kubernetesCheckpointStore2.recover();

		CompletedCheckpoint recoveredCheckpoint = kubernetesCheckpointStore2.getLatestCheckpoint(false);
		assertTrue(recoveredCheckpoint instanceof TestCompletedCheckpoint);
		TestCompletedCheckpoint recoveredTestCheckpoint = (TestCompletedCheckpoint) recoveredCheckpoint;

		// Check that the recovered checkpoint is not yet discarded
		assertFalse(recoveredTestCheckpoint.isDiscarded());

		// complete another checkpoint --> this should remove the first checkpoint from the store
		// because the number of retained checkpoints == 1
		TestCompletedCheckpoint completedCheckpoint2 = createCheckpoint(2, sharedStateRegistry);
		kubernetesCheckpointStore1.addCheckpoint(completedCheckpoint2);

		List<CompletedCheckpoint> allCheckpoints = kubernetesCheckpointStore1.getAllCheckpoints();

		// check that we have removed the first checkpoint from zkCompletedStore1
		assertEquals(Collections.singletonList(completedCheckpoint2), allCheckpoints);

		// lets wait a little bit to see that no discard operation will be executed
		assertFalse("The checkpoint should not have been discarded.", recoveredTestCheckpoint.awaitDiscard(waitingTimeout));

		// check that we have not discarded the first completed checkpoint
		assertFalse(recoveredTestCheckpoint.isDiscarded());

		TestCompletedCheckpoint completedCheckpoint3 = createCheckpoint(3, sharedStateRegistry);

		// this should release the last lock on completedCheckpoint and thus discard it
		kubernetesCheckpointStore2.addCheckpoint(completedCheckpoint3);

		// the checkpoint should be discarded eventually because there is no lock on it anymore
		recoveredTestCheckpoint.awaitDiscard();
	}

	private int getCompletedCheckpointSizeFromKubernetes() throws ApiException {
		return KubernetesUtils.getAllBinaryDataFromConfigMap(apiContext, KubernetesUtils.concatPath(CHECKPOINT_PATH,
			KubernetesCompletedCheckpointStore.COMPLETED_CHECKPOINTS_CONFIG_MAP_NAME)).size();
	}

	private boolean existCompletedCheckpointInKubernetes(long checkpointId) throws ApiException {
		Map<String, byte[]> data = KubernetesUtils.getAllBinaryDataFromConfigMap(apiContext, KubernetesUtils.concatPath(CHECKPOINT_PATH,
			KubernetesCompletedCheckpointStore.COMPLETED_CHECKPOINTS_CONFIG_MAP_NAME));
		if (data == null) {
			return false;
		}
		return data.containsKey(KubernetesCompletedCheckpointStore.checkpointIdToPath(checkpointId));
	}

	static class HeapStateStorageHelper implements RetrievableStateStorageHelper<CompletedCheckpoint> {
		@Override
		public RetrievableStateHandle<CompletedCheckpoint> store(CompletedCheckpoint state) throws Exception {
			return new HeapRetrievableStateHandle<>(state);
		}
	}

	static class HeapRetrievableStateHandle<T extends Serializable> implements RetrievableStateHandle<T> {

		private static final long serialVersionUID = -268548467968932L;

		private static AtomicInteger nextKey = new AtomicInteger(0);

		private static HashMap<Integer, Object> stateMap = new HashMap<>();

		private final int key;

		public HeapRetrievableStateHandle(T state) {
			key = nextKey.getAndIncrement();
			stateMap.put(key, state);
		}

		@SuppressWarnings("unchecked")
		@Override
		public T retrieveState() {
			return (T) stateMap.get(key);
		}

		@Override
		public void discardState() throws Exception {
			stateMap.remove(key);
		}

		@Override
		public long getStateSize() {
			return 0;
		}
	}
}
