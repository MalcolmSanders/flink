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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@link KubernetesStateHandleStore}.
 */
public class KubernetesStateHandleStoreTest {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateHandleStoreTest.class);

	private static KubernetesApiContext apiContext;

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

	@Test
	public void testAdd() throws Exception {
		LongStateStorage longStateStorage = new LongStateStorage();
		KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
			apiContext, longStateStorage, "test-add-cm");

		// Config
		final String key = "testAdd";
		final Long state = 1239712317L;

		// Test
		store.add(key, state);

		// Verify
		// State handle created
		assertEquals(1, store.getAll().size());
		assertEquals(state, store.get(key).retrieveState());

		// TODO: more assertions
	}

	/**
	 * Tests that a non existing path throws an Exception.
	 */
	@Test(expected = Exception.class)
	public void testGetNonExistingPath() throws Exception {
		LongStateStorage stateHandleProvider = new LongStateStorage();
		KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
			apiContext, stateHandleProvider, "test-get-non-existing-path-cm");

		store.get("testGetNonExistingPath");
	}

	/**
	 * Tests that all added state is returned.
	 */
	@Test
	public void testGetAll() throws Exception {
		// Setup
		LongStateStorage stateHandleProvider = new LongStateStorage();
		KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
			apiContext, stateHandleProvider, "test-get-all-cm");

		// Config
		final String key = "testGetAll";

		final Set<Long> expected = new HashSet<>();
		expected.add(311222268470898L);
		expected.add(132812888L);
		expected.add(27255442L);
		expected.add(11122233124L);

		// Test
		for (long val : expected) {
			store.add(key + val, val);
		}

		for (Tuple2<RetrievableStateHandle<Long>, String> val : store.getAll()) {
			assertTrue(expected.remove(val.f0.retrieveState()));
		}
		assertEquals(0, expected.size());
	}

	/**
	 * Tests that state handles are correctly removed.
	 */
	@Test
	public void testRemove() throws Exception {
		// Setup
		LongStateStorage stateHandleProvider = new LongStateStorage();
		KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
			apiContext, stateHandleProvider, "test-remove-cm");

		// Config
		final String key = "testRemove";
		final Long state = 27255442L;

		store.add(key, state);

		final int numberOfGlobalDiscardCalls = LongRetrievableStateHandle.getNumberOfGlobalDiscardCalls();

		// Test
		store.removeAndDiscardState(key);

		// Verify discarded
		assertEquals(0, store.getAll().size());
		assertEquals(0, store.getAllKeysForState().size());
		assertEquals(numberOfGlobalDiscardCalls + 1, LongRetrievableStateHandle.getNumberOfGlobalDiscardCalls());
	}

	// ---------------------------------------------------------------------------------------------
	// Simple test helpers
	// ---------------------------------------------------------------------------------------------

	private static class LongStateStorage implements RetrievableStateStorageHelper<Long> {

		private final List<LongRetrievableStateHandle> stateHandles = new ArrayList<>();

		@Override
		public RetrievableStateHandle<Long> store(Long state) throws Exception {
			LongRetrievableStateHandle stateHandle = new LongRetrievableStateHandle(state);
			stateHandles.add(stateHandle);

			return stateHandle;
		}

		List<LongRetrievableStateHandle> getStateHandles() {
			return stateHandles;
		}
	}

	private static class LongRetrievableStateHandle implements RetrievableStateHandle<Long> {

		private static final long serialVersionUID = -3555329254423838912L;

		private static int numberOfGlobalDiscardCalls = 0;

		private final Long state;

		private int numberOfDiscardCalls = 0;

		public LongRetrievableStateHandle(Long state) {
			this.state = state;
		}

		@Override
		public Long retrieveState() {
			return state;
		}

		@Override
		public void discardState() throws Exception {
			numberOfGlobalDiscardCalls++;
			numberOfDiscardCalls++;
		}

		@Override
		public long getStateSize() {
			return 0;
		}

		int getNumberOfDiscardCalls() {
			return numberOfDiscardCalls;
		}

		public static int getNumberOfGlobalDiscardCalls() {
			return numberOfGlobalDiscardCalls;
		}
	}
}
