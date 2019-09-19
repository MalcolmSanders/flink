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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.kubernetes.KubernetesRunningJobsRegistry;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus.DONE;
import static org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus.PENDING;
import static org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus.RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@link KubernetesRunningJobsRegistry}.
 */
public class KubernetesRunningJobsRegistryTest {
	private static final Logger LOG = LoggerFactory.getLogger(KubernetesRunningJobsRegistryTest.class);

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
	public void testBasic() throws IOException {
		org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
		KubernetesRunningJobsRegistry jobsRegistry = new KubernetesRunningJobsRegistry(apiContext, configuration);
		JobID jobID = new JobID();

		assertEquals(PENDING, jobsRegistry.getJobSchedulingStatus(jobID));

		jobsRegistry.setJobRunning(jobID);
		assertEquals(RUNNING, jobsRegistry.getJobSchedulingStatus(jobID));

		jobsRegistry.setJobFinished(jobID);
		assertEquals(DONE, jobsRegistry.getJobSchedulingStatus(jobID));

		jobsRegistry.clearJob(jobID);
		assertEquals(PENDING, jobsRegistry.getJobSchedulingStatus(jobID));
	}

}
