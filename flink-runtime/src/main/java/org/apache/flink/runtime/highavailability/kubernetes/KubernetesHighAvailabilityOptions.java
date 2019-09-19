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

package org.apache.flink.runtime.highavailability.kubernetes;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to high-availability settings.
 */
@PublicEvolving
@ConfigGroups(groups = {
	@ConfigGroup(name = "HighAvailabilityKubernetes", keyPrefix = "high-availability.kubernetes")
})
public class KubernetesHighAvailabilityOptions {

	/**
	 * The root path under which Flink stores its entries in Kubernetes.
	 */
	public static final ConfigOption<String> HA_KUBERNETES_ROOT =
		key("high-availability.kubernetes.path.root")
			.defaultValue("flink")
			.withDescription("The root path under which Flink stores its entries in Kubernetes.");

	public static final ConfigOption<String> HA_KUBERNETES_LATCH_PATH =
		key("high-availability.kubernetes.path.latch")
			.defaultValue("leaderlatch")
			.withDescription("Defines the suffix of the leader elector name.");

	/** Kubernetes root path for job graphs. */
	public static final ConfigOption<String> HA_KUBERNETES_JOBGRAPHS_PATH =
		key("high-availability.kubernetes.path.jobgraphs")
			.defaultValue("jobgraphs")
			.withDescription("Kubernetes root path for job graphs");

	public static final ConfigOption<String> HA_KUBERNETES_LEADER_PATH =
		key("high-availability.kubernetes.path.leader")
			.defaultValue("leader")
			.withDescription("Defines the ConfigMap suffix of the leader which contains the URL to the leader "
				+ "and the current leader session ID.");

	/** Kubernetes root path for completed checkpoints. */
	public static final ConfigOption<String> HA_KUBERNETES_CHECKPOINTS_PATH =
		key("high-availability.kubernetes.path.checkpoints")
			.defaultValue("checkpoints")
			.withDescription("Kubernetes root path for completed checkpoints.");

	/** Kubernetes root path for checkpoint counters. */
	public static final ConfigOption<String> HA_KUBERNETES_CHECKPOINT_COUNTER_PATH =
		key("high-availability.kubernetes.path.checkpoint-counter")
			.defaultValue("checkpointcounter")
			.withDescription("Kubernetes root path for checkpoint counters.");

	public static final ConfigOption<String> HA_KUBERNETES_RUNNING_JOB_REGISTRY_PATH =
		key("high-availability.kubernetes.path.running-registry")
			.defaultValue("runningjobregistry");

	// ------------------------------------------------------------------------
	//  Kubernetes Api Context Configurations
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> KUBERNETES_NAMESPACE =
		key("high-availability.kubernetes.namespace")
			.noDefaultValue();

	public static final ConfigOption<String> KUBERNETES_OWNER_REFERENCE_NAME =
		key("high-availability.kubernetes.owner.reference.name")
			.noDefaultValue();

	public static final ConfigOption<String> KUBERNETES_OWNER_REFERENCE_KIND =
		key("high-availability.kubernetes.owner.reference.kind")
			.noDefaultValue();

	public static final ConfigOption<String> KUBERNETES_OWNER_REFERENCE_UID =
		key("high-availability.kubernetes.owner.reference.uid")
			.noDefaultValue();

	public static final ConfigOption<Long> HA_KUBERNETES_LEADER_ELECTION_LEASE_DURATION_IN_MS =
		key("high-availability.kubernetes.leader.election.lease.duration.ms")
			.defaultValue(10000L);

	public static final ConfigOption<Long> HA_KUBERNETES_LEADER_ELECTION_RENEW_DEADLINE_IN_MS =
		key("high-availability.kubernetes.leader.election.renew.deadline.ms")
			.defaultValue(10000L);

	public static final ConfigOption<Long> HA_KUBERNETES_LEADER_ELECTION_RETRY_PERIOD_IN_MS =
		key("high-availability.kubernetes.leader.election.retry.period.ms")
			.defaultValue(5000L);
}
