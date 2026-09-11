/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.QuorumFeatures;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.kafka.image.TopicsImageTest.DELTA1_RECORDS;
import static org.apache.kafka.image.TopicsImageTest.IMAGE1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KRaftMigrationDriverTest {
    private static final QuorumFeatures QUORUM_FEATURES = new QuorumFeatures(4,
        QuorumFeatures.defaultFeatureMap(true),
        Arrays.asList(4, 5, 6));

    static class MockControllerMetrics extends QuorumControllerMetrics {
        final AtomicBoolean closed = new AtomicBoolean(false);
        final AtomicLong dualWriteOffset = new AtomicLong(0);

        MockControllerMetrics() {
            super(Optional.empty(), Time.SYSTEM, false);
        }

        @Override
        public void updateDualWriteOffset(long offset) {
            super.updateDualWriteOffset(offset);
            dualWriteOffset.set(offset);
        }

        @Override
        public void close() {
            super.close();
            closed.set(true);
        }
    }
    MockControllerMetrics metrics = new MockControllerMetrics();

    Time mockTime = new MockTime(1) {
        public long nanoseconds() {
            // We poll the event for each 1 sec, make it happen for each 10 ms to speed up the test
            return System.nanoTime() - NANOSECONDS.convert(990, MILLISECONDS);
        }
    };

    /**
     * Return a {@link org.apache.kafka.metadata.migration.KRaftMigrationDriver.Builder} that uses the mocks
     * defined in this class.
     */
    KRaftMigrationDriver.Builder defaultTestBuilder() {
        return KRaftMigrationDriver.newBuilder()
            .setNodeId(3000)
            .setZkRecordConsumer(new NoOpRecordConsumer())
            .setInitialZkLoadHandler(metadataPublisher -> { })
            .setFaultHandler(new MockFaultHandler("test"))
            .setQuorumFeatures(QUORUM_FEATURES)
            .setConfigSchema(KafkaConfigSchema.EMPTY)
            .setControllerMetrics(metrics)
            .setTime(mockTime);
    }

    static class NoOpRecordConsumer implements ZkRecordConsumer {
        @Override
        public CompletableFuture<?> beginMigration() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<?> acceptBatch(List<ApiMessageAndVersion> recordBatch) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<OffsetAndEpoch> completeMigration() {
            return CompletableFuture.completedFuture(new OffsetAndEpoch(100, 1));
        }

        @Override
        public void abortMigration() {

        }
    }

    static class CountingMetadataPropagator implements LegacyPropagator {

        public int deltas = 0;
        public int images = 0;

        @Override
        public void startup() {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public void publishMetadata(MetadataImage image) {

        }

        @Override
        public void sendRPCsToBrokersFromMetadataDelta(
            MetadataDelta delta,
            MetadataImage image,
            int zkControllerEpoch
        ) {
            deltas += 1;
        }

        @Override
        public void sendRPCsToBrokersFromMetadataImage(MetadataImage image, int zkControllerEpoch) {
            images += 1;
        }

        @Override
        public void clear() {

        }
    }

    static LogDeltaManifest.Builder logDeltaManifestBuilder(MetadataProvenance provenance, LeaderAndEpoch newLeader) {
        return LogDeltaManifest.newBuilder()
            .provenance(provenance)
            .leaderAndEpoch(newLeader)
            .numBatches(1)
            .elapsedNs(100)
            .numBytes(42);
    }

    RegisterBrokerRecord zkBrokerRecord(int id) {
        RegisterBrokerRecord record = new RegisterBrokerRecord();
        record.setBrokerId(id);
        record.setIsMigratingZkBroker(true);
        record.setFenced(false);
        return record;
    }

    /**
     * Enqueues a metadata change event with the migration driver and returns a future that can be waited on in
     * the test code. The future will complete once the metadata change event executes completely.
     */
    CompletableFuture<Void> enqueueMetadataChangeEventWithFuture(
        KRaftMigrationDriver driver,
        MetadataDelta delta,
        MetadataImage newImage,
        MetadataProvenance provenance
    ) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Consumer<Throwable> completionHandler = ex -> {
            if (ex == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(ex);
            }
        };

        driver.enqueueMetadataChangeEvent(delta, newImage, provenance, false, completionHandler);
        return future;
    }

    @Test
    public void testOnControllerChangeWhenUninitialized() throws InterruptedException {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient.newBuilder().build();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().build();
        MockFaultHandler faultHandler = new MockFaultHandler("testBecomeLeaderUninitialized");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator)
            .setFaultHandler(faultHandler);
        try (KRaftMigrationDriver driver = builder.build()) {
            // Fake a complete migration with ZK client
            migrationClient.setMigrationRecoveryState(
                    ZkMigrationLeadershipState.EMPTY.withKRaftMetadataOffsetAndEpoch(100, 1));

            // simulate the Raft layer running before the driver has fully started.
            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));

            // start up the driver. this will enqueue a poll event. once run, this will enqueue a recovery event
            driver.start();

            // Even though we contrived a race above, the driver still makes it past initialization.
            TestUtils.waitForCondition(() -> driver.migrationState().get(30, TimeUnit.SECONDS).equals(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM),
                "Waiting for KRaftMigrationDriver to enter WAIT_FOR_CONTROLLER_QUORUM state");
        }
    }
    /**
     * Don't send RPCs to brokers for every metadata change, only when brokers or topics change.
     * This is a regression test for KAFKA-14668
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOnlySendNeededRPCsToBrokers(boolean registerControllers) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(1, 2, 3)
            .setConfigMigrationClient(configClient)
            .build();
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator)
            .setInitialZkLoadHandler(metadataPublisher -> { });

        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            setupDeltaForMigration(delta, registerControllers);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            assertEquals(1, metadataPropagator.images);
            assertEquals(0, metadataPropagator.deltas);

            delta = new MetadataDelta(image);
            delta.replay(new ConfigRecord()
                .setResourceType(ConfigResource.Type.BROKER.id())
                .setResourceName("1")
                .setName("foo")
                .setValue("bar"));
            provenance = new MetadataProvenance(120, 1, 2);
            image = delta.apply(provenance);
            enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance).get(1, TimeUnit.MINUTES);

            assertEquals(1, configClient.writtenConfigs.size());
            assertEquals(1, metadataPropagator.images);
            assertEquals(0, metadataPropagator.deltas);

            delta = new MetadataDelta(image);
            delta.replay(new BrokerRegistrationChangeRecord()
                .setBrokerId(1)
                .setBrokerEpoch(0)
                .setFenced(BrokerRegistrationFencingChange.NONE.value())
                .setInControlledShutdown(BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value()));
            provenance = new MetadataProvenance(130, 1, 3);
            image = delta.apply(provenance);
            enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance).get(1, TimeUnit.MINUTES);

            assertEquals(1, metadataPropagator.images);
            assertEquals(1, metadataPropagator.deltas);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMigrationWithClientException(boolean authException) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CountDownLatch claimLeaderAttempts = new CountDownLatch(3);
        CapturingMigrationClient migrationClient = new CapturingMigrationClient(new HashSet<>(Arrays.asList(1, 2, 3)),
                new CapturingTopicMigrationClient(),
                new CapturingConfigMigrationClient(),
                new CapturingAclMigrationClient(),
                new CapturingDelegationTokenMigrationClient(),
                CapturingMigrationClient.EMPTY_BATCH_SUPPLIER) {
            @Override
            public ZkMigrationLeadershipState claimControllerLeadership(ZkMigrationLeadershipState state) {
                if (claimLeaderAttempts.getCount() == 0) {
                    return super.claimControllerLeadership(state);
                } else {
                    claimLeaderAttempts.countDown();
                    if (authException) {
                        throw new MigrationClientAuthException(new RuntimeException("Some kind of ZK auth error!"));
                    } else {
                        throw new MigrationClientException("Some kind of ZK error!");
                    }
                }

            }
        };
        MockFaultHandler faultHandler = new MockFaultHandler("testMigrationClientExpiration");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setFaultHandler(faultHandler)
            .setPropagator(metadataPropagator);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);
            setupDeltaForMigration(delta, true);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Notify the driver that it is the leader
            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));
            // Publish metadata of all the ZK brokers being ready
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());
            assertTrue(claimLeaderAttempts.await(1, TimeUnit.MINUTES));
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            if (authException) {
                assertEquals(MigrationClientAuthException.class, faultHandler.firstException().getCause().getClass());
            } else {
                Assertions.assertNull(faultHandler.firstException());
            }
        }
    }

    @Test
    public void testMigrationWithClientExceptionWhileMigratingZnodeCreation() throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        // suppose the ZNode creation failed 3 times
        CountDownLatch createZnodeAttempts = new CountDownLatch(3);
        CapturingMigrationClient migrationClient = new CapturingMigrationClient(new HashSet<>(Arrays.asList(1, 2, 3)),
                new CapturingTopicMigrationClient(),
                new CapturingConfigMigrationClient(),
                new CapturingAclMigrationClient(),
                new CapturingDelegationTokenMigrationClient(),
                CapturingMigrationClient.EMPTY_BATCH_SUPPLIER) {
            @Override
            public ZkMigrationLeadershipState getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState initialState) {
                if (createZnodeAttempts.getCount() == 0) {
                    this.setMigrationRecoveryState(initialState);
                    return initialState;
                } else {
                    createZnodeAttempts.countDown();
                    throw new MigrationClientException("Some kind of ZK error!");
                }
            }
        };
        MockFaultHandler faultHandler = new MockFaultHandler("testMigrationClientExpiration");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
                .setZkMigrationClient(migrationClient)
                .setFaultHandler(faultHandler)
                .setPropagator(metadataPropagator);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);
            setupDeltaForMigration(delta, true);

            startAndWaitForRecoveringMigrationStateFromZK(driver);

            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);
            // Before leadership claiming, the getOrCreateMigrationRecoveryState should be able to get correct state
            assertTrue(createZnodeAttempts.await(1, TimeUnit.MINUTES));

            // Notify the driver that it is the leader
            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));
            // Publish metadata of all the ZK brokers being ready
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                    new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            Assertions.assertNull(faultHandler.firstException());
        }
    }

    /**
     * Reproduces an incident where a write to the /migration znode succeeds in ZK, but the driver
     * never learns its cached version has become stale (i.e. the acknowledgment was lost).
     * Since {@code applyMigrationOperation} only replaces the cached {@link ZkMigrationLeadershipState}
     * on a successful write, every subsequent write keeps reusing the same stale version and keeps getting
     * rejected.
     * <p>
     * As a result, three things that should agree start to diverge, and keep diverging further
     * with every subsequent change: what KRaft's own metadata log says (always current), what is
     * actually persisted in ZK's own znodes (frozen once a write depending on the stale version
     * fails), and what a ZK-mode broker was actually told via RPC (also frozen, and for the same
     * reason as the RPC only fires after a successful write).
     * <p>
     * The test walks through this in four stages: a healthy baseline, a no-op delta that fails
     * only when we try to update the migration state znode (silently, never escalated to the fault handler),
     * and two separate leadership changes whose own content writes also fail (this time escalated),
     * proving the driver reuses the exact same cached state and reports the exact same rejection
     * both times. This will then continue indefinitely for the lifetime of the driver and KRaft and ZK will
     * continue to diverge.
     */
    @Test
    public void testKRaftAndZkBrokerStateDivergeWhenMigrationVersionChecksFail() throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        AtomicInteger actualMigrationZkVersion = new AtomicInteger(0);
        Map<String, Integer> zkPartitionLeaders = new HashMap<>();
        List<ZkMigrationLeadershipState> capturedStatesAtLeaderChangeAttempts = new ArrayList<>();
        CapturingTopicMigrationClient topicClient = newVersionCheckingTopicClient(
                actualMigrationZkVersion, zkPartitionLeaders, capturedStatesAtLeaderChangeAttempts);
        CapturingMigrationClient migrationClient = newVersionCheckingMigrationClient(topicClient, actualMigrationZkVersion);

        MockFaultHandler faultHandler = new MockFaultHandler("testKRaftAndZkBrokerStateDivergeWhenMigrationVersionChecksFail");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
                .setPropagator(metadataPropagator)
                .setZkMigrationClient(migrationClient)
                .setFaultHandler(faultHandler)
                .setInitialZkLoadHandler(metadataPublisher -> { });

        try (KRaftMigrationDriver driver = builder.build()) {
            // Setup: migrate a topic/partition to DUAL_WRITE with broker 1 as leader
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);
            setupDeltaForMigration(delta, true);

            startAndWaitForRecoveringMigrationStateFromZK(driver);

            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            Uuid topicId = Uuid.randomUuid();
            delta.replay(new TopicRecord().setName("test-topic").setTopicId(topicId));
            delta.replay(new PartitionRecord()
                    .setPartitionId(0)
                    .setTopicId(topicId)
                    .setReplicas(Arrays.asList(1, 2, 3))
                    .setIsr(Arrays.asList(1, 2, 3))
                    .setLeader(1)
                    .setLeaderEpoch(0)
                    .setPartitionEpoch(0));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            // Healthy baseline: KRaft, ZK, and brokers all agree broker 1 leads.
            assertEquals(1, metadataPropagator.images);
            assertEquals(0, metadataPropagator.deltas);
            assertEquals(1, image.topics().getTopic(topicId).partitions().get(0).leader);
            assertEquals(Integer.valueOf(1), zkPartitionLeaders.get("test-topic-0"));

            // The migrationZkVersion advances by 1, but the driver's cached version is never told.
            // i.e. A lost acknowledgement.
            int cachedVersionBeforeDivergence = actualMigrationZkVersion.get();
            actualMigrationZkVersion.incrementAndGet();

            // Enqueue a no-op record, this makes no changes in ZK initially, but fails when we try and update the migration state znode.
            // This throws a MigrationClientException.
            delta = new MetadataDelta(image);
            delta.replay(new NoOpRecord());
            provenance = new MetadataProvenance(110, 1, 2);
            image = delta.apply(provenance);

            CompletableFuture<Void> noOpFuture = enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance);
            ExecutionException noOpException = Assertions.assertThrows(ExecutionException.class, () -> noOpFuture.get(1, TimeUnit.MINUTES));
            assertEquals(MigrationClientException.class, noOpException.getCause().getClass());

            // MigrationClientExceptions are just logged, never escalated to the fault handler.
            Assertions.assertNull(faultHandler.firstException());

            // Enqueue a leadership change: this attempts to write to ZK, but fails again because of the mismatched versions.
            // This time with a RuntimeException.
            delta = new MetadataDelta(image);
            delta.replay(new PartitionChangeRecord()
                    .setPartitionId(0)
                    .setTopicId(topicId)
                    .setLeader(2));
            provenance = new MetadataProvenance(120, 1, 3);
            image = delta.apply(provenance);

            CompletableFuture<Void> firstLeaderChangeFuture = enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance);
            ExecutionException firstLeaderChangeException = Assertions.assertThrows(ExecutionException.class, () -> firstLeaderChangeFuture.get(1, TimeUnit.MINUTES));
            assertEquals(RuntimeException.class, firstLeaderChangeException.getCause().getClass());

            // The real version is still ahead of the driver's cached version, exactly as it was before this attempt.
            assertEquals(cachedVersionBeforeDivergence + 1, actualMigrationZkVersion.get());
            // The stale cached version was used for the leadership change.
            assertEquals(cachedVersionBeforeDivergence, capturedStatesAtLeaderChangeAttempts.get(0).migrationZkVersion());

            // KRaft's own log is unaffected and already reflects broker 2.
            assertEquals(2, image.topics().getTopic(topicId).partitions().get(0).leader);
            // ZK's own content is unchanged as the write never landed.
            assertEquals(Integer.valueOf(1), zkPartitionLeaders.get("test-topic-0"));
            // No ZK-mode broker was ever notified of this change either.
            assertEquals(1, metadataPropagator.images);
            assertEquals(0, metadataPropagator.deltas);

            // Unlike a failure updating just the migration state znode, this raw exception is escalated to the fault handler.
            Assertions.assertNotNull(faultHandler.firstException());

            // Enqueue a second leadership change: the driver continues to reuse the same stale cache and will fail again.
            delta = new MetadataDelta(image);
            delta.replay(new PartitionChangeRecord()
                    .setPartitionId(0)
                    .setTopicId(topicId)
                    .setLeader(3));
            provenance = new MetadataProvenance(130, 1, 4);
            image = delta.apply(provenance);

            CompletableFuture<Void> secondLeaderChangeFuture = enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance);
            ExecutionException secondLeaderChangeException = Assertions.assertThrows(ExecutionException.class,
                    () -> secondLeaderChangeFuture.get(1, TimeUnit.MINUTES));
            assertEquals(RuntimeException.class, secondLeaderChangeException.getCause().getClass());

            // Confirm the exact same cached object was passed in both times.
            assertEquals(2, capturedStatesAtLeaderChangeAttempts.size());
            Assertions.assertSame(
                    capturedStatesAtLeaderChangeAttempts.get(0),
                    capturedStatesAtLeaderChangeAttempts.get(1));

            // The cache is still stuck on the exact same stale version as before.
            assertEquals(cachedVersionBeforeDivergence + 1, actualMigrationZkVersion.get());
            assertEquals(cachedVersionBeforeDivergence, capturedStatesAtLeaderChangeAttempts.get(1).migrationZkVersion());

            // KRaft has moved on again, now to broker 3.
            assertEquals(3, image.topics().getTopic(topicId).partitions().get(0).leader);
            // ZK's own content is now two changes behind, still broker 1.
            assertEquals(Integer.valueOf(1), zkPartitionLeaders.get("test-topic-0"));
            // Brokers still heard nothing at all and the gap keeps widening.
            assertEquals(1, metadataPropagator.images);
            assertEquals(0, metadataPropagator.deltas);
        }
    }

    /**
     * Shows that the dualWriteOffset (used to calculate the ZkWriteBehindLag) is not a reliable
     * signal for a stuck write. It resets to the latest offset on every no-op delta (since handleDelta
     * trivially succeeds for those), silently overwriting any indication that a real change in between
     * (such as a leadership change in this test), got stuck and never made it to ZK.
     */
    @Test
    public void testDualWriteOffsetUpdatesToLatestOnNoOpsWhileWriteRemainsStuck() throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        AtomicInteger actualMigrationZkVersion = new AtomicInteger(0);
        Map<String, Integer> zkPartitionLeaders = new HashMap<>();
        List<ZkMigrationLeadershipState> capturedStatesAtLeaderChangeAttempts = new ArrayList<>();
        CapturingTopicMigrationClient topicClient = newVersionCheckingTopicClient(
                actualMigrationZkVersion, zkPartitionLeaders, capturedStatesAtLeaderChangeAttempts);
        CapturingMigrationClient migrationClient = newVersionCheckingMigrationClient(topicClient, actualMigrationZkVersion);

        MockFaultHandler faultHandler = new MockFaultHandler("testDualWriteOffsetUpdatesToLatestOnNoOpsWhileWriteRemainsStuck");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
                .setPropagator(metadataPropagator)
                .setZkMigrationClient(migrationClient)
                .setFaultHandler(faultHandler)
                .setInitialZkLoadHandler(metadataPublisher -> { });

        try (KRaftMigrationDriver driver = builder.build()) {
            // Setup: migrate a topic/partition to DUAL_WRITE with broker 1 as leader
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);
            setupDeltaForMigration(delta, true);

            startAndWaitForRecoveringMigrationStateFromZK(driver);

            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            Uuid topicId = Uuid.randomUuid();
            delta.replay(new TopicRecord().setName("test-topic").setTopicId(topicId));
            delta.replay(new PartitionRecord()
                    .setPartitionId(0)
                    .setTopicId(topicId)
                    .setReplicas(Arrays.asList(1, 2, 3))
                    .setIsr(Arrays.asList(1, 2, 3))
                    .setLeader(1)
                    .setLeaderEpoch(0)
                    .setPartitionEpoch(0));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            // The migrationZkVersion advances by 1, but the driver's cached version is never told.
            // i.e. A lost acknowledgement.
            actualMigrationZkVersion.incrementAndGet();

            // First no-op: no content to write, so handleDelta succeeds and the metric advances
            // to 110, even though the write to the migration state znode right after it fails.
            delta = new MetadataDelta(image);
            delta.replay(new NoOpRecord());
            provenance = new MetadataProvenance(110, 1, 2);
            image = delta.apply(provenance);

            CompletableFuture<Void> firstNoOpFuture = enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance);
            ExecutionException firstNoOpException = Assertions.assertThrows(ExecutionException.class,
                    () -> firstNoOpFuture.get(1, TimeUnit.MINUTES));
            assertEquals(MigrationClientException.class, firstNoOpException.getCause().getClass());
            assertEquals(110, metrics.dualWriteOffset.get());

            // Leadership change: the content write itself now fails, inside handleDelta, before
            // the metric update is ever reached, so it stays frozen at 110, not 120.
            delta = new MetadataDelta(image);
            delta.replay(new PartitionChangeRecord()
                    .setPartitionId(0)
                    .setTopicId(topicId)
                    .setLeader(2));
            provenance = new MetadataProvenance(120, 1, 3);
            image = delta.apply(provenance);

            CompletableFuture<Void> leaderChangeFuture = enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance);
            ExecutionException leaderChangeException = Assertions.assertThrows(ExecutionException.class,
                    () -> leaderChangeFuture.get(1, TimeUnit.MINUTES));
            assertEquals(RuntimeException.class, leaderChangeException.getCause().getClass());
            assertEquals(110, metrics.dualWriteOffset.get());

            // Second no-op: handleDelta succeeds again, so the metric resets to 130 (the latest offset)
            // with no trace left of the leadership change that got stuck at 120.
            delta = new MetadataDelta(image);
            delta.replay(new NoOpRecord());
            provenance = new MetadataProvenance(130, 1, 4);
            image = delta.apply(provenance);

            CompletableFuture<Void> secondNoOpFuture = enqueueMetadataChangeEventWithFuture(driver, delta, image, provenance);
            ExecutionException secondNoOpException = Assertions.assertThrows(ExecutionException.class,
                    () -> secondNoOpFuture.get(1, TimeUnit.MINUTES));
            assertEquals(MigrationClientException.class, secondNoOpException.getCause().getClass());

            // The metric now reports the latest offset, even though the write to ZK has been
            // stuck this whole time and a real leadership change was silently dropped in between.
            assertEquals(130, metrics.dualWriteOffset.get());
        }
    }

    private CapturingTopicMigrationClient newVersionCheckingTopicClient(
            AtomicInteger actualMigrationZkVersion,
            Map<String, Integer> zkPartitionLeaders,
            List<ZkMigrationLeadershipState> capturedStatesAtLeaderChangeAttempts
    ) {
        return new CapturingTopicMigrationClient() {
            @Override
            public ZkMigrationLeadershipState createTopic(
                    String topicName,
                    Uuid topicId,
                    Map<Integer, PartitionRegistration> topicPartitions,
                    ZkMigrationLeadershipState state
            ) {
                topicPartitions.forEach((partitionId, registration) ->
                        zkPartitionLeaders.put(topicName + "-" + partitionId, registration.leader));
                return super.createTopic(topicName, topicId, topicPartitions, state);
            }

            @Override
            public ZkMigrationLeadershipState updateTopicPartitions(
                    Map<String, Map<Integer, PartitionRegistration>> topicPartitions,
                    ZkMigrationLeadershipState state
            ) {
                capturedStatesAtLeaderChangeAttempts.add(state);
                if (state.migrationZkVersion() != actualMigrationZkVersion.get()) {
                    throw new RuntimeException("Conditional update on KRaft Migration ZNode failed. Sent zkVersion = " +
                            state.migrationZkVersion() + ". The failed write was: " + state +
                            ". This indicates that another KRaft controller is making writes to ZooKeeper.");
                }
                topicPartitions.forEach((topicName, partitionMap) -> partitionMap.forEach((partitionId, registration) ->
                        zkPartitionLeaders.put(topicName + "-" + partitionId, registration.leader)));
                ZkMigrationLeadershipState newState = state.withMigrationZkVersion(actualMigrationZkVersion.incrementAndGet());
                return super.updateTopicPartitions(topicPartitions, newState);
            }
        };
    }

    private CapturingMigrationClient newVersionCheckingMigrationClient(
            CapturingTopicMigrationClient topicClient,
            AtomicInteger actualMigrationZkVersion
    ) {
        return new CapturingMigrationClient(
                new HashSet<>(Arrays.asList(1, 2, 3)),
                topicClient,
                new CapturingConfigMigrationClient(),
                new CapturingAclMigrationClient(),
                new CapturingDelegationTokenMigrationClient(),
                CapturingMigrationClient.EMPTY_BATCH_SUPPLIER
        ) {
            @Override
            public ZkMigrationLeadershipState setMigrationRecoveryState(ZkMigrationLeadershipState state) {
                if (state.migrationZkVersion() != -1 && state.migrationZkVersion() != actualMigrationZkVersion.get()) {
                    throw new MigrationClientException("KeeperErrorCode = BadVersion for /migration");
                }
                ZkMigrationLeadershipState newState = state.withMigrationZkVersion(actualMigrationZkVersion.incrementAndGet());
                return super.setMigrationRecoveryState(newState);
            }
        };
    }

    private void setupDeltaForMigration(
        MetadataDelta delta,
        boolean registerControllers
    ) {
        if (registerControllers) {
            delta.replay(new FeatureLevelRecord().
                    setName(MetadataVersion.FEATURE_NAME).
                    setFeatureLevel(MetadataVersion.IBP_3_7_IV0.featureLevel()));
            for (int id : QUORUM_FEATURES.quorumNodeIds()) {
                delta.replay(RecordTestUtils.createTestControllerRegistration(id, true));
            }
        } else {
            delta.replay(new FeatureLevelRecord().
                    setName(MetadataVersion.FEATURE_NAME).
                    setFeatureLevel(MetadataVersion.IBP_3_6_IV2.featureLevel()));
        }
    }

    private void setupDeltaWithControllerRegistrations(
        MetadataDelta delta,
        List<Integer> notReadyIds,
        List<Integer> readyIds
    ) {
        delta.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.IBP_3_7_IV0.featureLevel()));
        delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
        for (int id : notReadyIds) {
            delta.replay(RecordTestUtils.createTestControllerRegistration(id, false));
        }
        for (int id : readyIds) {
            delta.replay(RecordTestUtils.createTestControllerRegistration(id, true));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testShouldNotMoveToNextStateIfControllerNodesAreNotReadyToMigrate(
        boolean allNodePresent
    ) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().setBrokersInZk(1).build();

        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            if (allNodePresent) {
                setupDeltaWithControllerRegistrations(delta, Arrays.asList(4, 5, 6), Collections.emptyList());
            } else {
                setupDeltaWithControllerRegistrations(delta, Collections.emptyList(), Arrays.asList(4, 5));
            }
            delta.replay(zkBrokerRecord(1));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Not all controller nodes are ready. So we should stay at WAIT_FOR_CONTROLLER_QUORUM state.
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM),
                "Waiting for KRaftMigrationDriver to enter WAIT_FOR_CONTROLLER_QUORUM state");

            // Controller nodes don't have zkMigrationReady set. Should still stay at WAIT_FOR_CONTROLLER_QUORUM state.
            assertEquals(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM, driver.migrationState().get(1, TimeUnit.MINUTES));

            // Update so that all controller nodes are zkMigrationReady. Now we should be able to move to the next state.
            delta = new MetadataDelta(image);
            setupDeltaWithControllerRegistrations(delta, Collections.emptyList(), Arrays.asList(4, 5, 6));
            image = delta.apply(new MetadataProvenance(200, 1, 2));
            driver.onMetadataUpdate(delta, image, new LogDeltaManifest.Builder().
                    provenance(image.provenance()).
                    leaderAndEpoch(newLeader).
                    numBatches(1).
                    elapsedNs(100).
                    numBytes(42).
                    build());
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");
        }
    }

    @Test
    public void testSkipWaitForBrokersInDualWrite() throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient.newBuilder().build();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().build();
        MockFaultHandler faultHandler = new MockFaultHandler("testMigrationClientExpiration");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator)
            .setFaultHandler(faultHandler);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            // Fake a complete migration with ZK client
            migrationClient.setMigrationRecoveryState(
                ZkMigrationLeadershipState.EMPTY.withKRaftMetadataOffsetAndEpoch(100, 1));

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(ZkMigrationState.MIGRATION.toRecord().message());
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");
        }
    }

    @FunctionalInterface
    interface TopicDualWriteVerifier {
        void verify(
            KRaftMigrationDriver driver,
            CapturingMigrationClient migrationClient,
            CapturingTopicMigrationClient topicClient,
            CapturingConfigMigrationClient configClient
        ) throws Exception;
    }

    public void setupTopicDualWrite(TopicDualWriteVerifier verifier) throws Exception {
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();

        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient() {
            @Override
            public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {
                IMAGE1.topicsByName().forEach((topicName, topicImage) -> {
                    Map<Integer, List<Integer>> assignment = new HashMap<>();
                    topicImage.partitions().forEach((partitionId, partitionRegistration) ->
                        assignment.put(partitionId, IntStream.of(partitionRegistration.replicas).boxed().collect(Collectors.toList()))
                    );
                    visitor.visitTopic(topicName, topicImage.id(), assignment);

                    topicImage.partitions().forEach((partitionId, partitionRegistration) ->
                        visitor.visitPartition(new TopicIdPartition(topicImage.id(), new TopicPartition(topicName, partitionId)), partitionRegistration)
                    );
                });
            }
        };
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0, 1, 2, 3, 4, 5)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .build();
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setPropagator(metadataPropagator);
        try (KRaftMigrationDriver driver = builder.build()) {
            verifier.verify(driver, migrationClient, topicClient, configClient);
        }
    }

    @Test
    public void testTopicDualWriteSnapshot() throws Exception {
        setupTopicDualWrite((driver, migrationClient, topicClient, configClient) -> {
            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(0));
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(zkBrokerRecord(4));
            delta.replay(zkBrokerRecord(5));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Wait for migration
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            // Modify topics in a KRaft snapshot -- delete foo, modify bar, add baz, add new foo, add bam, delete bam
            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            RecordTestUtils.replayAll(delta, DELTA1_RECORDS);
            image = delta.apply(provenance);
            driver.onMetadataUpdate(delta, image, new SnapshotManifest(provenance, 100));
            driver.migrationState().get(1, TimeUnit.MINUTES);

            assertEquals(1, topicClient.deletedTopics.size());
            assertEquals("foo", topicClient.deletedTopics.get(0));
            assertEquals(2, topicClient.createdTopics.size());
            assertTrue(topicClient.createdTopics.contains("foo"));
            assertTrue(topicClient.createdTopics.contains("baz"));
            assertTrue(topicClient.updatedTopicPartitions.get("bar").contains(0));
            assertEquals(0, configClient.deletedResources.size());
        });
    }

    @Test
    public void testTopicDualWriteDelta() throws Exception {
        setupTopicDualWrite((driver, migrationClient, topicClient, configClient) -> {
            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(0));
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(zkBrokerRecord(4));
            delta.replay(zkBrokerRecord(5));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Wait for migration
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            // Modify topics in a KRaft snapshot -- delete foo, modify bar, add baz, add new foo, add bam, delete bam
            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            RecordTestUtils.replayAll(delta, DELTA1_RECORDS);
            image = delta.apply(provenance);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());
            driver.migrationState().get(1, TimeUnit.MINUTES);

            assertEquals(1, topicClient.deletedTopics.size());
            assertEquals("foo", topicClient.deletedTopics.get(0));
            assertEquals(2, topicClient.createdTopics.size());
            assertTrue(topicClient.createdTopics.contains("foo"));
            assertTrue(topicClient.createdTopics.contains("baz"));
            assertTrue(topicClient.updatedTopicPartitions.get("bar").contains(0));
            assertEquals(0, configClient.deletedResources.size());
        });
    }

    @Test
    public void testNoDualWriteBeforeMigration() throws Exception {
        setupTopicDualWrite((driver, migrationClient, topicClient, configClient) -> {
            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(0));
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(zkBrokerRecord(4));
            delta.replay(zkBrokerRecord(5));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta with this node (3000) as the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM),
                "Waiting for KRaftMigrationDriver to enter WAIT_FOR_CONTROLLER_QUORUM state");

            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            driver.transitionTo(MigrationDriverState.WAIT_FOR_BROKERS);
            driver.transitionTo(MigrationDriverState.BECOME_CONTROLLER);
            driver.transitionTo(MigrationDriverState.ZK_MIGRATION);
            driver.transitionTo(MigrationDriverState.SYNC_KRAFT_TO_ZK);

            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            RecordTestUtils.replayAll(delta, DELTA1_RECORDS);
            image = delta.apply(provenance);
            driver.onMetadataUpdate(delta, image, new SnapshotManifest(provenance, 100));


            // Wait for migration
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");
        });
    }

    @Test
    public void testControllerFailover() throws Exception {
        setupTopicDualWrite((driver, migrationClient, topicClient, configClient) -> {
            MetadataImage image = new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                IMAGE1,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
            MetadataDelta delta = new MetadataDelta(image);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(0));
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            delta.replay(zkBrokerRecord(4));
            delta.replay(zkBrokerRecord(5));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            // Publish a delta making a different node the leader
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(3001), 1);
            driver.onControllerChange(newLeader);
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Fake a complete migration
            migrationClient.setMigrationRecoveryState(
                ZkMigrationLeadershipState.EMPTY.withKRaftMetadataOffsetAndEpoch(100, 1));

            // Modify topics in a KRaft -- delete foo, modify bar, add baz, add new foo, add bam, delete bam
            provenance = new MetadataProvenance(200, 1, 1);
            delta = new MetadataDelta(image);
            RecordTestUtils.replayAll(delta, DELTA1_RECORDS);
            image = delta.apply(provenance);

            // Standby driver does not do anything with this delta besides remember the image
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance, newLeader).build());

            // Standby becomes leader
            newLeader = new LeaderAndEpoch(OptionalInt.of(3000), 1);
            driver.onControllerChange(newLeader);
            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                "");
            assertEquals(1, topicClient.deletedTopics.size());
            assertEquals("foo", topicClient.deletedTopics.get(0));
            assertEquals(2, topicClient.createdTopics.size());
            assertTrue(topicClient.createdTopics.contains("foo"));
            assertTrue(topicClient.createdTopics.contains("baz"));
            assertTrue(topicClient.updatedTopicPartitions.get("bar").contains(0));
            assertEquals(0, configClient.deletedResources.size());
        });
    }

    @Test
    public void testBeginMigrationOnce() throws Exception {
        AtomicInteger migrationBeginCalls = new AtomicInteger(0);
        NoOpRecordConsumer recordConsumer = new NoOpRecordConsumer() {
            @Override
            public CompletableFuture<?> beginMigration() {
                migrationBeginCalls.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        };
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().setBrokersInZk(1, 2, 3).build();
        MockFaultHandler faultHandler = new MockFaultHandler("testBeginMigrationOnce");
        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
            .setZkMigrationClient(migrationClient)
            .setZkRecordConsumer(recordConsumer)
            .setPropagator(metadataPropagator)
            .setFaultHandler(faultHandler);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));
            
            // Call onMetadataUpdate twice. The first call will trigger the migration to begin (due to presence of brokers)
            // Both calls will "wakeup" the driver and cause a PollEvent to be run. Calling these back-to-back effectively
            // causes two MigrateMetadataEvents to be enqueued. Ensure only one is actually run.
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");
            assertEquals(1, migrationBeginCalls.get());
        }
    }

    private List<ApiMessageAndVersion> fillBatch(int size) {
        ApiMessageAndVersion[] batch = new ApiMessageAndVersion[size];
        Arrays.fill(batch, new ApiMessageAndVersion(new TopicRecord().setName("topic-fill").setTopicId(Uuid.randomUuid()), (short) 0));
        return Arrays.asList(batch);
    }

    static Stream<Arguments> batchSizes() {
        int defaultBatchSize = 200;
        return Stream.of(
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(0, 0, 0, 0), 0, 0),
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(0, 0, 1, 0), 1, 1),
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(1, 1, 1, 1), 1, 4),
            Arguments.of(Optional.of(1000), Collections.singletonList(999), 1, 999),
            Arguments.of(Optional.of(1000), Collections.singletonList(1000), 1, 1000),
            Arguments.of(Optional.of(1000), Collections.singletonList(1001), 1, 1001),
            Arguments.of(Optional.of(1000), Arrays.asList(1000, 1), 2, 1001),
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(0, 0, 0, 0), 0, 0),
            Arguments.of(Optional.of(1000), Arrays.asList(1000, 1000, 1000), 3, 3000),
            Arguments.of(Optional.of(defaultBatchSize), Collections.singletonList(defaultBatchSize + 1), 1, 201),
            Arguments.of(Optional.of(defaultBatchSize), Arrays.asList(defaultBatchSize, 1), 2, 201),
            Arguments.of(Optional.empty(), Collections.singletonList(defaultBatchSize + 1), 1, 201),
            Arguments.of(Optional.empty(), Arrays.asList(defaultBatchSize, 1), 2, 201)
        );
    }
    @ParameterizedTest
    @MethodSource("batchSizes")
    public void testCoalesceMigrationRecords(Optional<Integer> configBatchSize, List<Integer> batchSizes, int expectedBatchCount, int expectedRecordCount) throws Exception {
        List<List<ApiMessageAndVersion>> batchesPassedToController = new ArrayList<>();
        NoOpRecordConsumer recordConsumer = new NoOpRecordConsumer() {
            @Override
            public CompletableFuture<?> acceptBatch(List<ApiMessageAndVersion> recordBatch) {
                batchesPassedToController.add(recordBatch);
                return CompletableFuture.completedFuture(null);
            }
        };
        CountingMetadataPropagator metadataPropagator = new CountingMetadataPropagator();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(1, 2, 3)
            .setBatchSupplier(new CapturingMigrationClient.MigrationBatchSupplier() {
                @Override
                public List<List<ApiMessageAndVersion>> recordBatches() {
                    List<List<ApiMessageAndVersion>> batches = new ArrayList<>();
                    for (int batchSize : batchSizes) {
                        batches.add(fillBatch(batchSize));
                    }
                    return batches;
                }
            })
            .build();
        MockFaultHandler faultHandler = new MockFaultHandler("testRebatchMigrationRecords");

        KRaftMigrationDriver.Builder builder = defaultTestBuilder()
                .setZkMigrationClient(migrationClient)
                .setZkRecordConsumer(recordConsumer)
                .setPropagator(metadataPropagator)
                .setFaultHandler(faultHandler);
        configBatchSize.ifPresent(builder::setMinMigrationBatchSize);
        try (KRaftMigrationDriver driver = builder.build()) {
            MetadataImage image = MetadataImage.EMPTY;
            MetadataDelta delta = new MetadataDelta(image);

            startAndWaitForRecoveringMigrationStateFromZK(driver);
            setupDeltaForMigration(delta, true);
            delta.replay(ZkMigrationState.PRE_MIGRATION.toRecord().message());
            delta.replay(zkBrokerRecord(1));
            delta.replay(zkBrokerRecord(2));
            delta.replay(zkBrokerRecord(3));
            MetadataProvenance provenance = new MetadataProvenance(100, 1, 1);
            image = delta.apply(provenance);

            driver.onControllerChange(new LeaderAndEpoch(OptionalInt.of(3000), 1));

            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                    new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());
            driver.onMetadataUpdate(delta, image, logDeltaManifestBuilder(provenance,
                    new LeaderAndEpoch(OptionalInt.of(3000), 1)).build());

            TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.DUAL_WRITE),
                    "Waiting for KRaftMigrationDriver to enter DUAL_WRITE state");

            assertEquals(expectedBatchCount, batchesPassedToController.size());
            assertEquals(expectedRecordCount, batchesPassedToController.stream().mapToInt(List::size).sum());
        }
    }

    // Wait until the driver has recovered MigrationState From ZK. This is to simulate the driver needs to be installed as the metadata publisher
    // so that it can receive onControllerChange (KRaftLeaderEvent) and onMetadataUpdate (MetadataChangeEvent) events.
    private void startAndWaitForRecoveringMigrationStateFromZK(KRaftMigrationDriver driver) throws InterruptedException {
        driver.start();
        TestUtils.waitForCondition(() -> driver.migrationState().get(1, TimeUnit.MINUTES).equals(MigrationDriverState.INACTIVE),
                "Waiting for KRaftMigrationDriver to enter INACTIVE state");
    }
}
