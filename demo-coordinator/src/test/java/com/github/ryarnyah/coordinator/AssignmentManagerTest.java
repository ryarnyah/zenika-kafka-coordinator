package com.github.ryarnyah.coordinator;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class AssignmentManagerTest {

    @RegisterExtension
    private static final SharedKafkaTestResource SHARED_KAFKA_TEST_RESOURCE = new SharedKafkaTestResource();

    @Test
    void testCanAssignLeader() {
        AssignmentManagerConfig managerConfig = buildConfig("testCanAssignLeader");

        try (AssignmentManager manager = new AssignmentManager(managerConfig, new Metrics())) {
            LeaderListener listener = new LeaderListener();
            manager.registerListener(listener);

            manager.poll();

            assertTrue(listener.isLeader());
        }
    }

    @Test
    void testCanAssignOnlyOneLeader() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService secondExecutorService = Executors.newSingleThreadScheduledExecutor();

        AssignmentManagerConfig managerConfig = buildConfig("testCanAssignOnlyOneLeader");
        AssignmentManagerConfig secondManagerConfig = buildConfig("testCanAssignOnlyOneLeader");

        try (AssignmentManager manager = new AssignmentManager(managerConfig, new Metrics());
             AssignmentManager secondManager = new AssignmentManager(secondManagerConfig, new Metrics())) {
            LeaderListener listener = new LeaderListener();
            LeaderListener secondListener = new LeaderListener();

            executorService.scheduleAtFixedRate(manager::poll, 100, 100, TimeUnit.MILLISECONDS);
            secondExecutorService.scheduleAtFixedRate(secondManager::poll, 100, 100, TimeUnit.MILLISECONDS);

            manager.registerListener(listener);
            secondManager.registerListener(secondListener);

            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertTrue(!listener.isRejoinNeeded() && !secondListener.isRejoinNeeded());
                assertTrue(listener.isLeader() || secondListener.isLeader());
                assertFalse(listener.isLeader() && secondListener.isLeader());
            });
        } finally {
            executorService.shutdown();
            secondExecutorService.shutdown();
        }
    }

    @Test
    void testCanAssignDistributeValuesSingleMember() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        AssignmentManagerConfig managerConfig = buildConfig("testCanAssignDistributeValues");

        Set<String> toDistribute = new HashSet<>() {{
            add("abc");
            add("def");
            add("ghi");
        }};

        try (AssignmentManager manager = new AssignmentManager(managerConfig, new Metrics())) {
            ListStringListener listener = new ListStringListener(toDistribute);

            executorService.scheduleAtFixedRate(manager::poll, 100, 100, TimeUnit.MILLISECONDS);

            manager.registerListener(listener);

            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertFalse(listener.isRejoinNeeded());

                Set<String> allOf = new HashSet<>(listener.getValueAssigned());
                assertEquals(toDistribute.size(), allOf.size());
            });
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    void testCanAssignDistributeValues() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService secondExecutorService = Executors.newSingleThreadScheduledExecutor();

        AssignmentManagerConfig managerConfig = buildConfig("testCanAssignDistributeValues");
        AssignmentManagerConfig secondManagerConfig = buildConfig("testCanAssignDistributeValues");

        Set<String> toDistribute = new HashSet<>() {{
            add("abc");
            add("def");
            add("ghi");
        }};

        try (AssignmentManager manager = new AssignmentManager(managerConfig, new Metrics());
             AssignmentManager secondManager = new AssignmentManager(secondManagerConfig, new Metrics())) {
            ListStringListener listener = new ListStringListener(toDistribute);
            ListStringListener secondListener = new ListStringListener(toDistribute);

            executorService.scheduleAtFixedRate(manager::poll, 100, 110, TimeUnit.MILLISECONDS);
            secondExecutorService.scheduleAtFixedRate(secondManager::poll, 100, 100, TimeUnit.MILLISECONDS);

            manager.registerListener(listener);
            secondManager.registerListener(secondListener);

            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertTrue(!listener.isRejoinNeeded() && !secondListener.isRejoinNeeded());

                Set<String> allOf = new HashSet<>();
                allOf.addAll(listener.getValueAssigned());
                allOf.addAll(secondListener.getValueAssigned());

                assertEquals(toDistribute.size(), allOf.size());
                assertNotEquals(listener.getValueAssigned().size(), secondListener.getValueAssigned().size());
            });
        } finally {
            executorService.shutdown();
            secondExecutorService.shutdown();
        }
    }

    @Test
    void testCanAssignDistributeValuesInCooperativeWay() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService secondExecutorService = Executors.newSingleThreadScheduledExecutor();

        AssignmentManagerConfig managerConfig = buildConfig("testCanAssignDistributeValuesInCooperativeWay");
        AssignmentManagerConfig secondManagerConfig = buildConfig("testCanAssignDistributeValuesInCooperativeWay");

        Set<String> toDistribute = new HashSet<>() {{
            add("abc");
            add("def");
            add("ghi");
        }};

        AssignmentManager secondManager = null;
        try (AssignmentManager manager = new AssignmentManager(managerConfig, new Metrics())) {
            secondManager = new AssignmentManager(secondManagerConfig, new Metrics());

            CooperativeListStringListener listener = new CooperativeListStringListener(toDistribute);
            CooperativeListStringListener secondListener = new CooperativeListStringListener(toDistribute);

            executorService.scheduleAtFixedRate(manager::poll, 100, 100, TimeUnit.MILLISECONDS);
            Future<?> secondFuture = secondExecutorService.scheduleAtFixedRate(secondManager::poll, 100, 100, TimeUnit.MILLISECONDS);

            manager.registerListener(listener);
            secondManager.registerListener(secondListener);

            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertTrue(!listener.isRejoinNeeded() && !secondListener.isRejoinNeeded());

                Set<String> allOf = new HashSet<>();
                allOf.addAll(listener.getValueAssigned());
                allOf.addAll(secondListener.getValueAssigned());

                assertEquals(toDistribute.size(), allOf.size());
                assertNotEquals(listener.getValueAssigned().size(), secondListener.getValueAssigned().size());
            });

            secondFuture.cancel(true);
            secondManager.close();

            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertFalse(listener.isRejoinNeeded());

                Set<String> allOf = new HashSet<>(listener.getValueAssigned());
                assertEquals(toDistribute.size(), allOf.size());
            });

            secondListener.reset();
            secondManager = new AssignmentManager(secondManagerConfig, new Metrics());
            secondExecutorService.scheduleAtFixedRate(secondManager::poll, 100, 100, TimeUnit.MILLISECONDS);
            secondManager.registerListener(secondListener);

            await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertTrue(!listener.isRejoinNeeded() && !secondListener.isRejoinNeeded());

                List<String> allOf = new ArrayList<>();
                allOf.addAll(listener.getValueAssigned());
                allOf.addAll(secondListener.getValueAssigned());

                assertEquals(toDistribute.size(), allOf.size());
                assertNotEquals(listener.getValueAssigned().size(), secondListener.getValueAssigned().size());
            });
        } finally {
            executorService.shutdown();
            secondExecutorService.shutdown();
            if (secondManager != null) {
                secondManager.close();
            }
        }
    }

    private AssignmentManagerConfig buildConfig(String groupId) {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put(CommonClientConfigs.GROUP_ID_CONFIG, groupId);
        configuration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SHARED_KAFKA_TEST_RESOURCE.getKafkaConnectString());
        return new AssignmentManagerConfig(configuration);
    }
}
