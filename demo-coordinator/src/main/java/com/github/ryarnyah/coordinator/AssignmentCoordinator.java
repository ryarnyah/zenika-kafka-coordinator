package com.github.ryarnyah.coordinator;

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class AssignmentCoordinator extends AbstractCoordinator implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssignmentCoordinator.class);

    /**
     * Type de protocole.
     */
    private static final String PROTOCOL_TYPE = "assign";
    /**
     * Sub-type de protocole.
     */
    private static final String SUB_PROTOCOL_TYPE_V0 = "assign_v0";

    private final Collection<AssignmentListener> listeners = new HashSet<>();

    /**
     * Etat actuel du membre.
     */
    private AssignmentProtocol assignmentSnapshot = new AssignmentProtocol();
    private int previousAssignmentGenerationId = Generation.NO_GENERATION.generationId;

    public AssignmentCoordinator(
            LogContext logContext,
            ConsumerNetworkClient client,
            String groupId,
            Optional<String> groupInstanceId,
            int rebalanceTimeoutMs,
            int sessionTimeoutMs,
            int heartbeatIntervalMs,
            Metrics metrics,
            String metricGrpPrefix,
            Time time,
            long retryBackoffMs) {

        super(new GroupRebalanceConfig(
                sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId,
                groupInstanceId,
                retryBackoffMs,
                true
        ), logContext, client, metrics, metricGrpPrefix, time);
    }

    public void registerListener(AssignmentListener listener) {
        this.listeners.add(listener);
    }

    public void unregisterListener(AssignmentListener listener) {
        this.listeners.remove(listener);
    }

    @Override
    protected String protocolType() {
        return PROTOCOL_TYPE;
    }

    @Override
    protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        return new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName(SUB_PROTOCOL_TYPE_V0)
                        .setMetadata(assignmentSnapshot.serialize().array())).iterator());
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        try {
            if (generation == Generation.NO_GENERATION.generationId &&
                    memberId.equals(Generation.NO_GENERATION.memberId)) {
                LOGGER.info("Giving away assignment as lost since generation has been reset," +
                        "indicating that client is no longer part of the group");
                listeners.forEach(listener -> listener.onAssignmentLost(memberId, assignmentSnapshot));
                // Reset snapshot when assignment lost
                assignmentSnapshot = new AssignmentProtocol();
            } else {
                LOGGER.debug("Revoking previous assignment {}", assignmentSnapshot);
                listeners.forEach(listener -> listener.onAssignmentRevoked(memberId, assignmentSnapshot));
            }
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("User rebalance callback throws an error", e);
        }
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId, String protocol, List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
        LOGGER.debug("Performing assignment");

        Generation generation = generation();

        Map<String, AssignmentProtocol> memberConfigs = new HashMap<>();
        for (JoinGroupResponseData.JoinGroupResponseMember entry : allMemberMetadata) {
            AssignmentProtocol nodeMetadata
                    = AssignmentProtocol.deserializeAssignment(ByteBuffer.wrap(entry.metadata()));
            if (nodeMetadata.getGenerationId() != Generation.NO_GENERATION.generationId &&
                    previousAssignmentGenerationId != Generation.NO_GENERATION.generationId &&
                    previousAssignmentGenerationId != nodeMetadata.getGenerationId()) {
                LOGGER.warn("Node {} generation {} unmatched previous version {}. Other assignments may have occured. " +
                                "Giving away current assignment.",
                        entry.memberId(), nodeMetadata.getGenerationId(), previousAssignmentGenerationId);
                nodeMetadata = new AssignmentProtocol();
            }

            nodeMetadata.setLeaderId(leaderId);
            nodeMetadata.setGenerationId(generation.generationId);
            memberConfigs.put(entry.memberId(), nodeMetadata);
        }

        Map<String, AssignmentProtocol> assignments = memberConfigs;
        for (AssignmentListener assigner : listeners) {
            assignments = assigner.assign(leaderId, memberConfigs);
        }

        LOGGER.debug("Member information: {} Assignments: {}", memberConfigs, assignments);
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> AssignmentProtocol.serializeAssignment(e.getValue())));
    }

    @Override
    protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {
        try {
            assignmentSnapshot = AssignmentProtocol.deserializeAssignment(memberAssignment);
            LOGGER.debug("Updated assignment {}", assignmentSnapshot);

            if (generation != Generation.NO_GENERATION.generationId &&
                    assignmentSnapshot.getGenerationId() != Generation.NO_GENERATION.generationId &&
                    generation != assignmentSnapshot.getGenerationId()) {
                LOGGER.warn("assignment generation-id {} mismatch current generation-id {}",
                        assignmentSnapshot.getGenerationId(), generation);
                requestRejoin("assignment generation-id mismatch current generation-id");
            }

            listeners.forEach(listener -> listener.onAssigned(memberId, assignmentSnapshot,
                    assignmentSnapshot.getLeaderId(), generation));
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (SchemaException e) {
            requestRejoin(e.getMessage());
        } catch (Exception e) {
            throw new KafkaException("User rebalance callback throws an error", e);
        } finally {
            previousAssignmentGenerationId = generation;
        }
    }

    @Override
    protected synchronized boolean rejoinNeededOrPending() {
        boolean isRejoinNeeded = false;
        for (AssignmentListener assigner : listeners) {
            isRejoinNeeded |= assigner.isRejoinNeeded();
        }
        if (isRejoinNeeded) {
            LOGGER.debug("assigner requested rejoin");
            requestRejoin("assigner requested rejoin");
        }
        return super.rejoinNeededOrPending();
    }

    public void poll(Timer timer) {
        if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
            throw new TimeoutException("timeout on waiting for coordinator");
        }

        if (rejoinNeededOrPending()) {
            ensureActiveGroup();
        }

        pollHeartbeat(timer.currentTimeMs());

        // Note that because the network client is shared with the background heartbeat thread,
        // we do not want to block in poll longer than the time to the next heartbeat.
        do {
            client.poll(timer);
        } while (assignmentSnapshot.getLeaderId() == null && timer.notExpired());

        if (timer.isExpired()) {
            throw new TimeoutException("timeout on waiting for assignment");
        }

        timer.update();
    }

    @Override
    protected void close(Timer timer) {
        client.disableWakeups();
        try {
            super.close(timer);

            while (client.hasPendingRequests() && timer.notExpired()) {
                client.poll(timer);
            }
        } finally {
            assignmentSnapshot = new AssignmentProtocol();
        }
    }
}
