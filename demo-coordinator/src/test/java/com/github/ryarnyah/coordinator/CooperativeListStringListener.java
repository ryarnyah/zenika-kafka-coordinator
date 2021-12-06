package com.github.ryarnyah.coordinator;

import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.utils.CircularIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class CooperativeListStringListener implements AssignmentListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CooperativeListStringListener.class);

    private List<String> valueAssigned = null;
    private final Set<String> initialAssign;
    private boolean forceRejoin = false;

    private static final String ASSIGN_KEY = "assign_key";
    private static final Schema SCHEMA = new Schema(
            new Field(ASSIGN_KEY, new ArrayOf(Type.STRING))
    );

    private ByteBuffer serialize(List<String> assign) {
        Struct struct = new Struct(SCHEMA);
        struct.set(ASSIGN_KEY, assign.toArray(new String[0]));
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();
        return buffer;
    }

    private List<String> deserialize(ByteBuffer data) {
        List<String> res = new ArrayList<>();
        if (data.remaining() > 0) {
            Struct struct = SCHEMA.read(data);
            for (Object o : struct.getArray(ASSIGN_KEY)) {
                String v = (String) o;
                res.add(v);
            }
        }
        return res;
    }

    public CooperativeListStringListener(Set<String> initialAssign) {
        this.initialAssign = initialAssign;
    }

    @Override
    public Map<String, AssignmentProtocol> assign(String leaderId, Map<String, AssignmentProtocol> memberConfigs) {
        Map<String, List<String>> initialAssignments =
                memberConfigs.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> deserialize(ByteBuffer.wrap(e.getValue().getAssignedTasks()))
                ));

        Map<String, ArrayDeque<String>> assignments =
                memberConfigs.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            List<String> assign = deserialize(ByteBuffer.wrap(e.getValue().getAssignedTasks()));
                            Collections.sort(assign);
                            return new ArrayDeque<>(assign);
                        }
                ));

        double mean = initialAssign.size() * 1.0 / memberConfigs.size();

        // Remove already assigned values
        List<String> toAssignValues = new ArrayList<>(initialAssign);
        assignments.forEach((k, v) -> toAssignValues.removeAll(v));

        // Remove assignments for members that have too much
        for (Map.Entry<String, ArrayDeque<String>> entry: assignments.entrySet()) {
            while (entry.getValue().size() > mean) {
                toAssignValues.add(entry.getValue().pop());
            }
        }
        Collections.sort(toAssignValues);

        ArrayDeque<String> dequeueToAssign = new ArrayDeque<>(toAssignValues);
        // Add values to member with less than mean
        for (Map.Entry<String, ArrayDeque<String>> entry: assignments.entrySet()) {
            while (entry.getValue().size() < mean && !dequeueToAssign.isEmpty()) {
                entry.getValue().add(dequeueToAssign.pop());
            }
        }

        // Round-robin the rest
        CircularIterator<Map.Entry<String, ArrayDeque<String>>> circularIterator
                = new CircularIterator<>(assignments.entrySet());
        while (!dequeueToAssign.isEmpty()) {
            String strToAssign = dequeueToAssign.pop();
            circularIterator.next().getValue().add(strToAssign);
        }

        // Only send non-revoked assignments in cooperative algorithm
        Set<String> revokedAssignments = new HashSet<>();
        for (Map.Entry<String, ArrayDeque<String>> assignment : assignments.entrySet()) {
            List<String> revokedAssignment = initialAssignments.get(assignment.getKey());
            revokedAssignment.removeAll(assignment.getValue());
            revokedAssignments.addAll(revokedAssignment);
        }
        for (Map.Entry<String, ArrayDeque<String>> assignment : assignments.entrySet()) {
            assignment.getValue().removeAll(revokedAssignments);
        }
        if (!revokedAssignments.isEmpty()) {
            forceRejoin = true;
        }

        // Map assignments
        memberConfigs.forEach((k, v) -> v.setAssignedTasks(serialize(
                new ArrayList<>(assignments.get(k))
        ).array()));
        return memberConfigs;
    }

    @Override
    public AssignmentProtocol onAssigned(String memberId, AssignmentProtocol assignment, String leaderId, int generation) {
        forceRejoin = false;
        List<String> newAssignment = deserialize(ByteBuffer.wrap(assignment.getAssignedTasks()));
        // Try to know if there is assignment that has been removed to request rebalance
        if (valueAssigned != null) {
            List<String> removedAssignments = new ArrayList<>(valueAssigned);
            removedAssignments.removeAll(newAssignment);
            if (!removedAssignments.isEmpty()) {
                forceRejoin = true;
                LOGGER.info("removed {}", removedAssignments);
            }
        }
        valueAssigned = newAssignment;
        LOGGER.info("assigned {}", valueAssigned);
        return assignment;
    }

    @Override
    public AssignmentProtocol onAssignmentRevoked(String memberId, AssignmentProtocol assignment) {
        return assignment;
    }

    @Override
    public AssignmentProtocol onAssignmentLost(String memberId, AssignmentProtocol assignment) {
        valueAssigned = null;
        LOGGER.info("lost {}", assignment);
        return AssignmentListener.super.onAssignmentLost(memberId, assignment);
    }

    @Override
    public boolean isRejoinNeeded() {
        return forceRejoin || valueAssigned == null;
    }

    public List<String> getValueAssigned() {
        return valueAssigned;
    }

    public void reset() {
        valueAssigned = null;
    }
}