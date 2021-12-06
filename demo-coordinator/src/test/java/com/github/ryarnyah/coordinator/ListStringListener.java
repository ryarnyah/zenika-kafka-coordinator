package com.github.ryarnyah.coordinator;

import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.utils.CircularIterator;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class ListStringListener implements AssignmentListener {
    private Set<String> valueAssigned = null;
    private final Set<String> initialAssign;

    private static final String ASSIGN_KEY = "assign_key";
    private static final Schema SCHEMA = new Schema(
            new Field(ASSIGN_KEY, new ArrayOf(Type.STRING))
    );

    private ByteBuffer serialize(Set<String> assign) {
        Struct struct = new Struct(SCHEMA);
        struct.set(ASSIGN_KEY, assign.toArray(new String[0]));
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();
        return buffer;
    }

    private Set<String> deserialize(ByteBuffer data) {
        Set<String> res = new HashSet<>();
        if (data.remaining() > 0) {
            Struct struct = SCHEMA.read(data);
            for (Object o : struct.getArray(ASSIGN_KEY)) {
                String v = (String) o;
                res.add(v);
            }
        }
        return res;
    }

    public ListStringListener(Set<String> initialAssign) {
        this.initialAssign = initialAssign;
    }

    @Override
    public Map<String, AssignmentProtocol> assign(String leaderId, Map<String, AssignmentProtocol> memberConfigs) {
        Map<String, Set<String>> assignments =
                memberConfigs.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new HashSet<>()
                ));
        Set<String> toAssignValues = new HashSet<>(initialAssign);

        // Round-robin assignment
        CircularIterator<Map.Entry<String, Set<String>>> circularIterator
                = new CircularIterator<>(assignments.entrySet());
        ArrayDeque<String> toAssign = new ArrayDeque<>(toAssignValues);
        while (!toAssign.isEmpty()) {
            String strToAssign = toAssign.pop();
            circularIterator.next().getValue().add(strToAssign);
        }

        memberConfigs.forEach((k, v) -> v.setAssignedTasks(serialize(assignments.get(k)).array()));
        return memberConfigs;
    }

    @Override
    public AssignmentProtocol onAssigned(String memberId, AssignmentProtocol assignment, String leaderId, int generation) {
        valueAssigned = deserialize(ByteBuffer.wrap(assignment.getAssignedTasks()));
        return AssignmentListener.super.onAssigned(memberId, assignment, leaderId, generation);
    }

    @Override
    public AssignmentProtocol onAssignmentRevoked(String memberId, AssignmentProtocol assignment) {
        Set<String> initiallyAssignedValues = deserialize(ByteBuffer.wrap(assignment.getAssignedTasks()));
        if (valueAssigned != null) {
            valueAssigned.removeAll(initiallyAssignedValues);
        }
        return AssignmentListener.super.onAssignmentRevoked(memberId, assignment);
    }

    @Override
    public AssignmentProtocol onAssignmentLost(String memberId, AssignmentProtocol assignment) {
        valueAssigned = null;
        return AssignmentListener.super.onAssignmentLost(memberId, assignment);
    }

    @Override
    public boolean isRejoinNeeded() {
        return valueAssigned == null;
    }

    public Set<String> getValueAssigned() {
        return valueAssigned;
    }
}
