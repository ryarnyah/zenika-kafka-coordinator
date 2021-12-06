package com.github.ryarnyah.coordinator;

import java.util.Map;

public interface AssignmentListener {
    /**
     * Invoked when a new assignment is created by joining the group. This is
     * invoked for both successful and unsuccessful assignments.
     */
    default AssignmentProtocol onAssigned(String memberId, AssignmentProtocol assignment,
                                          String leaderId, int generation) {
        return assignment;
    }

    /**
     * Invoked when a rebalance operation starts, revoking leadership
     */
    default AssignmentProtocol onAssignmentRevoked(String memberId, AssignmentProtocol assignment) {
        return assignment;
    }

    /**
     * Invoked when assignment is lost.
     */
    default AssignmentProtocol onAssignmentLost(String memberId, AssignmentProtocol assignment) {
        return onAssignmentRevoked(memberId, assignment);
    }

    /**
     * Assign tasks to members.
     *
     * @param leaderId      Leader elected for this group.
     * @param memberConfigs existing members config.
     * @return updated members configurations.
     */
    default Map<String, AssignmentProtocol> assign(String leaderId,
                                                   Map<String, AssignmentProtocol> memberConfigs) {
        return memberConfigs;
    }

    /**
     * <code>true</code> if rejoin is needed.
     */
    default boolean isRejoinNeeded() {
        return false;
    }
}
