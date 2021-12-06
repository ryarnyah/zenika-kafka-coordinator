package com.github.ryarnyah.coordinator;

public class LeaderListener implements AssignmentListener {
    private Boolean isLeader = null;

    @Override
    public AssignmentProtocol onAssigned(String memberId, AssignmentProtocol assignment, String leaderId, int generation) {
        isLeader = memberId.equals(leaderId);
        return AssignmentListener.super.onAssigned(memberId, assignment, leaderId, generation);
    }

    @Override
    public AssignmentProtocol onAssignmentLost(String memberId, AssignmentProtocol assignment) {
        isLeader = null;
        return AssignmentListener.super.onAssignmentLost(memberId, assignment);
    }

    @Override
    public boolean isRejoinNeeded() {
        return isLeader == null;
    }

    public boolean isLeader() {
        return isLeader != null && isLeader;
    }
}
