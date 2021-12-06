package com.github.ryarnyah.coordinator;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.utils.Crc32C;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class AssignmentProtocol {
    private String leaderId;
    private int generationId = -1;
    private byte[] assignedTasks = new byte[0];

    /**
     * Header Schema
     */
    private static final short PROTOCOL_V0 = 0;
    private static final String VERSION_KEY_NAME = "version";
    private static final Schema TASK_PROTOCOL_HEADER_SCHEMA = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16, "Schema version"));
    private static final Struct PROTOCOL_HEADER_V0 = new Struct(TASK_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, PROTOCOL_V0);

    /**
     * Schema
     */
    private static final String LEADER_ID_KEY = "leader_id";
    private static final String ASSIGNED_TASKS_KEY = "assigned_tasks";
    private static final String GENERATION_KEY = "generation_key";

    private static final Schema SCHEMA_V0 = new Schema(
            new Field(GENERATION_KEY, Type.INT32, "Generation of assignment"),
            new Field(LEADER_ID_KEY, Type.STRING, "MemberId of group leader", ""),
            new Field(ASSIGNED_TASKS_KEY, Type.BYTES, "Current groups assigned")
    );

    public AssignmentProtocol() {}

    public void setAssignedTasks(byte[] assignedTasks) {
        this.assignedTasks = assignedTasks;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public void setGenerationId(int generationId) {
        this.generationId = generationId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public int getGenerationId() {
        return generationId;
    }

    public byte[] getAssignedTasks() {
        return assignedTasks;
    }

    private Struct struct() {
        Struct struct = new Struct(SCHEMA_V0);
        struct.set(LEADER_ID_KEY, leaderId);
        struct.set(GENERATION_KEY, generationId);
        struct.setByteArray(ASSIGNED_TASKS_KEY, assignedTasks);
        return struct;
    }

    public ByteBuffer serialize() {
        Struct struct = struct();
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf() + PROTOCOL_HEADER_V0.sizeOf() + Long.BYTES);
        TASK_PROTOCOL_HEADER_SCHEMA.write(buffer, PROTOCOL_HEADER_V0);
        SCHEMA_V0.write(buffer, struct);
        long crc = Crc32C.compute(buffer.array(), 0, buffer.limit() - Long.BYTES);
        buffer.putLong(crc);

        buffer.flip();
        return buffer;
    }

    public void fromBytes(ByteBuffer data) {
        // Check size
        if (data.remaining() < (Long.BYTES + 1)) {
            throw new CorruptAssignmentException("Corrupt Record size " + data.remaining());
        }

        // Check CRC
        data.position(data.limit() - Long.BYTES);
        long expectedCrc = data.getLong();
        long crc = Crc32C.compute(data.array(), 0, data.limit() - Long.BYTES);
        if (crc != expectedCrc) {
            throw new CorruptAssignmentException("Corrupt CRC: got " + crc + " expected " + expectedCrc);
        }
        data.rewind();

        // Reader header first
        Struct header = TASK_PROTOCOL_HEADER_SCHEMA.read(data);
        Short version = header.getShort(VERSION_KEY_NAME);
        Struct struct;
        if (version == PROTOCOL_V0) {
            struct = SCHEMA_V0.read(data);
        } else {
            throw new UnsupportedVersionException("Unsupported subscription version: " + version);
        }

        assignedTasks = struct.getByteArray(ASSIGNED_TASKS_KEY);
        generationId = struct.getInt(GENERATION_KEY);
        leaderId = struct.getString(LEADER_ID_KEY);
    }

    public static ByteBuffer serializeAssignment(AssignmentProtocol assignment) {
        return assignment.serialize();
    }

    public static AssignmentProtocol deserializeAssignment(
            ByteBuffer buffer) {
        AssignmentProtocol memberAssignment = new AssignmentProtocol();
        memberAssignment.fromBytes(buffer);
        return memberAssignment;
    }

    @Override
    public String toString() {
        return "AssignmentProtocol{" +
                "leaderId='" + leaderId + '\'' +
                ", generationId=" + generationId +
                ", assignedTasks=" + Arrays.toString(assignedTasks) +
                '}';
    }
}
