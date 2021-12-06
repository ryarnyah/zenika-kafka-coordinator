package com.github.ryarnyah.coordinator;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.Sensor;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.CommonClientConfigs.*;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.common.config.SecurityConfig.SECURITY_PROVIDERS_CONFIG;
import static org.apache.kafka.common.config.SecurityConfig.SECURITY_PROVIDERS_DOC;

public class AssignmentManagerConfig extends AbstractConfig {
    private static final AtomicInteger SR_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * <code>default.api.timeout.ms</code>
     */
    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG;

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(DEFAULT_API_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC)
                .define(REBALANCE_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        300000,
                        ConfigDef.Importance.LOW,
                        REBALANCE_TIMEOUT_MS_DOC)
                .define(SESSION_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        10000,
                        ConfigDef.Importance.LOW,
                        SESSION_TIMEOUT_MS_DOC)
                .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.INT,
                        3000,
                        ConfigDef.Importance.LOW,
                        HEARTBEAT_INTERVAL_MS_DOC)
                .define(CLIENT_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        "assignment-manager-" + SR_CLIENT_ID_SEQUENCE.getAndIncrement(),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.CLIENT_ID_DOC)
                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        30000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(METRICS_NUM_SAMPLES_CONFIG,
                        ConfigDef.Type.INT,
                        2,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(METRICS_RECORDING_LEVEL_CONFIG,
                        ConfigDef.Type.STRING,
                        Sensor.RecordingLevel.INFO.toString(),
                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                .define(METRIC_REPORTER_CLASSES_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(RETRY_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        100L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .define(GROUP_ID_CONFIG, ConfigDef.Type.STRING, "assignment-manager", ConfigDef.Importance.HIGH, GROUP_ID_DOC)
                .define(GROUP_INSTANCE_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        GROUP_INSTANCE_ID_DOC)
                .define(METADATA_MAX_AGE_CONFIG,
                        ConfigDef.Type.LONG,
                        5 * 60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                .define(BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.HIGH,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(CLIENT_DNS_LOOKUP_CONFIG,
                        ConfigDef.Type.STRING,
                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        9 * 60 * 1000,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                .define(RECONNECT_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        50L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        1000L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                .define(SEND_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        128 * 1024,
                        atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SEND_BUFFER_DOC)
                .define(RECEIVE_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        64 * 1024,
                        atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        30000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        REQUEST_TIMEOUT_MS_DOC)
                .define(SECURITY_PROVIDERS_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        SECURITY_PROVIDERS_DOC)
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .withClientSslSupport()
                .withClientSaslSupport();
    }

    public AssignmentManagerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }
}
