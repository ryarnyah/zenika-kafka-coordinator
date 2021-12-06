package com.github.ryarnyah.coordinator;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

public class AssignmentManager implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssignmentManager.class);

    /**
     * Metadata du cluster Kafka.
     */
    private final Metadata metadata;

    /**
     * Network client.
     */
    private final ConsumerNetworkClient client;
    /**
     * Assignment Manager coordinator.
     */
    private final AssignmentCoordinator coordinator;

    private final ChannelBuilder channelBuilder;

    /**
     * Default timeout (used to wait until coordinator shutdown).
     */
    private final long defaultApiTimeoutMs;

    public AssignmentManager(
            AssignmentManagerConfig config,
            Metrics metrics
    ) {
        String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        Time time = Time.SYSTEM;

        String metricGrpPrefix = "assignment-manager-" + clientId;

        long retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
        String groupId = config.getString(CommonClientConfigs.GROUP_ID_CONFIG);
        LogContext logContext = new LogContext("[AssignmentManager clientId=" + clientId + ", groupId="
                + groupId + "] ");
        this.metadata = new Metadata(
                retryBackoffMs,
                config.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
                logContext,
                new ClusterResourceListeners()
        );
        List<String> bootstrapServers
                = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(bootstrapServers,
                config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG));
        this.metadata.bootstrap(addresses);

        this.channelBuilder = ClientUtils.createChannelBuilder(
                config,
                time,
                logContext);
        long maxIdleMs = config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG);

        Optional<String> groupInstanceId =
                Optional.ofNullable(config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG));
        groupInstanceId.ifPresent(JoinGroupRequest::validateGroupInstanceId);

        NetworkClient netClient = new NetworkClient(
                new Selector(maxIdleMs, metrics, time, metricGrpPrefix, channelBuilder, logContext),
                this.metadata,
                clientId,
                100, // a fixed large enough value will suffice
                config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
                config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time,
                true,
                new ApiVersions(),
                logContext);

        this.client = new ConsumerNetworkClient(
                logContext,
                netClient,
                metadata,
                time,
                retryBackoffMs,
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                Integer.MAX_VALUE
        );

        this.coordinator = new AssignmentCoordinator(
                logContext,
                this.client,
                groupId,
                groupInstanceId,
                config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG),
                config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG),
                config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG),
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs
        );
        this.defaultApiTimeoutMs = config.getLong(AssignmentManagerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
    }

    /**
     * Must be called periodically to manage task distribution.
     */
    public void poll() {
        try {
            this.coordinator.poll(Time.SYSTEM.timer(defaultApiTimeoutMs));
        } catch (WakeupException e) {
            // Do nothing
        }
    }

    public void registerListener(AssignmentListener listener) {
        this.coordinator.registerListener(listener);
    }

    public void unregisterListener(AssignmentListener listener) {
        this.coordinator.unregisterListener(listener);
    }

    @Override
    public void close() {
        if (client != null) {
            client.wakeup();
        }
        if (coordinator != null) {
            try {
                coordinator.close(Time.SYSTEM.timer(defaultApiTimeoutMs));
            } catch (RuntimeException t) {
                LOGGER.warn("Failed to close coordinator with type {}", this.getClass().getName(), t);
            }
        }
        Utils.closeQuietly(client, "client");
        Utils.closeQuietly(metadata, "metadata");
        Utils.closeQuietly(channelBuilder, "channelBuilder");
        LOGGER.info("assignment manager closed.");
    }
}
