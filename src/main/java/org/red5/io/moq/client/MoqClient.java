package org.red5.io.moq.client;

import org.red5.io.moq.catalog.Catalog;
import org.red5.io.moq.model.*;
import org.red5.io.moq.protocol.MoqMessageType;
import org.red5.io.moq.protocol.VarInt;
import org.red5.io.moq.timeline.Timeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MOQ Transport client using QUIC with Netty.
 * Implements MOQ Transport Draft 14.
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/draft-ietf-moq-transport-14">MOQ Transport Draft 14</a>
 */
public class MoqClient {
    private static final Logger logger = LoggerFactory.getLogger(MoqClient.class);

    private final String endpoint;
    private final boolean validateCert;
    private final BlockingQueue<List<TrackEvent>> eventQueue;
    private final AtomicBoolean continuePublishing;
    private final Map<String, Channel> subscribers;
    private final Catalog catalog;
    private final Timeline timeline;
    private final String namespace;
    private final AtomicBoolean goAwayReceived = new AtomicBoolean(false);
    private final AtomicBoolean namespacePublished = new AtomicBoolean(false);

    // Latch to signal when publisher is ready (has received PUBLISH_OK for media tracks)
    // This prevents frames from being processed before subscriptions exist
    private CountDownLatch publisherReadyLatch;

    private QuicChannel quicChannel;
    private QuicStreamChannel controlStream;
    private long requestIdCounter = 0; // Client uses even request IDs

    // Track active subscriptions (subscribeId -> Subscription)
    private final Map<Long, Subscription> subscriptions = new ConcurrentHashMap<>();

    // Track namespace subscriptions (subscribeId -> List of track subscriptions)
    private final Map<Long, List<Subscription>> namespaceSubscriptions = new ConcurrentHashMap<>();

    // Track active streams per subscription (subscribeId:groupId -> stream)
    // One stream is reused for all objects in the same group
    private final Map<String, QuicStreamChannel> activeGroupStreams = new ConcurrentHashMap<>();

    // Track last object ID sent on each stream (streamKey -> lastObjectId)
    // Needed for computing object ID deltas in SUBGROUP_HEADER format
    private final Map<String, Long> streamLastObjectId = new ConcurrentHashMap<>();

    // Catalog and Timeline versioning
    // For catalog/timeline tracks, we use incrementing group IDs as versions
    // Object ID is always 0 (single object per group)
    private final AtomicLong catalogVersion = new AtomicLong(0);
    private final AtomicLong timelineVersion = new AtomicLong(0);

    // Map track names to track aliases for easier lookup
    private final Map<String, Long> trackAliases = new ConcurrentHashMap<>();

    // Track pending PUBLISH requests (requestId -> TrackInfo)
    private final Map<Long, TrackInfo> pendingPublishes = new ConcurrentHashMap<>();

    // Helper record to store track information for PUBLISH requests
    private record TrackInfo(String trackName, long trackAlias) {}

    public MoqClient(String endpoint, boolean validateCert, BlockingQueue<List<TrackEvent>> eventQueue) {
        this(endpoint, validateCert, eventQueue, "red5moq");
    }

    public MoqClient(String endpoint, boolean validateCert, BlockingQueue<List<TrackEvent>> eventQueue, String namespace) {
        this.endpoint = endpoint;
        this.validateCert = validateCert;
        this.eventQueue = eventQueue;
        this.namespace = namespace;
        this.continuePublishing = new AtomicBoolean(true);
        this.subscribers = new ConcurrentHashMap<>();
        this.catalog = new Catalog();
        this.timeline = new Timeline();
    }

    /**
     * Get next client request ID (even numbers, increments by 2).
     */
    private synchronized long nextRequestId() {
        long id = requestIdCounter;
        requestIdCounter += 2;
        return id;
    }

    /**
     * Get the latch that signals when publisher is ready to accept frames.
     * This latch counts down when PUBLISH_OK responses are received for media tracks.
     * External callers (like MoqPublisher) can wait on this to ensure subscriptions
     * exist before starting to send frames.
     */
    public CountDownLatch getPublisherReadyLatch() {
        return publisherReadyLatch;
    }

    /**
     * Run the client - connect, setup, and publish.
     */
    public void run() throws Exception {
        // Start event loop in separate thread
        startEventLoop();

        // Parse endpoint URL
        String host;
        int port;
        if (endpoint.startsWith("https://")) {
            String hostPort = endpoint.substring(8);
            int colonIdx = hostPort.indexOf(':');
            if (colonIdx > 0) {
                host = hostPort.substring(0, colonIdx);
                port = Integer.parseInt(hostPort.substring(colonIdx + 1));
            } else {
                host = hostPort;
                port = 4433;
            }
        } else {
            host = "127.0.0.1";
            port = 4433;
        }

        logger.info("Connecting to {}:{} with ALPN: moq-00", host, port);

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            // Configure QUIC client with MOQ Transport ALPN
            // Use "moq-00" for native MOQ QUIC (not "h3" which is for WebTransport/HTTP3)
            QuicSslContext sslContext;
            if (!validateCert) {
                sslContext = QuicSslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .applicationProtocols("moq-00")
                        .build();
            } else {
                sslContext = QuicSslContextBuilder.forClient()
                        .applicationProtocols("moq-00")
                        .build();
            }

            QuicClientCodecBuilder codecBuilder = new QuicClientCodecBuilder()
                    .sslContext(sslContext)
                    .maxIdleTimeout(300000, TimeUnit.MILLISECONDS)  // 5 minutes (increased from 5 seconds)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamDataUnidirectional(1000000)
                    .initialMaxStreamsBidirectional(100)
                    .initialMaxStreamsUnidirectional(100);

            Bootstrap bs = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(codecBuilder.build());

            Channel channel = bs.bind(0).sync().channel();

            QuicChannelBootstrap quicBootstrap = QuicChannel.newBootstrap(channel)
                    .streamHandler(new ChannelInboundHandlerAdapter())
                    .remoteAddress(new InetSocketAddress(host, port));

            io.netty.util.concurrent.Future<QuicChannel> connectFuture = quicBootstrap.connect();
            quicChannel = connectFuture.sync().getNow();

            logger.info("QUIC connection established (ALPN: moq-00, draft-14)");

            // Open bidirectional control stream
            QuicStreamChannelBootstrap streamBootstrap = quicChannel.newStreamBootstrap()
                    .handler(new ControlStreamHandler())
                    .type(QuicStreamType.BIDIRECTIONAL);

            io.netty.util.concurrent.Future<QuicStreamChannel> streamFuture = streamBootstrap.create();
            controlStream = streamFuture.sync().getNow();
            logger.info("Control stream opened");

            // Send client setup
            sendClientSetup();

            // Wait for server setup
            receiveServerSetup();

            // Start publisher
            startPublisher();

            // Keep running
            while (continuePublishing.get()) {
                Thread.sleep(100);
            }

        } finally {
            group.shutdownGracefully();
        }
    }

    /**
     * Send CLIENT_SETUP message (Draft 14 format).
     * Format: message_type (varint) + message_length (u16) + payload
     * Payload: num_versions + versions[] + num_params + params[]
     */
    private void sendClientSetup() {
        logger.info("Sending CLIENT_SETUP with version negotiation");

        // First, build the payload in a separate buffer
        ByteBuf payload = Unpooled.buffer();

        // Get supported versions (in order of preference, newest first)
        long[] supportedVersions = MoqMessageType.getSupportedVersions();

        // Number of supported versions (varint)
        VarInt.write(payload, supportedVersions.length);

        // Write all supported versions (newest first for server to pick best)
        for (long version : supportedVersions) {
            VarInt.write(payload, version);
        }

        // Number of parameters (varint)
        VarInt.write(payload, 1);

        // Parameter: ROLE = PUBLISHER
        // Per Draft-14 Section 1.4.2: EVEN parameter types have NO length field
        // Type (i) + Value (i) - for even types, value is a single varint
        VarInt.write(payload, MoqMessageType.PARAM_ROLE);      // Type: 0x00 (EVEN)
        VarInt.write(payload, MoqMessageType.ROLE_PUBLISHER);  // Value: 0x01 (varint, no length)

        // Now build the complete message with length prefix
        ByteBuf setupMessage = Unpooled.buffer();

        // Message type (varint)
        VarInt.write(setupMessage, MoqMessageType.CLIENT_SETUP.getValue());

        // Message length (u16 - 2 bytes, big-endian)
        int payloadLength = payload.readableBytes();
        setupMessage.writeShort(payloadLength);

        // Payload
        setupMessage.writeBytes(payload);
        payload.release();

        logger.info("CLIENT_SETUP: Sending {} bytes total ({} payload) on control stream",
                setupMessage.readableBytes(), payloadLength);
        controlStream.writeAndFlush(setupMessage);

        // Log supported version(s)
        logger.info("CLIENT_SETUP sent: version=0x{}, role=PUBLISHER",
                    String.format("%08X", MoqMessageType.VERSION_DRAFT_14));
        logger.info("Note: Only draft-14 supported (CLIENT_SETUP=0x20). Earlier drafts used 0x40.");
    }

    private void receiveServerSetup() throws InterruptedException {
        logger.info("Waiting for SERVER_SETUP from server...");
        // SERVER_SETUP is now parsed asynchronously by ControlStreamHandler
        // Give it time to arrive and be processed
        Thread.sleep(200);
    }

    /**
     * Send PUBLISH_NAMESPACE message (Draft 14 format).
     * Format: message_type (varint) + message_length (u16) + payload
     * Payload: request_id + track_namespace_prefix + num_params + params[]
     * NOTE: Draft-14 REQUIRES request_id field (Section 9.23, Line 3744)
     */
    private void sendPublishNamespace() {
        logger.info("Sending PUBLISH_NAMESPACE for namespace: {}", namespace);

        // First, build the payload in a separate buffer
        ByteBuf payload = Unpooled.buffer();

        // Request ID (varint) - Required in draft-14
        long requestId = nextRequestId();
        VarInt.write(payload, requestId);

        // Track Namespace Prefix (tuple of strings)
        // For simplicity, we use a single-element tuple
        String[] namespaceParts = namespace.split("/");
        VarInt.write(payload, namespaceParts.length); // tuple length

        for (String part : namespaceParts) {
            VarInt.writeString(payload, part);
        }

        // Number of parameters (varint)
        VarInt.write(payload, 0);

        // Now build the complete message with length prefix
        ByteBuf message = Unpooled.buffer();

        // Message type (varint)
        VarInt.write(message, MoqMessageType.PUBLISH_NAMESPACE.getValue());

        // Message length (u16 - 2 bytes, big-endian)
        int payloadLength = payload.readableBytes();
        message.writeShort(payloadLength);

        // Payload
        message.writeBytes(payload);
        payload.release();

        logger.info("PUBLISH_NAMESPACE: Sending {} bytes total ({} payload), type=0x{}, requestId={}",
                message.readableBytes(), payloadLength, Integer.toHexString(MoqMessageType.PUBLISH_NAMESPACE.getValue()), requestId);

        if (controlStream == null || !controlStream.isActive()) {
            logger.error("Control stream is null or inactive! Cannot send PUBLISH_NAMESPACE");
            return;
        }

        controlStream.writeAndFlush(message);
        logger.info("PUBLISH_NAMESPACE sent: requestId={}, namespace={}", requestId, namespace);

        // Mark namespace as published - now PUBLISH messages can be sent
        namespacePublished.set(true);
    }

    /**
     * Sends PUBLISH message for an individual track (Draft 14 Section 9.13).
     *
     * PUBLISH Message {
     *   Type (i) = 0x1D,
     *   Length (i),
     *   Request ID (i),
     *   Track Namespace (tuple),
     *   Track Name Length (i),
     *   Track Name (..),
     *   Track Alias (i),
     *   Group Order (8),
     *   Content Exists (8),
     *   [Largest Location (Location),]  // only if Content Exists = 1
     *   Forward (8),
     *   Number of Parameters (i),
     *   Parameters (..) ...,
     * }
     */
    private void sendPublish(String trackName, long trackAlias) {
        logger.info("Sending PUBLISH for track: {}, alias: {}", trackName, trackAlias);

        ByteBuf payload = Unpooled.buffer();

        // Request ID (varint)
        long requestId = nextRequestId();
        VarInt.write(payload, requestId);

        // Store pending PUBLISH for later PUBLISH_OK matching
        pendingPublishes.put(requestId, new TrackInfo(trackName, trackAlias));

        // Track Namespace (tuple of strings)
        String[] namespaceParts = namespace.split("/");
        VarInt.write(payload, namespaceParts.length);
        for (String part : namespaceParts) {
            VarInt.writeString(payload, part);
        }

        // Track Name Length + Track Name
        VarInt.writeString(payload, trackName);

        // Track Alias (varint)
        VarInt.write(payload, trackAlias);

        // Group Order (u8) - 0x01 = Ascending
        payload.writeByte(0x01);

        // Content Exists (u8) - 0x00 = no objects yet
        payload.writeByte(0x00);

        // Largest Location omitted since Content Exists = 0

        // Forward (u8) - 0x01 = send immediately
        payload.writeByte(0x01);

        // Number of Parameters (varint)
        VarInt.write(payload, 0);

        // Build message with type and length
        ByteBuf message = Unpooled.buffer();
        VarInt.write(message, MoqMessageType.PUBLISH.getValue());
        message.writeShort(payload.readableBytes());
        message.writeBytes(payload);
        payload.release();

        if (controlStream == null || !controlStream.isActive()) {
            logger.error("Control stream is null or inactive! Cannot send PUBLISH");
            return;
        }

        controlStream.writeAndFlush(message);
        logger.info("PUBLISH sent: requestId={}, track={}, alias={}", requestId, trackName, trackAlias);
    }

    /**
     * Sends SUBSCRIBE_OK message on control stream (Draft 14 Section 9.8).
     * Format per draft-14:
     * SUBSCRIBE_OK Message {
     *   Type (i) = 0x4,
     *   Length (16),
     *   Request ID (i),
     *   Track Alias (i),
     *   Expires (i),
     *   Group Order (8),
     *   Content Exists (8),
     *   [Largest Location (Location)],
     *   Number of Parameters (i),
     *   Parameters (..) ...
     * }
     */
    private void sendSubscribeOk(long requestId, long trackAlias, long expires, boolean contentExists) {
        logger.info("Sending SUBSCRIBE_OK for request: {}, alias: {}", requestId, trackAlias);

        ByteBuf payload = Unpooled.buffer();

        // Request ID (varint)
        VarInt.write(payload, requestId);

        // Track Alias (varint)
        VarInt.write(payload, trackAlias);

        // Expires (varint) - 0 means no expiration, stays active until UNSUBSCRIBE
        VarInt.write(payload, expires);

        // Group Order (u8) - 0x01 = Ascending
        payload.writeByte(0x01);

        // Content Exists (u8) - 1 byte: 0=does not exist, 1=exists
        payload.writeByte(contentExists ? 1 : 0);

        // Optional Largest Location - only if content exists
        // For now, we'll include it as 0 to indicate we'll send from current position
        if (contentExists) {
            VarInt.write(payload, 0);  // Largest group ID
            VarInt.write(payload, 0);  // Largest object ID within that group
        }

        // Number of Parameters (varint)
        VarInt.write(payload, 0);

        // Build message with type and length
        ByteBuf message = Unpooled.buffer();
        VarInt.write(message, MoqMessageType.SUBSCRIBE_OK.getValue());
        message.writeShort(payload.readableBytes());
        message.writeBytes(payload);
        payload.release();

        controlStream.writeAndFlush(message);
        logger.info("SUBSCRIBE_OK sent for request: {}, alias: {}", requestId, trackAlias);
    }

    /**
     * Sends SUBSCRIBE_NAMESPACE_OK message on control stream (Draft 14 Section 6.5.2).
     * Format per draft-ietf-moq-transport-14:
     * SUBSCRIBE_NAMESPACE_OK Message {
     *   Type (i) = 0x12,
     *   Length (16),
     *   Request ID (i),
     * }
     *
     * NOTE: Only Request ID field! NOT Subscribe ID + Expires
     */
    private void sendSubscribeNamespaceOk(long requestId, long expires) {
        logger.info("Sending SUBSCRIBE_NAMESPACE_OK for request ID: {}", requestId);

        ByteBuf payload = Unpooled.buffer();

        // Request ID (varint) - ONLY field per draft-14 Section 9.29
        VarInt.write(payload, requestId);

        logger.debug("SUBSCRIBE_NAMESPACE_OK: requestId={}, payload size={} bytes", requestId, payload.readableBytes());
        if (payload.readableBytes() <= 10) {
            byte[] payloadBytes = new byte[payload.readableBytes()];
            payload.getBytes(0, payloadBytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : payloadBytes) {
                hex.append(String.format("%02X ", b));
            }
            logger.debug("Payload hex: {}", hex.toString().trim());
        }

        // Build message with type and length
        ByteBuf message = Unpooled.buffer();
        VarInt.write(message, MoqMessageType.SUBSCRIBE_NAMESPACE_OK.getValue());
        message.writeShort(payload.readableBytes());
        message.writeBytes(payload);
        payload.release();

        controlStream.writeAndFlush(message);
        logger.info("SUBSCRIBE_NAMESPACE_OK sent for request ID: {}", requestId);
    }

    /**
     * Sends an object over a unidirectional QUIC stream (Draft 14 Section 7).
     *
     * Stream Management:
     * - One stream is reused for all objects in the same group (GOP)
     * - When a new group starts, the old stream is closed and a new one is opened
     * - This is more efficient than creating a stream per object
     *
     * Format: OBJECT_STREAM Type (i) {
     *   Subscribe ID (i),
     *   Track Alias (i),
     *   Group ID (i),
     *   Object ID (i),
     *   Publisher Priority (i),
     *   Object Status (i),
     *   Object Payload (..)
     * }
     *
     * Object Status values:
     *   0x00 = Normal
     *   0x01 = Object Does Not Exist
     *   0x03 = Group Does Not Exist
     *   0x04 = End Of Track
     *   0x05 = End Of Group
     */
    private void sendObjectStream(long subscribeId, long trackAlias, long groupId, long objectId,
                                  long publisherPriority, int objectStatus, ByteBuf payload) {
        logger.debug("Sending object: subscribeId={}, alias={}, group={}, object={}, status={}, size={}",
                subscribeId, trackAlias, groupId, objectId, objectStatus,
                payload != null ? payload.readableBytes() : 0);

        String streamKey = subscribeId + ":" + groupId;

        // Check if we have an active stream for this subscription+group
        QuicStreamChannel stream = activeGroupStreams.get(streamKey);

        if (stream == null || !stream.isActive()) {
            // Need to create a new stream for this group
            // First, close any previous group stream for this subscription
            closeOldGroupStreams(subscribeId, groupId);

            // Create new unidirectional stream for this group
            final QuicStreamChannel[] streamHolder = new QuicStreamChannel[1];

            quicChannel.createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) {
                    streamHolder[0] = (QuicStreamChannel) ctx.channel();
                    activeGroupStreams.put(streamKey, streamHolder[0]);
                    logger.info("Opened new stream for subscribeId={}, groupId={}", subscribeId, groupId);

                    // Send the first object on the new stream
                    sendObjectOnStream(ctx, subscribeId, trackAlias, groupId, objectId,
                                      publisherPriority, objectStatus, payload);
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                    logger.error("Error in object stream", cause);
                    activeGroupStreams.remove(streamKey);
                    streamLastObjectId.remove(streamKey);
                    ctx.close();
                }
            });
        } else {
            // Reuse existing stream for this group
            logger.debug("Reusing stream for subscribeId={}, groupId={}", subscribeId, groupId);
            sendObjectOnStream(stream.pipeline().firstContext(), subscribeId, trackAlias,
                              groupId, objectId, publisherPriority, objectStatus, payload);
        }
    }

    /**
     * Sends a single object on a SUBGROUP_HEADER stream (Draft 14 Section 10.4.2).
     *
     * For the first object on a new stream, sends SUBGROUP_HEADER:
     *   Stream Type (varint) - 0x10 (SUBGROUP_HEADER)
     *   Track Alias (varint)
     *   Group ID (varint)
     *   Subgroup ID (varint) - 0 for single subgroup
     *   Publisher Priority (8 bits)
     *
     * For each object:
     *   Object ID Delta (varint) - delta from last object ID on this stream
     *   Object Payload Length (varint)
     *   Object Payload (bytes)
     */
    private void sendObjectOnStream(ChannelHandlerContext ctx, long subscribeId, long trackAlias,
                                   long groupId, long objectId, long publisherPriority,
                                   int objectStatus, ByteBuf payload) {
        try {
            String streamKey = subscribeId + ":" + groupId;
            Long lastObjectId = streamLastObjectId.get(streamKey);
            boolean isFirstObject = (lastObjectId == null);

            ByteBuf message = Unpooled.buffer();

            if (isFirstObject) {
                // Send SUBGROUP_HEADER at start of stream
                // Stream Type: 0x10 = SUBGROUP_HEADER (no extensions, no end-of-group)
                VarInt.write(message, MoqMessageType.SUBGROUP_HEADER.getValue());

                // Track Alias
                VarInt.write(message, trackAlias);

                // Group ID
                VarInt.write(message, groupId);

                // Subgroup ID (0 for single subgroup)
                VarInt.write(message, 0);

                // Publisher Priority (8 bits, lower = higher priority)
                message.writeByte((int) publisherPriority);

                logger.debug("Sent SUBGROUP_HEADER: alias={}, group={}, subgroup=0, priority={}",
                        trackAlias, groupId, publisherPriority);
            }

            // Now send the object
            // Object ID Delta (difference from last object ID, or absolute if first)
            long objectIdDelta = isFirstObject ? objectId : (objectId - lastObjectId);
            VarInt.write(message, objectIdDelta);

            // Object Payload Length
            int payloadLength = (payload != null) ? payload.readableBytes() : 0;
            VarInt.write(message, payloadLength);

            // Object Status (only if payload length is 0)
            if (payloadLength == 0) {
                VarInt.write(message, objectStatus);
            }

            // Write header
            ctx.write(message);

            // Write payload if present
            if (payload != null && payloadLength > 0) {
                ctx.write(payload.retainedDuplicate());
            }

            // Flush
            ctx.flush();

            // Update last object ID for this stream
            streamLastObjectId.put(streamKey, objectId);

            logger.debug("Sent object on stream: group={}, object={}, delta={}, size={}",
                    groupId, objectId, objectIdDelta, payloadLength);
        } catch (Exception e) {
            logger.error("Error sending object on stream", e);
        }
    }

    /**
     * Closes any streams for this subscription that belong to old groups.
     */
    private void closeOldGroupStreams(long subscribeId, long currentGroupId) {
        // Find and close streams for this subscription with different group IDs
        activeGroupStreams.entrySet().removeIf(entry -> {
            String key = entry.getKey();
            String[] parts = key.split(":");
            if (parts.length == 2) {
                long subId = Long.parseLong(parts[0]);
                long groupId = Long.parseLong(parts[1]);

                if (subId == subscribeId && groupId != currentGroupId) {
                    QuicStreamChannel stream = entry.getValue();
                    if (stream != null && stream.isActive()) {
                        logger.debug("Closing old stream for subscribeId={}, groupId={}", subId, groupId);
                        stream.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                    }
                    // Clean up tracking map
                    streamLastObjectId.remove(key);
                    return true; // Remove from map
                }
            }
            return false;
        });
    }

    /**
     * Closes all active streams for a subscription (called on UNSUBSCRIBE).
     */
    private void closeAllStreamsForSubscription(long subscribeId) {
        activeGroupStreams.entrySet().removeIf(entry -> {
            String key = entry.getKey();
            String[] parts = key.split(":");
            if (parts.length == 2) {
                long subId = Long.parseLong(parts[0]);

                if (subId == subscribeId) {
                    QuicStreamChannel stream = entry.getValue();
                    if (stream != null && stream.isActive()) {
                        logger.info("Closing stream for unsubscribed subscribeId={}", subId);
                        stream.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                    }
                    // Clean up tracking map
                    streamLastObjectId.remove(key);
                    return true; // Remove from map
                }
            }
            return false;
        });
    }

    private void startPublisher() throws InterruptedException {
        logger.info("Starting publisher");

        // Send PUBLISH_NAMESPACE (formerly ANNOUNCE in earlier drafts)
        sendPublishNamespace();

        // Wait for PUBLISH_NAMESPACE_OK (parsed by ControlStreamHandler)
        logger.info("Waiting for PUBLISH_NAMESPACE_OK...");
        Thread.sleep(200);

        // If catalog was already received before PUBLISH_NAMESPACE was sent,
        // send PUBLISH messages now for all tracks
        if (catalog.getTracks() != null && !catalog.getTracks().isEmpty()) {
            logger.info("Sending delayed PUBLISH messages for {} tracks (catalog received early)", catalog.getTracks().size());

            // Count media tracks (exclude catalog and timeline metadata tracks)
            long mediaTrackCount = catalog.getTracks().stream()
                .filter(track -> !track.getName().equals("catalog") && !track.getName().equals("timeline"))
                .count();

            // Initialize latch to wait for PUBLISH_OK responses for media tracks only
            // This prevents frames from being processed before subscriptions exist
            publisherReadyLatch = new CountDownLatch((int) mediaTrackCount);
            logger.info("Initialized publisher ready latch with count {} (waiting for PUBLISH_OK for media tracks)", mediaTrackCount);

            catalog.getTracks().forEach(track -> {
                Long alias = trackAliases.get(track.getName());
                if (alias != null) {
                    sendPublish(track.getName(), alias);
                }
            });
        } else {
            // No catalog yet - will initialize latch when catalog arrives
            publisherReadyLatch = new CountDownLatch(0);
            logger.warn("No catalog available yet - publisher ready latch initialized to 0");
        }

        logger.info("Publisher ready - will transmit objects to subscribers as they arrive");
    }

    private void startEventLoop() {
        Thread eventThread = new Thread(() -> {
            logger.info("Event loop started");

            while (continuePublishing.get()) {
                try {
                    List<TrackEvent> events = eventQueue.poll(50, TimeUnit.MILLISECONDS);
                    if (events == null) {
                        continue;
                    }

                    for (TrackEvent event : events) {
                        processEvent(event);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("Event loop interrupted");
                    break;
                } catch (Exception e) {
                    logger.error("Error in event loop", e);
                }
            }

            logger.info("Event loop stopped");
        });

        eventThread.setDaemon(true);
        eventThread.start();
    }

    private void processEvent(TrackEvent event) {
        switch (event) {
            case TrackEvent.Keyframe keyframe -> {
                FrameData frameData = keyframe.data();

                // Wait for subscriptions to be ready before processing media frames
                // This prevents frame loss during the PUBLISH → PUBLISH_OK handshake
                if (publisherReadyLatch != null && publisherReadyLatch.getCount() > 0) {
                    logger.debug("Buffering keyframe {} (waiting for subscriptions): track={}, group={}, object={}",
                            frameData, frameData.getTrackName(), frameData.getGroupId(), frameData.getObjectId());
                    try {
                        // Wait up to 5 seconds for subscriptions
                        boolean ready = publisherReadyLatch.await(5, TimeUnit.SECONDS);
                        if (!ready) {
                            logger.warn("Timeout waiting for subscriptions - processing frame anyway");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warn("Interrupted while waiting for subscriptions");
                    }
                }

                logger.info("Processing keyframe: {}", frameData);
                // Send to subscribers using draft-14 SUBGROUP_HEADER
                sendFrameToSubscribers(frameData, true);
            }
            case TrackEvent.Frame frame -> {
                FrameData frameData = frame.data();

                // Wait for subscriptions to be ready before processing media frames
                if (publisherReadyLatch != null && publisherReadyLatch.getCount() > 0) {
                    logger.trace("Buffering frame {} (waiting for subscriptions): track={}, group={}, object={}",
                            frameData, frameData.getTrackName(), frameData.getGroupId(), frameData.getObjectId());
                    try {
                        boolean ready = publisherReadyLatch.await(5, TimeUnit.SECONDS);
                        if (!ready) {
                            logger.warn("Timeout waiting for subscriptions - processing frame anyway");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warn("Interrupted while waiting for subscriptions");
                    }
                }

                logger.debug("Processing frame: {}", frameData);
                // Send to subscribers using draft-14 SUBGROUP_HEADER
                sendFrameToSubscribers(frameData, false);
            }
            case TrackEvent.InitSegment initSegment -> {
                FrameData frameData = initSegment.data();
                logger.info("Processing init segment: {}", frameData);
                sendFrameToSubscribers(frameData, false);
            }
            case TrackEvent.CatalogEvent catalogEvent -> {
                Catalog cat = catalogEvent.catalog();
                logger.info("Processing catalog with {} tracks", cat.getTracks().size());

                // Store track aliases from catalog for later use
                cat.getTracks().forEach(track -> {
                    try {
                        long alias = Long.parseLong(track.getName());
                        trackAliases.put(track.getName(), alias);
                        logger.info("Registered track alias: {} -> {}", track.getName(), alias);
                    } catch (NumberFormatException e) {
                        // For non-numeric track names (catalog, timeline), use hash code as alias
                        long alias = track.getName().hashCode() & 0x7FFFFFFF; // Positive int
                        trackAliases.put(track.getName(), alias);
                        logger.info("Registered track alias: {} -> {} (hash)", track.getName(), alias);
                    }
                });

                // Send PUBLISH messages for each track to announce them to namespace subscribers
                // Per draft-14 Section 9.28: publisher forwards existing PUBLISH messages to
                // matching SUBSCRIBE_NAMESPACE subscribers
                // IMPORTANT: Only send PUBLISH after PUBLISH_NAMESPACE has been sent (per draft-14 Section 9.1)
                if (namespacePublished.get()) {
                    cat.getTracks().forEach(track -> {
                        Long alias = trackAliases.get(track.getName());
                        if (alias != null) {
                            sendPublish(track.getName(), alias);
                        }
                    });
                } else {
                    logger.debug("Skipping PUBLISH messages - namespace not yet published (avoiding request ID race)");
                }

                // Send catalog to subscribers using draft-14 SUBGROUP_HEADER
                sendCatalogToSubscribers(cat);
            }
            case TrackEvent.TimeEvent timeEvent -> {
                logger.info("Processing time event: {}", timeEvent.data());
                // TimeEvent contains individual event data - could be used for live timeline updates
                // For now, we rely on TimelineEvent for full timeline updates
            }
            case TrackEvent.TimelineEvent timelineEvent -> {
                logger.info("Processing timeline event with {} records", timelineEvent.timeline().getRecords().size());
                sendTimelineToSubscribers(timelineEvent.timeline());
            }
            default -> logger.debug("Unhandled event type: {}", event);
        }
    }

    /**
     * Sends a frame to all matching subscriptions using draft-14 SUBGROUP_HEADER.
     *
     * CRITICAL: This method ONLY sends to active subscribers who have sent SUBSCRIBE messages.
     * Uses SUBGROUP_HEADER (0x10-0x1D) per draft-14 Section 10.4.2.
     * Does NOT use legacy STREAM_HEADER_TRACK (0x50) or STREAM_HEADER_GROUP (0x51).
     */
    private void sendFrameToSubscribers(FrameData frameData, boolean isKeyframe) {
        if (subscriptions.isEmpty() && namespaceSubscriptions.isEmpty()) {
            logger.debug("No active subscriptions, skipping frame transmission for track {}", frameData.getTrackName());
            return;
        }

        String trackName = frameData.getTrackName();
        long groupId = frameData.getGroupId();
        long objectId = frameData.getObjectId();
        ByteBuf payload = frameData.getPayload();

        logger.debug("sendFrameToSubscribers: track={}, group={}, object={}, subscriptions={}, namespaceSubscriptions={}",
                trackName, groupId, objectId, subscriptions.size(), namespaceSubscriptions.size());

        int matchCount = 0;

        // Check individual track subscriptions
        for (Subscription subscription : subscriptions.values()) {
            logger.debug("Checking subscription: subId={}, namespace={}, track={}, alias={}",
                    subscription.getSubscribeId(), subscription.getTrackNamespace(),
                    subscription.getTrackName(), subscription.getTrackAlias());

            // Match by namespace and track name
            if (!subscription.getTrackNamespace().equals(namespace)) {
                logger.debug("Subscription {} namespace mismatch: expected={}, got={}",
                        subscription.getSubscribeId(), namespace, subscription.getTrackNamespace());
                continue;
            }

            if (!subscription.getTrackName().equals(trackName)) {
                logger.debug("Subscription {} trackName mismatch: expected={}, got={}",
                        subscription.getSubscribeId(), trackName, subscription.getTrackName());
                continue;
            }

            // Check if this subscription should receive this object based on its range
            if (!subscription.shouldSend(groupId, objectId)) {
                logger.debug("Subscription {} skipping object {}/{} (out of range)",
                        subscription.getSubscribeId(), groupId, objectId);
                continue;
            }

            matchCount++;

            // Send the object
            // Object Status: 0x00 = Normal
            // Publisher Priority: lower = higher priority, use 0 for keyframes, 1 for regular frames
            long priority = isKeyframe ? 0 : 1;
            sendObjectStream(subscription.getSubscribeId(), subscription.getTrackAlias(),
                    groupId, objectId, priority, 0x00, payload);

            // Update subscription's last sent position
            subscription.updateLastSent(groupId, objectId);

            logger.info("Sent {} to subscription {}: track={}, group={}, object={}, size={}",
                    isKeyframe ? "keyframe" : "frame",
                    subscription.getSubscribeId(), trackName, groupId, objectId, payload.readableBytes());
        }

        if (matchCount == 0) {
            logger.debug("No matching subscriptions found for track={}, group={}, object={}", trackName, groupId, objectId);
        } else {
            logger.info("Sent to {} matching subscriptions for track={}", matchCount, trackName);
        }

        // Check namespace subscriptions
        for (Map.Entry<Long, List<Subscription>> entry : namespaceSubscriptions.entrySet()) {
            for (Subscription subscription : entry.getValue()) {
                // Match by namespace and track name
                if (!subscription.getTrackNamespace().equals(namespace)) {
                    continue;
                }

                if (!subscription.getTrackName().equals(trackName)) {
                    continue;
                }

                // Check if this subscription should receive this object based on its range
                if (!subscription.shouldSend(groupId, objectId)) {
                    logger.trace("Namespace subscription {} skipping object {}/{} (out of range)",
                            subscription.getSubscribeId(), groupId, objectId);
                    continue;
                }

                // Send the object
                long priority = isKeyframe ? 0 : 1;
                sendObjectStream(subscription.getSubscribeId(), subscription.getTrackAlias(),
                        groupId, objectId, priority, 0x00, payload);

                // Update subscription's last sent position
                subscription.updateLastSent(groupId, objectId);

                logger.debug("Sent {} to namespace subscription {}: track={}, group={}, object={}, size={}",
                        isKeyframe ? "keyframe" : "frame",
                        subscription.getSubscribeId(), trackName, groupId, objectId, payload.readableBytes());
            }
        }
    }

    /**
     * Sends catalog to all subscriptions for the "catalog" track.
     * Catalog uses group ID as version number, with object ID always 0.
     */
    private void sendCatalogToSubscribers(Catalog newCatalog) {
        if (subscriptions.isEmpty()) {
            logger.trace("No active subscriptions, skipping catalog transmission");
            return;
        }

        // Update internal catalog by copying tracks
        catalog.setTracks(newCatalog.getTracks());
        catalog.setGeneratedAt(newCatalog.getGeneratedAt());

        // Increment catalog version (group ID)
        long groupId = catalogVersion.incrementAndGet();
        long objectId = 0; // Catalog is always object 0 in the group

        logger.info("Sending catalog version {} with {} tracks", groupId, catalog.getTracks().size());

        // Get catalog payload as JSON
        ByteBuf payload = catalog.getPayload();

        // Send to all subscriptions for "catalog" track
        for (Subscription subscription : subscriptions.values()) {
            if (!subscription.getTrackNamespace().equals(namespace)) {
                continue;
            }

            if (!subscription.getTrackName().equals("catalog")) {
                continue;
            }

            // Catalog should be sent regardless of range filters (always latest)
            sendObjectStream(subscription.getSubscribeId(), subscription.getTrackAlias(),
                    groupId, objectId, 0, 0x00, payload);

            subscription.updateLastSent(groupId, objectId);

            logger.info("Sent catalog version {} to subscription {}, size={} bytes",
                    groupId, subscription.getSubscribeId(), payload.readableBytes());
        }

        payload.release();
    }

    /**
     * Sends timeline to all subscriptions for the "timeline" track.
     * Timeline uses group ID as version number, with object ID always 0.
     */
    private void sendTimelineToSubscribers(Timeline newTimeline) {
        if (subscriptions.isEmpty()) {
            logger.trace("No active subscriptions, skipping timeline transmission");
            return;
        }

        // Update internal timeline by copying records
        timeline.setRecords(newTimeline.getRecords());
        timeline.setVersion(newTimeline.getVersion());
        timeline.setGeneratedAt(newTimeline.getGeneratedAt());

        // Increment timeline version (group ID)
        long groupId = timelineVersion.incrementAndGet();
        long objectId = 0; // Timeline is always object 0 in the group

        logger.info("Sending timeline version {} with {} records", groupId, timeline.getRecords().size());

        // Get timeline payload as JSON
        ByteBuf payload = timeline.getPayload();

        // Send to all subscriptions for "timeline" track
        for (Subscription subscription : subscriptions.values()) {
            if (!subscription.getTrackNamespace().equals(namespace)) {
                continue;
            }

            if (!subscription.getTrackName().equals("timeline")) {
                continue;
            }

            // Timeline should be sent regardless of range filters (always latest)
            sendObjectStream(subscription.getSubscribeId(), subscription.getTrackAlias(),
                    groupId, objectId, 0, 0x00, payload);

            subscription.updateLastSent(groupId, objectId);

            logger.info("Sent timeline version {} to subscription {}, size={} bytes",
                    groupId, subscription.getSubscribeId(), payload.readableBytes());
        }

        payload.release();
    }

    /**
     * Sends the current catalog to a newly subscribed client.
     */
    private void sendCurrentCatalogToSubscription(Subscription subscription) {
        if (!subscription.getTrackName().equals("catalog")) {
            return;
        }

        long groupId = catalogVersion.get();
        if (groupId == 0) {
            logger.debug("No catalog available yet for subscription {}", subscription.getSubscribeId());
            return;
        }

        long objectId = 0;
        ByteBuf payload = catalog.getPayload();

        logger.info("Sending current catalog (version {}) to new subscription {}, size={} bytes",
                groupId, subscription.getSubscribeId(), payload.readableBytes());

        sendObjectStream(subscription.getSubscribeId(), subscription.getTrackAlias(),
                groupId, objectId, 0, 0x00, payload);

        subscription.updateLastSent(groupId, objectId);
        payload.release();
    }

    /**
     * Sends the current timeline to a newly subscribed client.
     */
    private void sendCurrentTimelineToSubscription(Subscription subscription) {
        if (!subscription.getTrackName().equals("timeline")) {
            return;
        }

        long groupId = timelineVersion.get();
        if (groupId == 0) {
            logger.debug("No timeline available yet for subscription {}", subscription.getSubscribeId());
            return;
        }

        long objectId = 0;
        ByteBuf payload = timeline.getPayload();

        logger.info("Sending current timeline (version {}) to new subscription {}, size={} bytes",
                groupId, subscription.getSubscribeId(), payload.readableBytes());

        sendObjectStream(subscription.getSubscribeId(), subscription.getTrackAlias(),
                groupId, objectId, 0, 0x00, payload);

        subscription.updateLastSent(groupId, objectId);
        payload.release();
    }

    public void stop() {
        continuePublishing.set(false);
        if (quicChannel != null) {
            quicChannel.close();
        }
    }

    /**
     * Handler for the control stream.
     * Processes incoming SERVER_SETUP, PUBLISH_NAMESPACE_OK, GOAWAY, etc.
     */
    private class ControlStreamHandler extends ChannelInboundHandlerAdapter {
        private ByteBuf accumulatedBuffer = Unpooled.buffer();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf buf = (ByteBuf) msg;
            try {
                logger.debug("Received {} bytes on control stream", buf.readableBytes());

                // Accumulate data
                accumulatedBuffer.writeBytes(buf);

                // Try to parse messages
                parseMessages();

            } finally {
                buf.release();
            }
        }

        private void parseMessages() {
            while (accumulatedBuffer.readableBytes() >= 3) {  // min: 1 byte type + 2 bytes length
                accumulatedBuffer.markReaderIndex();

                try {
                    // Read message type (varint)
                    long messageType = VarInt.read(accumulatedBuffer);

                    // Read message length (u16)
                    if (accumulatedBuffer.readableBytes() < 2) {
                        accumulatedBuffer.resetReaderIndex();
                        break;
                    }
                    int messageLength = accumulatedBuffer.readUnsignedShort();

                    // Check if we have the full payload
                    if (accumulatedBuffer.readableBytes() < messageLength) {
                        accumulatedBuffer.resetReaderIndex();
                        break;
                    }

                    // Process the message
                    handleMessage((int)messageType, messageLength);

                } catch (Exception e) {
                    logger.error("Error parsing control stream message", e);
                    accumulatedBuffer.resetReaderIndex();
                    break;
                }
            }
        }

        private void handleMessage(int messageType, int payloadLength) {
            MoqMessageType type = MoqMessageType.fromValue(messageType);
            if (type == null) {
                logger.warn("Received unsupported message type: 0x{}", Integer.toHexString(messageType));
                accumulatedBuffer.skipBytes(payloadLength);  // Skip unknown message
                return;
            }

            switch (type) {
                case SERVER_SETUP -> handleServerSetup(payloadLength);
                case PUBLISH_NAMESPACE_OK -> handlePublishNamespaceOk(payloadLength);
                case PUBLISH_OK -> handlePublishOk(payloadLength);
                case SUBSCRIBE -> handleSubscribe(payloadLength);
                case SUBSCRIBE_NAMESPACE -> handleSubscribeNamespace(payloadLength);
                case UNSUBSCRIBE -> handleUnsubscribe(payloadLength);
                case UNSUBSCRIBE_NAMESPACE -> handleUnsubscribeNamespace(payloadLength);
                case GOAWAY -> handleGoAway(payloadLength);
                default -> {
                    logger.warn("Received unsupported message type: {}", type);
                    accumulatedBuffer.skipBytes(payloadLength);  // Skip unknown message
                }
            }
        }

        private void handleServerSetup(int payloadLength) {
            logger.info("Received SERVER_SETUP from server");

            // Read selected version
            long selectedVersion = VarInt.read(accumulatedBuffer);
            logger.info("Server selected version: 0x{}", String.format("%08X", selectedVersion));

            // Read number of parameters
            long numParams = VarInt.read(accumulatedBuffer);
            logger.debug("Server sent {} parameters", numParams);

            // Parse parameters per draft-14 Section 1.4.2
            // ODD parameter types (0x01, 0x03, ...): Type + Length (varint) + Value (bytes)
            // EVEN parameter types (0x00, 0x02, ...): Type + Value (varint) - NO length field
            for (int i = 0; i < numParams; i++) {
                long paramKey = VarInt.read(accumulatedBuffer);

                if (paramKey % 2 == 1) {
                    // Odd parameter - has length prefix, value is bytes
                    long paramLength = VarInt.read(accumulatedBuffer);
                    byte[] paramValue = new byte[(int) paramLength];
                    accumulatedBuffer.readBytes(paramValue);
                    logger.debug("Server param: key={} (odd), length={}, value={}",
                                paramKey, paramLength, java.util.Arrays.toString(paramValue));
                } else {
                    // Even parameter - no length prefix, value is varint
                    long paramValue = VarInt.read(accumulatedBuffer);
                    logger.debug("Server param: key={} (even), value={}", paramKey, paramValue);
                }
            }

            logger.info("SERVER_SETUP processed successfully");
        }

        private void handlePublishNamespaceOk(int payloadLength) {
            logger.info("Received PUBLISH_NAMESPACE_OK from server");

            // According to draft-14 Section 9.24, PUBLISH_NAMESPACE_OK contains:
            // - Request ID (varint) - The request ID from the original PUBLISH_NAMESPACE
            // It does NOT contain namespace tuple or parameters
            if (payloadLength > 0) {
                try {
                    long requestId = VarInt.read(accumulatedBuffer);
                    logger.info("PUBLISH_NAMESPACE_OK: Request ID = {}", requestId);
                    logger.info("Namespace '{}' published successfully", namespace);
                } catch (Exception e) {
                    logger.error("Error parsing PUBLISH_NAMESPACE_OK: {}", e.getMessage());
                    // Skip remaining payload
                    accumulatedBuffer.skipBytes(Math.min(payloadLength, accumulatedBuffer.readableBytes()));
                }
            } else {
                logger.warn("PUBLISH_NAMESPACE_OK has empty payload (expected request_id)");
                logger.info("Namespace '{}' published successfully (assuming OK despite empty payload)", namespace);
            }
        }

        private void handlePublishOk(int payloadLength) {
            logger.info("Received PUBLISH_OK from relay");

            try {
                // Parse PUBLISH_OK message (draft-14 Section 9.14)
                // Format: Request ID + Forward (u8) + Subscriber Priority (u8) + Group Order (u8) +
                //         Filter Type + [Start Location] + [End Group] + Parameters

                // Request ID (varint)
                long requestId = VarInt.read(accumulatedBuffer);

                // Forward (u8)
                int forward = accumulatedBuffer.readUnsignedByte();

                // Subscriber Priority (u8)
                int subscriberPriority = accumulatedBuffer.readUnsignedByte();

                // Group Order (u8)
                int groupOrder = accumulatedBuffer.readUnsignedByte();

                // Filter Type (varint)
                long filterType = VarInt.read(accumulatedBuffer);

                // Parse filter (same as SUBSCRIBE)
                long startGroup = 0, startObject = 0, endGroup = Long.MAX_VALUE;
                if (filterType == 3) {  // AbsoluteStart
                    startGroup = VarInt.read(accumulatedBuffer);
                    startObject = VarInt.read(accumulatedBuffer);
                } else if (filterType == 4) {  // AbsoluteRange
                    startGroup = VarInt.read(accumulatedBuffer);
                    startObject = VarInt.read(accumulatedBuffer);
                    endGroup = VarInt.read(accumulatedBuffer);
                }

                // Number of parameters (varint)
                long numParams = VarInt.read(accumulatedBuffer);
                // Skip parameters (per draft-14 Section 1.4.2: odd=length+bytes, even=varint)
                for (int i = 0; i < numParams; i++) {
                    long paramKey = VarInt.read(accumulatedBuffer);
                    if (paramKey % 2 == 1) {
                        // Odd parameter - has length prefix
                        long paramLen = VarInt.read(accumulatedBuffer);
                        accumulatedBuffer.skipBytes((int) paramLen);
                    } else {
                        // Even parameter - no length, just varint value
                        VarInt.read(accumulatedBuffer);
                    }
                }

                logger.info("PUBLISH_OK: requestId={}, forward={}, priority={}, order={}, filter={}",
                        requestId, forward, subscriberPriority, groupOrder, filterType);

                // Look up which track this PUBLISH_OK corresponds to
                TrackInfo trackInfo = pendingPublishes.remove(requestId);
                if (trackInfo == null) {
                    logger.warn("Received PUBLISH_OK for unknown requestId: {}", requestId);
                    return;
                }

                logger.info("PUBLISH_OK for track: {}, alias: {}", trackInfo.trackName, trackInfo.trackAlias);

                // Per draft-14 Section 9.14: PUBLISH_OK establishes a push-based subscription
                // If forward=1, we should start sending objects immediately
                if (forward == 1) {
                    // Create subscription for this track
                    // Use requestId as subscription ID for push-based subscriptions
                    Subscription subscription = new Subscription(
                        requestId,
                        namespace,
                        trackInfo.trackName,
                        trackInfo.trackAlias,
                        startGroup, startObject,
                        endGroup, Long.MAX_VALUE
                    );

                    subscriptions.put(requestId, subscription);
                    logger.info("Created push-based subscription: {}", subscription);
                    logger.info("Total active subscriptions: {}", subscriptions.size());

                    // For catalog and timeline tracks, immediately send the current version
                    if (trackInfo.trackName.equals("catalog")) {
                        sendCurrentCatalogToSubscription(subscription);
                    } else if (trackInfo.trackName.equals("timeline")) {
                        sendCurrentTimelineToSubscription(subscription);
                    } else {
                        // Count down latch for media tracks only (not catalog/timeline)
                        // This signals that the publisher is ready to accept frames
                        if (publisherReadyLatch != null) {
                            publisherReadyLatch.countDown();
                            logger.info("Publisher ready latch countdown: {} remaining (media track subscription created)",
                                    publisherReadyLatch.getCount());
                        }
                    }
                } else {
                    logger.info("PUBLISH_OK with forward=0, not sending objects yet for track: {}", trackInfo.trackName);
                }

            } catch (Exception e) {
                logger.error("Error parsing PUBLISH_OK", e);
            }
        }

        private void handleGoAway(int payloadLength) {
            logger.warn("Received GOAWAY from server - session is shutting down");

            // Check for multiple GOAWAY messages (MUST terminate with PROTOCOL_VIOLATION per draft-14)
            if (goAwayReceived.getAndSet(true)) {
                logger.error("Received multiple GOAWAY messages - PROTOCOL_VIOLATION!");
                logger.error("Terminating session immediately");
                continuePublishing.set(false);
                if (controlStream != null) {
                    controlStream.close();
                }
                if (quicChannel != null) {
                    quicChannel.close();
                }
                return;
            }

            try {
                // Parse GOAWAY message per draft-14 Section 9.5
                // Format: New Session URI Length (i) + New Session URI (..)
                long uriLength = VarInt.read(accumulatedBuffer);

                // Validate URI length (max 8,192 bytes per spec)
                if (uriLength > 8192) {
                    logger.error("GOAWAY New Session URI length ({}) exceeds maximum (8192 bytes)", uriLength);
                    continuePublishing.set(false);
                    return;
                }

                // Read new session URI
                String newSessionUri = "";
                if (uriLength > 0) {
                    byte[] uriBytes = new byte[(int) uriLength];
                    accumulatedBuffer.readBytes(uriBytes);
                    newSessionUri = new String(uriBytes, java.nio.charset.StandardCharsets.UTF_8);
                }

                if (!newSessionUri.isEmpty()) {
                    logger.warn("═══════════════════════════════════════════════════════════");
                    logger.warn("Server sent GOAWAY - suggesting reconnection to new URI:");
                    logger.warn("  New URI: {}", newSessionUri);
                    logger.warn("  Current URI: {}", endpoint);
                    logger.warn("═══════════════════════════════════════════════════════════");
                    logger.info("GOAWAY: Client SHOULD reconnect to: {}", newSessionUri);
                } else {
                    logger.warn("═══════════════════════════════════════════════════════════");
                    logger.warn("Server sent GOAWAY - shutting down gracefully");
                    logger.warn("  No alternate URI provided - server is terminating");
                    logger.warn("═══════════════════════════════════════════════════════════");
                }

                // Graceful shutdown process
                logger.info("Initiating graceful shutdown due to GOAWAY");

                // 1. Stop accepting new events
                continuePublishing.set(false);

                // 2. Close all subscription streams
                logger.info("Closing {} active group streams", activeGroupStreams.size());
                for (QuicStreamChannel stream : activeGroupStreams.values()) {
                    try {
                        if (stream != null && stream.isActive()) {
                            stream.close();
                        }
                    } catch (Exception e) {
                        logger.error("Error closing subscription stream", e);
                    }
                }
                activeGroupStreams.clear();

                // 3. Clear subscription state
                logger.info("Clearing {} individual subscriptions and {} namespace subscriptions",
                        subscriptions.size(), namespaceSubscriptions.size());
                subscriptions.clear();
                namespaceSubscriptions.clear();

                logger.warn("Graceful shutdown complete - session terminated by GOAWAY");

            } catch (Exception e) {
                logger.error("Error handling GOAWAY message", e);
                continuePublishing.set(false);
            }
        }

        private void handleSubscribe(int payloadLength) {
            logger.info("Received SUBSCRIBE from subscriber (payload length: {} bytes, buffer readable: {} bytes)",
                       payloadLength, accumulatedBuffer.readableBytes());

            try {
                // Parse SUBSCRIBE message (draft-14 Section 9.7)
                // Format: Request ID + Track Namespace (tuple) + Track Name +
                //         Subscriber Priority (u8) + Group Order (u8) + Filter Type (u8) +
                //         [Start Location] + [End Group] + Parameters

                // Request ID (varint)
                logger.debug("Reading Request ID (buffer has {} bytes)", accumulatedBuffer.readableBytes());
                long requestId = VarInt.read(accumulatedBuffer);
                logger.debug("Request ID: {}", requestId);

                // Track namespace tuple
                logger.debug("Reading Track Namespace (buffer has {} bytes)", accumulatedBuffer.readableBytes());
                long tupleLength = VarInt.read(accumulatedBuffer);
                StringBuilder trackNamespace = new StringBuilder();
                for (int i = 0; i < tupleLength; i++) {
                    if (i > 0) trackNamespace.append("/");
                    long strLen = VarInt.read(accumulatedBuffer);
                    byte[] strBytes = new byte[(int) strLen];
                    accumulatedBuffer.readBytes(strBytes);
                    trackNamespace.append(new String(strBytes, java.nio.charset.StandardCharsets.UTF_8));
                }
                logger.debug("Track Namespace: {}", trackNamespace);

                // Track name length (varint) + Track name (bytes)
                logger.debug("Reading Track Name (buffer has {} bytes)", accumulatedBuffer.readableBytes());
                long trackNameLen = VarInt.read(accumulatedBuffer);
                byte[] trackNameBytes = new byte[(int) trackNameLen];
                accumulatedBuffer.readBytes(trackNameBytes);
                String trackName = new String(trackNameBytes, java.nio.charset.StandardCharsets.UTF_8);
                logger.debug("Track Name: {}", trackName);

                // Subscriber priority (u8 - 1 byte)
                logger.debug("Reading Subscriber Priority (buffer has {} bytes)", accumulatedBuffer.readableBytes());
                int subscriberPriority = accumulatedBuffer.readUnsignedByte();
                logger.debug("Subscriber Priority: {}", subscriberPriority);

                // Group order (u8 - 1 byte)
                logger.debug("Reading Group Order (buffer has {} bytes)", accumulatedBuffer.readableBytes());
                int groupOrder = accumulatedBuffer.readUnsignedByte();
                logger.debug("Group Order: {}", groupOrder);

                // Filter type (u8 - 1 byte)
                // NOTE: draft-14 removed Forward field from SUBSCRIBE (it's only in PUBLISH_OK now)
                logger.debug("Reading Filter Type (buffer has {} bytes)", accumulatedBuffer.readableBytes());
                long filterType = accumulatedBuffer.readUnsignedByte();
                logger.debug("Filter Type: {}", filterType);

                // Parse filter per draft-14 Section 9.7
                // Filter types: 1=NextGroup, 2=LatestObject, 3=AbsoluteStart, 4=AbsoluteRange
                long startGroup = 0, startObject = 0, endGroup = Long.MAX_VALUE, endObject = Long.MAX_VALUE;

                if (filterType == 1) {  // NextGroup (0x1)
                    // No additional fields - start from next group after largest
                    startGroup = Long.MAX_VALUE - 1;
                } else if (filterType == 2) {  // LatestObject (0x2)
                    // No additional fields - start from latest object + 1
                    startGroup = Long.MAX_VALUE - 1;
                    startObject = Long.MAX_VALUE - 1;
                } else if (filterType == 3) {  // AbsoluteStart (0x3)
                    // Start Location (Group + Object)
                    startGroup = VarInt.read(accumulatedBuffer);
                    startObject = VarInt.read(accumulatedBuffer);
                } else if (filterType == 4) {  // AbsoluteRange (0x4)
                    // Start Location (Group + Object) + End Group (just group, no object!)
                    startGroup = VarInt.read(accumulatedBuffer);
                    startObject = VarInt.read(accumulatedBuffer);
                    endGroup = VarInt.read(accumulatedBuffer);
                    // NOTE: endObject stays as Long.MAX_VALUE (no End Object field in draft-14!)
                }

                // Number of parameters (varint)
                logger.debug("Reading Number of Parameters (buffer has {} bytes)", accumulatedBuffer.readableBytes());
                long numParams = VarInt.read(accumulatedBuffer);
                logger.debug("Number of Parameters: {}", numParams);
                // Skip parameters (per draft-14 Section 1.4.2: odd=length+bytes, even=varint)
                for (int i = 0; i < numParams; i++) {
                    long paramKey = VarInt.read(accumulatedBuffer);
                    if (paramKey % 2 == 1) {
                        // Odd parameter - has length prefix
                        long paramLen = VarInt.read(accumulatedBuffer);
                        accumulatedBuffer.skipBytes((int) paramLen);
                    } else {
                        // Even parameter - no length, just varint value
                        VarInt.read(accumulatedBuffer);
                    }
                }

                // Look up track alias from our registered aliases
                Long trackAlias = trackAliases.get(trackName);
                if (trackAlias == null) {
                    logger.error("SUBSCRIBE for unknown track: {}. No alias registered.", trackName);
                    // Send SUBSCRIBE_ERROR
                    // TODO: Implement sendSubscribeError()
                    return;
                }

                logger.info("SUBSCRIBE: requestId={}, namespace={}, track={}, alias={}, priority={}, order={}, filter={}, range=[{}:{}]-[{}:{}]",
                           requestId, trackNamespace, trackName, trackAlias, subscriberPriority, groupOrder, filterType,
                           startGroup, startObject, endGroup, endObject);

                // Check if we already have a push-based subscription for this track
                // Per draft-14: PUBLISH (push) and SUBSCRIBE (pull) should not be mixed for the same track
                boolean hasExistingPushSubscription = subscriptions.values().stream()
                    .anyMatch(sub -> sub.getTrackName().equals(trackName) && sub.getTrackAlias() == trackAlias);

                if (hasExistingPushSubscription) {
                    logger.warn("Received SUBSCRIBE for track '{}' that already has a push-based subscription (from PUBLISH_OK). " +
                               "Per draft-14, PUBLISH and SUBSCRIBE should not be mixed. This may indicate a relay protocol issue.",
                               trackName);
                }

                // Create subscription (using requestId as subscribeId per draft-14)
                Subscription subscription = new Subscription(
                    requestId, trackNamespace.toString(), trackName, trackAlias,
                    startGroup, startObject, endGroup, endObject
                );

                subscriptions.put(requestId, subscription);
                logger.info("Added subscription: {}", subscription);

                // Send SUBSCRIBE_OK
                sendSubscribeOk(requestId, trackAlias, 0, true);  // expires=0 (no expiration), content_exists=true

                // For catalog and timeline tracks, immediately send the current version
                // This ensures new subscribers get the latest metadata right away
                if (trackName.equals("catalog")) {
                    sendCurrentCatalogToSubscription(subscription);
                } else if (trackName.equals("timeline")) {
                    sendCurrentTimelineToSubscription(subscription);
                }

            } catch (Exception e) {
                logger.error("Error parsing SUBSCRIBE message", e);
            }
        }

        private void handleUnsubscribe(int payloadLength) {
            logger.info("Received UNSUBSCRIBE from subscriber");

            long subscribeId = VarInt.read(accumulatedBuffer);
            Subscription removed = subscriptions.remove(subscribeId);

            if (removed != null) {
                logger.info("Removed subscription: {}", removed);

                // Close any active streams for this subscription
                closeAllStreamsForSubscription(subscribeId);
            } else {
                logger.warn("UNSUBSCRIBE for unknown subscription ID: {}", subscribeId);
            }
        }

        private void handleSubscribeNamespace(int payloadLength) {
            logger.info("Received SUBSCRIBE_NAMESPACE from subscriber");

            try {
                // Parse SUBSCRIBE_NAMESPACE message (draft-14 Section 9.28)
                // Format: Request ID (i) + Track Namespace Prefix (tuple) + num_params + params[]
                int readerIndexBefore = accumulatedBuffer.readerIndex();
                long requestId = VarInt.read(accumulatedBuffer);
                int bytesRead = accumulatedBuffer.readerIndex() - readerIndexBefore;
                logger.debug("Read Request ID: {} (consumed {} bytes)", requestId, bytesRead);

                // Track namespace tuple
                long tupleLength = VarInt.read(accumulatedBuffer);
                StringBuilder trackNamespace = new StringBuilder();
                for (int i = 0; i < tupleLength; i++) {
                    if (i > 0) trackNamespace.append("/");
                    long strLen = VarInt.read(accumulatedBuffer);
                    byte[] strBytes = new byte[(int) strLen];
                    accumulatedBuffer.readBytes(strBytes);
                    trackNamespace.append(new String(strBytes, java.nio.charset.StandardCharsets.UTF_8));
                }

                // Number of parameters
                long numParams = VarInt.read(accumulatedBuffer);
                // Skip parameters (per draft-14 Section 1.4.2: odd=length+bytes, even=varint)
                for (int i = 0; i < numParams; i++) {
                    long paramKey = VarInt.read(accumulatedBuffer);
                    if (paramKey % 2 == 1) {
                        // Odd parameter - has length prefix
                        long paramLen = VarInt.read(accumulatedBuffer);
                        accumulatedBuffer.skipBytes((int) paramLen);
                    } else {
                        // Even parameter - no length, just varint value
                        VarInt.read(accumulatedBuffer);
                    }
                }

                logger.info("SUBSCRIBE_NAMESPACE: requestId={}, namespace={}", requestId, trackNamespace);

                // Send SUBSCRIBE_NAMESPACE_OK with same Request ID
                sendSubscribeNamespaceOk(requestId, 0);

                // Send PUBLISH messages for existing tracks per draft-14 Section 9.28
                // "If the SUBSCRIBE_NAMESPACE is successful, the publisher will immediately
                // forward existing PUBLISH messages that match the Track Namespace Prefix"
                if (catalog != null && catalog.getTracks() != null) {
                    for (var catalogTrack : catalog.getTracks()) {
                        String trackName = catalogTrack.getName();
                        Long trackAlias = trackAliases.get(trackName);
                        if (trackAlias != null) {
                            sendPublish(trackName, trackAlias);
                        }
                    }
                }

                // Create subscriptions for ALL tracks in this namespace
                // Get all track names from the catalog
                List<Subscription> trackSubscriptions = new ArrayList<>();

                if (catalog != null && catalog.getTracks() != null) {
                    for (var catalogTrack : catalog.getTracks()) {
                        String trackName = catalogTrack.getName();
                        Long trackAlias = trackAliases.get(trackName);

                        if (trackAlias == null) {
                            logger.warn("No track alias for track {}, skipping", trackName);
                            continue;
                        }

                        // Create subscription for this track
                        // Use requestId as the subscription ID for namespace subscriptions
                        Subscription subscription = new Subscription(
                            requestId,
                            trackNamespace.toString(),
                            trackName,
                            trackAlias,
                            0, 0, // startGroup, startObject (start from beginning)
                            Long.MAX_VALUE, Long.MAX_VALUE // endGroup, endObject (no end)
                        );

                        trackSubscriptions.add(subscription);
                        logger.info("Created namespace subscription for track: {}, requestId={}, alias={}",
                                trackName, requestId, trackAlias);

                        // Send current catalog/timeline if this is those tracks
                        if (trackName.equals("catalog")) {
                            sendCurrentCatalogToSubscription(subscription);
                        } else if (trackName.equals("timeline")) {
                            sendCurrentTimelineToSubscription(subscription);
                        }
                    }
                }

                // Store all track subscriptions under this namespace requestId
                namespaceSubscriptions.put(requestId, trackSubscriptions);

                logger.info("SUBSCRIBE_NAMESPACE complete: requestId={}, namespace={}, {} tracks",
                        requestId, trackNamespace, catalog != null ? catalog.getTracks().size() : 0);

            } catch (Exception e) {
                logger.error("Error parsing SUBSCRIBE_NAMESPACE message", e);
            }
        }

        private void handleUnsubscribeNamespace(int payloadLength) {
            logger.info("Received UNSUBSCRIBE_NAMESPACE from subscriber");

            try {
                // Parse UNSUBSCRIBE_NAMESPACE message (draft-14 Section 9.31)
                // Format: Track Namespace Prefix (tuple) - NO Request ID!
                long tupleLength = VarInt.read(accumulatedBuffer);
                StringBuilder trackNamespace = new StringBuilder();
                for (int i = 0; i < tupleLength; i++) {
                    if (i > 0) trackNamespace.append("/");
                    long strLen = VarInt.read(accumulatedBuffer);
                    byte[] strBytes = new byte[(int) strLen];
                    accumulatedBuffer.readBytes(strBytes);
                    trackNamespace.append(new String(strBytes, java.nio.charset.StandardCharsets.UTF_8));
                }

                logger.info("UNSUBSCRIBE_NAMESPACE: namespace={}", trackNamespace);

                // Find and remove namespace subscriptions matching this namespace
                namespaceSubscriptions.entrySet().removeIf(entry -> {
                    List<Subscription> subs = entry.getValue();
                    if (!subs.isEmpty() && subs.get(0).getTrackNamespace().equals(trackNamespace.toString())) {
                        long requestId = entry.getKey();
                        logger.info("Removed namespace subscription: requestId={}, namespace={}, {} tracks",
                                requestId, trackNamespace, subs.size());
                        closeAllStreamsForSubscription(requestId);
                        return true;
                    }
                    return false;
                });

            } catch (Exception e) {
                logger.error("Error parsing UNSUBSCRIBE_NAMESPACE message", e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Control stream error", cause);
            continuePublishing.set(false);
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logger.warn("Control stream closed");
            continuePublishing.set(false);
            if (accumulatedBuffer != null) {
                accumulatedBuffer.release();
            }
        }
    }
}
