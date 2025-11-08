# Red5 MOQ Publisher in Java

A Java 21 implementation of the MOQtail publisher that processes MP4 streams and publishes them via MOQ Transport over QUIC using Netty. Implements **MOQ Transport Draft 14 ONLY** (draft-ietf-moq-transport-14).

This project is a Java port of the Rust MOQtail publisher from [demo-ibc25](https://github.com/moqtail/demo-ibc25).

## Features

- Processes MP4 streams from stdin or TCP socket
- Extracts video/audio frames and metadata from MP4 boxes
- Publishes via MOQ Transport protocol over QUIC
- Supports catalog and timeline tracks
- Built with Java 21, Netty 4.1.127.Final, and Netty HTTP/3 4.2.6.Final (includes QUIC)

## Requirements

- Java 21 or higher
- Maven 3.8+
- MOQ Transport relay server supporting Draft 14

## Building

```bash
mvn clean package
```

This will create a fat JAR with all dependencies: `target/moqpub-0.3.0.jar`

## Usage

### From stdin (default)

```bash
ffmpeg -re -i input.mp4 -c copy -f mp4 -movflags frag_keyframe+empty_moov - | \
  java -jar target/moqpub-0.3.0.jar --url https://moq-relay.red5.net:8443
```

Local dev box with big buck bunny:

```bash
ffmpeg -re -i /media/mondain/terrorbyte/Videos/bbb-fullhd.mp4 -c copy -f mp4 -movflags frag_keyframe+empty_moov - | \
  java -jar target/moqpub-0.3.0.jar --url https://moq-relay.red5.net:8443
```

**Note**: Native MOQ clients use port **8443** with ALPN `"moq-00"`. Port 4433 is for WebTransport/browser clients using ALPN `"h3"`.

### From TCP socket

Terminal 1 - Start publisher:

```bash
java -jar target/moqpub-0.3.0.jar --input-source tcp --tcp-port 12346
```

Terminal 2 - Stream via ffmpeg:

```bash
ffmpeg -re -i input.mp4 -c copy -f mp4 -movflags frag_keyframe+empty_moov tcp://127.0.0.1:12346
```

## Command-line Options

- `--name <name>` - Stream name to publish (default: "test")
- `--url <url>` - MOQ Transport URL (default: "https://127.0.0.1:8443")
- `--input-source <source>` - Input source: "stdin" or "tcp" (default: "stdin")
- `--tcp-port <port>` - TCP port for input when using tcp source (default: 12346)
- `--help` - Show help message
- `--version` - Show version

## Architecture

### Components

1. **Mp4StreamProcessor** - Parses MP4 stream boxes (ftyp, moov, moof, mdat, prft) and extracts frames
2. **MoqClient** - Establishes QUIC connection and publishes tracks via MOQ Transport protocol
3. **Catalog** - Describes available tracks (video, audio, timeline)
4. **Timeline** - Contains event metadata with timestamps and locations
5. **TrackEvent** - Sealed interface for different event types (Keyframe, Frame, Catalog, etc.)

### Data Flow

```plain
MP4 Stream → Mp4StreamProcessor → Event Queue → MoqClient → QUIC/MOQ → Relay
                                                      ↓
                                                 Subscribers
```

## MOQ Transport Protocol

This implementation supports **MOQ Transport Draft 14** with the following features:

- **CLIENT_SETUP / SERVER_SETUP** (0x20/0x21) - Connection establishment with version negotiation
- **PUBLISH_NAMESPACE / PUBLISH_NAMESPACE_OK** (0x06/0x07) - Namespace announcement
- **VarInt Encoding** - QUIC variable-length integer encoding (RFC 9000) for all protocol messages
- **Request ID Management** - Client uses even IDs, server uses odd IDs
- **Role Parameter** - Publisher role specified in CLIENT_SETUP
- **Stream Per Group** - One unidirectional QUIC stream per group (GOP), reused for all objects in that group

### Protocol Implementation Status

Implemented:

- ✅ CLIENT_SETUP with ROLE parameter
- ✅ PUBLISH_NAMESPACE for namespace announcement
- ✅ Full varint encoding/decoding utilities
- ✅ SUBSCRIBE / UNSUBSCRIBE message parsing
- ✅ SUBSCRIBE_OK response transmission
- ✅ OBJECT_STREAM message encoding and transmission
- ✅ Unidirectional stream management (stream per group, not per object)
- ✅ GOAWAY message handling
- ✅ Stream lifecycle management (reuse within groups, close on group change)

Not Yet Implemented:

- ❌ FETCH operations
- ❌ SUBSCRIBE_DONE message handling
- ❌ Track status reporting

## Current Status & Limitations

This is a **functional implementation** of MOQ Transport Draft 14 publisher with core features working:

### What Works

- ✅ QUIC connection establishment with MOQ Transport ALPN (`moq-00`)
- ✅ CLIENT_SETUP message transmission (Draft 14 format)
- ✅ SERVER_SETUP response parsing with version negotiation
- ✅ PUBLISH_NAMESPACE message transmission
- ✅ PUBLISH_NAMESPACE_OK response parsing
- ✅ GOAWAY message handling with graceful shutdown
- ✅ MP4 stream parsing (ftyp, moov, moof, mdat boxes)
- ✅ Catalog generation with track metadata
- ✅ Event generation (keyframes, frames, timeline events)
- ✅ **SUBSCRIBE message handling** from subscribers
- ✅ **SUBSCRIBE_OK response** transmission
- ✅ **Object transmission** over QUIC unidirectional streams
- ✅ **OBJECT_STREAM message** encoding with headers and payloads
- ✅ **Subscription lifecycle** (SUBSCRIBE, SUBSCRIBE_OK, UNSUBSCRIBE)
- ✅ **Catalog track transmission** - Catalog sent as JSON on "catalog" track with versioning
- ✅ **Timeline track transmission** - Timeline sent as JSON on "timeline" track with versioning

### What's Missing (NOT YET IMPLEMENTED)

- ❌ **FETCH operations** - On-demand object fetching
- ❌ **SUBSCRIBE_DONE** - Subscription completion signaling
- ❌ **Track status reporting** - TRACK_STATUS_REQUEST/TRACK_STATUS messages
- ❌ **OBJECT_DATAGRAM** - Datagram-based object delivery (only OBJECT_STREAM implemented)

### Implementation Details

**Stream Management:**

- Uses **one QUIC unidirectional stream per group** (GOP - Group of Pictures)
- All objects (frames) within the same group are sent on the same stream
- When a new group starts (keyframe), the old stream is closed and a new one is opened
- This is more efficient than creating a stream per object as per draft-14 recommendations
- Catalog and timeline use group ID as version number, each version on its own stream

**Simplifications:**

- MP4 box parsing uses simplified keyframe heuristics (every 25th frame)
- Certificate validation disabled by default for testing
- No advanced MOQ features (subgroup prioritization, delivery timeouts, etc.)

**Status**: Media objects (video/audio frames) are now **fully transmitted** to subscribers over QUIC streams. The publisher can handle multiple concurrent subscriptions and delivers objects based on subscription ranges. Catalog and timeline tracks are also fully functional, providing metadata delivery to subscribers.

## Comparison with Rust Version

The Java implementation replicates the core functionality of the Rust version:

| Feature | Rust | Java |
|---------|------|------|
| MP4 Processing | ✓ | ✓ (simplified) |
| QUIC Transport | wtransport | Netty QUIC |
| MOQ Protocol Version | Draft 11 | **Draft 14** |
| MOQ Implementation | moqtail crate | Custom (partial) |
| VarInt Encoding | ✓ | ✓ (RFC 9000) |
| Catalog/Timeline | ✓ | ✓ |
| Event API | ✓ | ✗ (not implemented) |

## Dependencies

- **Netty Core** (4.1.127.Final) - Network application framework
- **Netty HTTP/3 Codec** (4.2.6.Final) - HTTP/3 and QUIC protocol implementation
- **ISO Parser** (1.9.56) - MP4 container parsing
- **Jackson** (2.18.2) - JSON serialization
- **Picocli** (4.7.6) - Command-line interface
- **SLF4J + Logback** - Logging

## Development

### Project Structure

```plain
src/main/java/org/red5/io/moq/
├── MoqPublisher.java          - Main application entry point
├── client/
│   ├── MoqClient.java         - MOQ Transport client (Draft 14)
│   └── InsecureTrustManagerFactory.java
├── protocol/
│   ├── MoqMessageType.java    - Draft 14 message type constants
│   └── VarInt.java            - QUIC varint encoding/decoding
├── processor/
│   └── Mp4StreamProcessor.java - MP4 stream processing
├── model/
│   ├── TrackType.java
│   ├── TrackData.java
│   ├── FrameData.java
│   ├── TrackEvent.java
│   └── TimeEventData.java
├── catalog/
│   ├── Catalog.java
│   ├── CatalogTrack.java
│   └── PackagingType.java
└── timeline/
    ├── Timeline.java
    ├── TimelineRecord.java
    └── Location.java
```

### Running in Development

```bash
# Compile
mvn compile

# Run
mvn exec:java -Dexec.mainClass="org.red5.io.moq.MoqPublisher" -Dexec.args="--help"
```

## License

Apache License 2.0 - See LICENSE file for details

## Contributing

Based on the MOQtail project. Contributions welcome!

## Diamond Sponsors

- Red5 - [red5.net](https://red5.net)

## Related Projects

- [MOQtail](https://github.com/moqtail/moqtail) - Rust implementation of MOQ Transport
- [demo-ibc25](https://github.com/moqtail/demo-ibc25) - Original Rust demo
- [Netty QUIC](https://github.com/netty/netty-incubator-codec-quic) - Netty QUIC codec
