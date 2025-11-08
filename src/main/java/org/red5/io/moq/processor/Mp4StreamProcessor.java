package org.red5.io.moq.processor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.red5.io.moq.catalog.Catalog;
import org.red5.io.moq.catalog.CatalogTrack;
import org.red5.io.moq.catalog.PackagingType;
import org.red5.io.moq.catalog.SelectionParams;
import org.red5.io.moq.model.FrameData;
import org.red5.io.moq.model.TimeEventData;
import org.red5.io.moq.model.TrackEvent;
import org.red5.io.moq.model.TrackType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Processes MP4 stream from stdin or TCP and extracts frames and metadata.
 */
public class Mp4StreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(Mp4StreamProcessor.class);

    private final BlockingQueue<List<TrackEvent>> eventQueue;
    private ByteBuffer buffer; // Non-final to allow dynamic buffer growth
    private ProcessorState state;
    private final List<Byte> initSegment;
    private final List<Byte> lastMoof;
    private boolean moovProcessed;
    private boolean ftypProcessed;
    private long frameCount;
    private boolean isKeyframe;
    private final Map<Integer, TrackType> trackMap;
    private final Map<Integer, String> codecMap;
    private int lastTrackId;
    private TrackType lastTrackType;
    private long lastNtpTimestamp;
    private long lastMediaTime;
    private long keyframeCount;
    private int lastGroupFrameCount;
    private byte[] lastPrftBox;
    private final Map<Integer, Long> trackObjectCounters;

    private enum ProcessorState {
        READING_BOX_HEADER,
        READING_BOX_DATA
    }

    private static class BoxInfo {
        byte[] boxType;
        long boxSize;

        BoxInfo(byte[] boxType, long boxSize) {
            this.boxType = boxType;
            this.boxSize = boxSize;
        }
    }

    private BoxInfo currentBox;

    public Mp4StreamProcessor(BlockingQueue<List<TrackEvent>> eventQueue) {
        this.eventQueue = eventQueue;
        this.buffer = ByteBuffer.allocate(10 * 1024 * 1024); // 10MB buffer for large MP4 boxes (HD video keyframes)
        this.buffer.flip(); // Start in read mode
        this.state = ProcessorState.READING_BOX_HEADER;
        this.initSegment = new ArrayList<>();
        this.lastMoof = new ArrayList<>();
        this.moovProcessed = false;
        this.ftypProcessed = false;
        this.frameCount = 0;
        this.isKeyframe = false;
        this.trackMap = new HashMap<>();
        this.codecMap = new HashMap<>();
        this.lastTrackId = 0;
        this.lastTrackType = TrackType.OTHER;
        this.lastNtpTimestamp = 0;
        this.lastMediaTime = 0;
        this.keyframeCount = 0;
        this.lastGroupFrameCount = 0;
        this.lastPrftBox = new byte[0];
        this.trackObjectCounters = new HashMap<>();
    }

    /**
     * Process the MP4 stream from an InputStream.
     */
    public void processStream(InputStream inputStream) throws IOException {
        logger.info("Reading MP4 stream from provided input stream...");

        byte[] readBuffer = new byte[8192];
        int bytesRead;

        while ((bytesRead = inputStream.read(readBuffer)) != -1) {
            logger.debug("Read {} bytes from stream", bytesRead);

            // Append to our buffer
            buffer.compact();

            // Check if we have enough space
            if (buffer.remaining() < bytesRead) {
                logger.error("Buffer overflow: remaining={}, bytesRead={}, position={}, limit={}, capacity={}",
                    buffer.remaining(), bytesRead, buffer.position(), buffer.limit(), buffer.capacity());
                // Allocate a larger buffer
                ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
                buffer.flip();
                newBuffer.put(buffer);
                buffer = newBuffer;
                logger.info("Expanded buffer to {} MB", buffer.capacity() / (1024 * 1024));
            }

            buffer.put(readBuffer, 0, bytesRead);
            buffer.flip();

            processBuffer();
        }

        logger.info("End of input stream");
    }

    private void processBuffer() {
        while (true) {
            if (state == ProcessorState.READING_BOX_HEADER) {
                if (buffer.remaining() < 8) {
                    // Need at least 8 bytes for box header
                    break;
                }

                int position = buffer.position();
                long size = Integer.toUnsignedLong(buffer.getInt());
                byte[] boxType = new byte[4];
                buffer.get(boxType);

                long actualSize;
                int headerSize;

                if (size == 1) {
                    // Extended 64-bit size
                    if (buffer.remaining() < 8) {
                        buffer.position(position); // Reset
                        break;
                    }
                    actualSize = buffer.getLong();
                    headerSize = 16;
                } else {
                    actualSize = size;
                    headerSize = 8;
                }

                logger.debug("Found MP4 box: {} (size: {} bytes)", new String(boxType), actualSize);

                // Reset to start of box
                buffer.position(position);

                currentBox = new BoxInfo(boxType, actualSize);
                state = ProcessorState.READING_BOX_DATA;

            } else if (state == ProcessorState.READING_BOX_DATA) {
                if (buffer.remaining() < currentBox.boxSize) {
                    // Need more data
                    break;
                }

                byte[] boxData = new byte[(int) currentBox.boxSize];
                buffer.get(boxData);

                try {
                    processBox(currentBox.boxType, boxData);
                } catch (Exception e) {
                    logger.error("Error processing box", e);
                }

                state = ProcessorState.READING_BOX_HEADER;
            }
        }
    }

    private void processBox(byte[] boxType, byte[] data) {
        String boxTypeStr = new String(boxType);

        switch (boxTypeStr) {
            case "ftyp" -> processFtypBox(data);
            case "moov" -> processMoovBox(data);
            case "moof" -> processMoofBox(data);
            case "mdat" -> processMdatBox(data);
            case "prft" -> processPrftBox(data);
            default -> logger.debug("Skipping unknown box type: {}", boxTypeStr);
        }
    }

    private void processFtypBox(byte[] data) {
        if (!ftypProcessed) {
            logger.info("Processing ftyp box (size: {} bytes)", data.length);
            for (byte b : data) {
                initSegment.add(b);
            }
            ftypProcessed = true;
        } else {
            logger.warn("Skipping ftyp box. Already processed");
        }
    }

    private void processMoovBox(byte[] data) {
        if (!moovProcessed) {
            logger.debug("Processing moov box (size: {} bytes)", data.length);

            for (byte b : data) {
                initSegment.add(b);
            }

            // Build track map from moov box (simplified - in real implementation, parse the moov box)
            // For now, we'll assume track 1 is video, track 2 is audio
            trackMap.put(1, TrackType.VIDEO);
            trackMap.put(2, TrackType.AUDIO);
            codecMap.put(1, "avc1.42E01E");
            codecMap.put(2, "mp4a.40.2");

            logger.info("Built track map with {} tracks", trackMap.size());

            // Build catalog per draft-ietf-moq-catalogformat-01
            Catalog catalog = new Catalog();
            catalog.setStreamingFormat(1);  // WARP format
            catalog.setStreamingFormatVersion("0.2");  // draft-ietf-moq-warp-01
            catalog.setSupportsDeltaUpdates(false);
            List<String> depends = new ArrayList<>();

            for (Map.Entry<Integer, TrackType> entry : trackMap.entrySet()) {
                int trackId = entry.getKey();
                TrackType trackType = entry.getValue();
                String codec = codecMap.getOrDefault(trackId, "unknown");

                // Base64 encode init segment (NOTE: Per LOC spec, should be codec extradata, not ftyp+moov)
                String initData = Base64.getEncoder().encodeToString(listToByteArray(initSegment));
                String trackName = "track-" + trackId;  // Use "track-N" format expected by relay
                depends.add(trackName);

                // Create track with new structure per draft-ietf-moq-catalogformat-01
                CatalogTrack track = new CatalogTrack(trackName);
                track.setPackaging(PackagingType.LOC.getValue());
                track.setRenderGroup(1);
                track.setAltGroup(1);
                track.setInitData(initData);
                track.setLabel(trackType.toString());

                // Set selection parameters
                SelectionParams selectionParams = new SelectionParams();
                selectionParams.setCodec(codec);
                if (trackType == TrackType.VIDEO) {
                    // TODO: Parse actual width/height/framerate from moov box
                    selectionParams.setWidth(0);
                    selectionParams.setHeight(0);
                    selectionParams.setFramerate(0);
                    selectionParams.setBitrate(0);
                } else if (trackType == TrackType.AUDIO) {
                    // TODO: Parse actual samplerate/channels from moov box
                    selectionParams.setSamplerate(0);
                    selectionParams.setChannelConfig("2");
                    selectionParams.setBitrate(0);
                }
                track.setSelectionParams(selectionParams);

                catalog.getTracks().add(track);
            }

            // Add timeline track (NOTE: Timeline is not a packaging type, should be a separate track)
            CatalogTrack timelineTrack = new CatalogTrack("timeline");
            timelineTrack.setPackaging(PackagingType.LOC.getValue());
            timelineTrack.setRenderGroup(1);
            timelineTrack.setLabel("timeline");
            timelineTrack.setDepends(depends);

            SelectionParams timelineParams = new SelectionParams();
            timelineParams.setMimeType("text/csv");
            timelineTrack.setSelectionParams(timelineParams);

            catalog.getTracks().add(timelineTrack);

            try {
                eventQueue.put(List.of(new TrackEvent.CatalogEvent(catalog)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while putting catalog event", e);
            }

            moovProcessed = true;
        } else {
            logger.warn("Skipping moov box. Already processed");
        }
    }

    private void processMoofBox(byte[] data) {
        logger.debug("Processing moof box (size: {} bytes)", data.length);

        lastTrackId = getTrackIdFromMoof(data);
        lastTrackType = trackMap.getOrDefault(lastTrackId, TrackType.OTHER);

        // Simplified keyframe detection (check for sync sample flags)
        isKeyframe = parseMoofForKeyframe(data) && lastTrackType == TrackType.VIDEO;

        if (isKeyframe) {
            logger.info("🔑 Keyframe detected! Last group frame count: {}", lastGroupFrameCount);
            keyframeCount++;
            lastGroupFrameCount = 1;

            // Reset object counters
            trackObjectCounters.clear();
            logger.info("Reset object counters for all tracks due to keyframe");

            ByteBuf payload = Unpooled.wrappedBuffer(listToByteArray(lastMoof));
            FrameData frameData = new FrameData(
                    String.valueOf(lastTrackId),
                    lastTrackType,
                    payload,
                    keyframeCount,
                    1
            );

            TimeEventData timeEvent = new TimeEventData(
                    lastTrackId,
                    lastNtpTimestamp,
                    lastMediaTime,
                    keyframeCount
            );

            try {
                eventQueue.put(List.of(
                        new TrackEvent.Keyframe(frameData),
                        new TrackEvent.TimeEvent(timeEvent)
                ));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while putting keyframe event", e);
            }

            lastMoof.clear();
        } else {
            lastGroupFrameCount++;
        }

        // Store moof data
        lastMoof.clear();
        for (byte b : data) {
            lastMoof.add(b);
        }
    }

    private void processMdatBox(byte[] data) {
        logger.debug("Processing mdat box (size: {} bytes)", data.length);

        // Increment object counter
        long objectId = trackObjectCounters.compute(lastTrackId, (k, v) -> v == null ? 1 : v + 1);

        // Create payload with prft + moof + mdat
        List<Byte> payload = new ArrayList<>();
        for (byte b : lastPrftBox) payload.add(b);
        for (byte b : lastMoof) payload.add(b);
        for (byte b : data) payload.add(b);

        ByteBuf payloadBuf = Unpooled.wrappedBuffer(listToByteArray(payload));
        FrameData frameData = new FrameData(
                String.valueOf(lastTrackId),
                lastTrackType,
                payloadBuf,
                keyframeCount,
                objectId
        );

        try {
            eventQueue.put(List.of(new TrackEvent.Frame(frameData)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while putting frame event", e);
        }

        isKeyframe = false;
        frameCount++;
    }

    private void processPrftBox(byte[] data) {
        // Parse prft box to extract NTP timestamp and media time
        // Simplified parsing
        if (data.length >= 24) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            bb.position(12); // Skip size, type, version, flags, reference_track_id
            lastNtpTimestamp = bb.getLong();
            lastMediaTime = bb.getInt() & 0xFFFFFFFFL;
        }
        lastPrftBox = data;
    }

    private int getTrackIdFromMoof(byte[] data) {
        // Simplified: In real implementation, parse moof box structure
        // For now, assume track 1
        return 1;
    }

    private boolean parseMoofForKeyframe(byte[] data) {
        // Simplified: In real implementation, parse traf/trun boxes
        // For now, assume every 25th frame is a keyframe
        return frameCount % 25 == 0;
    }

    private byte[] listToByteArray(List<Byte> list) {
        byte[] array = new byte[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    public BlockingQueue<List<TrackEvent>> getEventQueue() {
        return eventQueue;
    }
}
