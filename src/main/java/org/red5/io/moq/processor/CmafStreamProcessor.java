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
import org.red5.io.moq.cmaf.deserialize.CmafDeserializer;
import org.red5.io.moq.cmaf.model.MoofBox;
import org.red5.io.moq.model.FrameData;
import org.red5.io.moq.model.TimeEventData;
import org.red5.io.moq.model.TrackEvent;
import org.red5.io.moq.model.TrackType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Processes MP4 stream using the red5-moq-cmaf library for proper box parsing.
 * This replaces the simplified Mp4StreamProcessor with full CMAF fragment parsing.
 */
public class CmafStreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CmafStreamProcessor.class);

    private final BlockingQueue<List<TrackEvent>> eventQueue;
    private final CmafDeserializer deserializer;
    private ByteBuffer buffer;
    private ProcessorState state;
    private final List<Byte> initSegment;
    private boolean moovProcessed;
    private boolean ftypProcessed;
    private long frameCount;
    private final Map<Integer, TrackType> trackMap;
    private final Map<Integer, String> codecMap;
    private long keyframeCount;
    private int lastGroupFrameCount;
    private final Map<Integer, Long> trackObjectCounters;
    private byte[] lastPrftBox;
    private byte[] lastMoof;
    private boolean lastFrameWasKeyframe;
    private int lastTrackId;
    private long lastNtpTimestamp;
    private long lastMediaTime;

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

    public CmafStreamProcessor(BlockingQueue<List<TrackEvent>> eventQueue) {
        this.eventQueue = eventQueue;
        this.deserializer = new CmafDeserializer();
        this.buffer = ByteBuffer.allocate(10 * 1024 * 1024); // 10MB buffer for large fragments
        this.buffer.flip(); // Start in read mode
        this.state = ProcessorState.READING_BOX_HEADER;
        this.initSegment = new ArrayList<>();
        this.moovProcessed = false;
        this.ftypProcessed = false;
        this.frameCount = 0;
        this.trackMap = new HashMap<>();
        this.codecMap = new HashMap<>();
        this.keyframeCount = 0;
        this.lastGroupFrameCount = 0;
        this.trackObjectCounters = new HashMap<>();
        this.lastPrftBox = new byte[0];
        this.lastMoof = new byte[0];
        this.lastFrameWasKeyframe = false;
        this.lastTrackId = 0;
        this.lastNtpTimestamp = 0;
        this.lastMediaTime = 0;
    }

    /**
     * Process the MP4 stream from an InputStream.
     */
    public void processStream(InputStream inputStream) throws IOException {
        logger.info("Reading MP4 stream using CMAF deserializer...");

        byte[] readBuffer = new byte[8192];
        int bytesRead;
        long totalBytesRead = 0;
        int readCount = 0;

        while ((bytesRead = inputStream.read(readBuffer)) != -1) {
            totalBytesRead += bytesRead;
            readCount++;

            // Log every 10 reads or when we get significant data
            if (readCount % 10 == 0 || bytesRead > 1000) {
                logger.info("Stream read #{}: {} bytes (total: {} bytes)", readCount, bytesRead, totalBytesRead);
            }
            logger.debug("Read {} bytes from stream", bytesRead);

            // Append to our buffer
            buffer.compact();

            // Check if we have enough space
            if (buffer.remaining() < bytesRead) {
                logger.error("Buffer overflow: remaining={}, bytesRead={}", buffer.remaining(), bytesRead);
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
        int boxesProcessed = 0;
        while (true) {
            if (state == ProcessorState.READING_BOX_HEADER) {
                if (buffer.remaining() < 8) {
                    // Need at least 8 bytes for box header
                    logger.debug("Waiting for box header: only {} bytes available", buffer.remaining());
                    break;
                }

                int position = buffer.position();
                long size = Integer.toUnsignedLong(buffer.getInt());
                byte[] boxType = new byte[4];
                buffer.get(boxType);

                long actualSize;
                if (size == 1) {
                    // Extended 64-bit size
                    if (buffer.remaining() < 8) {
                        buffer.position(position); // Reset
                        logger.debug("Waiting for extended box size");
                        break;
                    }
                    actualSize = buffer.getLong();
                } else {
                    actualSize = size;
                }

                String boxTypeStr = new String(boxType);
                logger.info("Found MP4 box: {} (size: {} bytes)", boxTypeStr, actualSize);

                // Reset to start of box
                buffer.position(position);

                currentBox = new BoxInfo(boxType, actualSize);
                state = ProcessorState.READING_BOX_DATA;

            } else if (state == ProcessorState.READING_BOX_DATA) {
                if (buffer.remaining() < currentBox.boxSize) {
                    // Need more data
                    logger.debug("Waiting for box data: have {}/{} bytes", buffer.remaining(), currentBox.boxSize);
                    break;
                }

                byte[] boxData = new byte[(int) currentBox.boxSize];
                buffer.get(boxData);

                try {
                    processBox(currentBox.boxType, boxData);
                    boxesProcessed++;
                } catch (Exception e) {
                    logger.error("Error processing box", e);
                }

                state = ProcessorState.READING_BOX_HEADER;
            }
        }

        if (boxesProcessed > 0) {
            logger.debug("Processed {} boxes in this buffer cycle", boxesProcessed);
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
            default -> logger.debug("Skipping box type: {}", boxTypeStr);
        }
    }

    private void processFtypBox(byte[] data) {
        if (!ftypProcessed) {
            logger.info("Processing ftyp box (size: {} bytes)", data.length);
            for (byte b : data) {
                initSegment.add(b);
            }
            ftypProcessed = true;
        }
    }

    private void processMoovBox(byte[] data) {
        if (!moovProcessed) {
            logger.debug("Processing moov box (size: {} bytes)", data.length);

            for (byte b : data) {
                initSegment.add(b);
            }

            // Build track map from moov box
            // For now, assume track 1 is video, track 2 is audio (will be extracted from actual fragments)
            trackMap.put(1, TrackType.VIDEO);
            trackMap.put(2, TrackType.AUDIO);
            codecMap.put(1, "avc1.42E01E");
            codecMap.put(2, "mp4a.40.2");

            logger.info("Built track map with {} tracks", trackMap.size());

            // Build catalog
            Catalog catalog = new Catalog();
            catalog.setStreamingFormat(1);  // WARP format
            catalog.setStreamingFormatVersion("0.2");
            catalog.setSupportsDeltaUpdates(false);
            List<String> depends = new ArrayList<>();

            for (Map.Entry<Integer, TrackType> entry : trackMap.entrySet()) {
                int trackId = entry.getKey();
                TrackType trackType = entry.getValue();
                String codec = codecMap.getOrDefault(trackId, "unknown");

                String initData = Base64.getEncoder().encodeToString(listToByteArray(initSegment));
                String trackName = "track-" + trackId;  // Use "track-N" format expected by relay
                depends.add(trackName);

                CatalogTrack track = new CatalogTrack(trackName);
                track.setPackaging(PackagingType.LOC.getValue());
                track.setRenderGroup(1);
                track.setAltGroup(1);
                track.setInitData(initData);
                track.setLabel(trackType.toString());

                SelectionParams selectionParams = new SelectionParams();
                selectionParams.setCodec(codec);
                if (trackType == TrackType.VIDEO) {
                    selectionParams.setWidth(0);
                    selectionParams.setHeight(0);
                    selectionParams.setFramerate(0);
                    selectionParams.setBitrate(0);
                } else if (trackType == TrackType.AUDIO) {
                    selectionParams.setSamplerate(0);
                    selectionParams.setChannelConfig("2");
                    selectionParams.setBitrate(0);
                }
                track.setSelectionParams(selectionParams);

                catalog.getTracks().add(track);
            }

            // Add timeline track
            CatalogTrack timelineTrack = new CatalogTrack("timeline");
            timelineTrack.setPackaging(PackagingType.LOC.getValue());
            timelineTrack.setRenderGroup(1);
            timelineTrack.setLabel("timeline");
            timelineTrack.setDepends(depends);

            SelectionParams timelineParams = new SelectionParams();
            timelineParams.setMimeType("text/csv");
            timelineTrack.setSelectionParams(timelineParams);

            catalog.getTracks().add(timelineTrack);

            // Add catalog track (self-describing)
            CatalogTrack catalogTrack = new CatalogTrack("catalog");
            catalogTrack.setPackaging(PackagingType.LOC.getValue());
            catalogTrack.setRenderGroup(1);
            catalogTrack.setLabel("catalog");

            SelectionParams catalogParams = new SelectionParams();
            catalogParams.setMimeType("application/json");
            catalogTrack.setSelectionParams(catalogParams);

            catalog.getTracks().add(catalogTrack);

            try {
                eventQueue.put(List.of(new TrackEvent.CatalogEvent(catalog)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while putting catalog event", e);
            }

            moovProcessed = true;
        }
    }

    private void processMoofBox(byte[] moofData) {
        logger.info("Processing moof box (size: {} bytes)", moofData.length);

        // Store moof data for combining with subsequent mdat
        lastMoof = moofData;

        try {
            // Use the CmafDeserializer to properly parse the moof box
            ByteBuffer buffer = ByteBuffer.wrap(moofData);
            MoofBox moof = new MoofBox();
            moof.deserialize(buffer);

            // Extract track ID from the first traf box
            if (!moof.getTrafs().isEmpty()) {
                MoofBox.TrafBox traf = moof.getTrafs().get(0);
                if (traf.getTfhd() != null) {
                    int trackId = (int) traf.getTfhd().getTrackId();
                    lastTrackId = trackId;
                    logger.info("Extracted track ID from tfhd: {}", trackId);

                    // Use proper keyframe detection from sample flags
                    boolean isKeyframe = isKeyframeFromTrun(moof);
                    lastFrameWasKeyframe = isKeyframe;

                    if (isKeyframe) {
                        logger.info("Keyframe detected from sample flags! Frame count: {}", frameCount);
                        keyframeCount++;
                        lastGroupFrameCount = 1;

                        // Reset object counters for new GOP
                        trackObjectCounters.clear();
                        logger.info("Reset object counters for all tracks due to keyframe (new GOP)");
                    } else {
                        logger.info("Regular frame (not keyframe), group frame count: {}", lastGroupFrameCount + 1);
                        lastGroupFrameCount++;
                    }
                } else {
                    logger.warn("Moof box has traf but no tfhd");
                }
            } else {
                logger.warn("Moof box has no traf boxes");
            }

        } catch (Exception e) {
            logger.error("Error processing moof box with CmafDeserializer", e);
        }
    }

    /**
     * Detect if this is a keyframe by checking sample flags in trun box.
     * Per ISO/IEC 14496-12, sample_flags indicate sync samples (keyframes).
     *
     * The red5-moq-cmaf library now has full SampleFlags support with proper
     * parsing of individual sample flags from trun boxes.
     */
    private boolean isKeyframeFromTrun(MoofBox moof) {
        if (moof.getTrafs().isEmpty()) {
            logger.debug("No traf boxes in moof, defaulting to non-keyframe");
            return false;
        }

        MoofBox.TrafBox traf = moof.getTrafs().get(0);

        // Check if traf has any trun boxes
        if (traf.getTruns().isEmpty()) {
            logger.debug("No trun boxes in traf, defaulting to non-keyframe");
            return false;
        }

        MoofBox.TrunBox trun = traf.getTruns().get(0);

        // Check the first sample's flags to determine if this fragment is a keyframe
        // In video encoding, the first sample of a fragment determines the fragment type
        if (!trun.getSamples().isEmpty()) {
            MoofBox.TrunBox.Sample firstSample = trun.getSamples().get(0);
            if (firstSample.getSampleFlags() != null) {
                boolean isSyncSample = firstSample.getSampleFlags().isSyncSample();
                logger.debug("First sample sync status from flags: {} (flags: 0x{}, sample count: {})",
                        isSyncSample,
                        Integer.toHexString(firstSample.getSampleFlags().getFlags()),
                        trun.getSamples().size());
                return isSyncSample;
            } else {
                logger.debug("First sample has no sample flags");
            }
        } else {
            logger.debug("Trun has no samples");
        }

        // Fallback: check first sample flags from tfhd
        if (traf.getTfhd() != null && traf.getTfhd().getDefaultSampleFlags() != null) {
            boolean isSyncSample = traf.getTfhd().getDefaultSampleFlags().isSyncSample();
            logger.debug("Using default sample flags from tfhd: {}", isSyncSample);
            return isSyncSample;
        }

        // Second fallback: use base media decode time from tfdt to detect GOP boundaries
        MoofBox.TfdtBox tfdt = traf.getTfdt();
        if (tfdt != null) {
            long baseMediaDecodeTime = tfdt.getBaseMediaDecodeTime();
            logger.debug("Base media decode time: {}", baseMediaDecodeTime);
            // If this is the first frame (time=0) or a multiple of typical GOP duration, it's likely a keyframe
            if (baseMediaDecodeTime == 0 || (baseMediaDecodeTime % 1000000) == 0) {
                logger.debug("Keyframe detected from base media decode time heuristic");
                return true;
            }
        }

        // Final fallback: assume keyframe every 25 frames (heuristic)
        boolean isKeyframe = (frameCount % 25 == 0);
        logger.debug("Using frame count heuristic: frame={}, isKeyframe={}", frameCount, isKeyframe);
        return isKeyframe;
    }

    private void processPrftBox(byte[] data) {
        // Parse prft box to extract NTP timestamp and media time
        if (data.length >= 24) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            bb.position(12); // Skip size, type, version, flags, reference_track_id
            lastNtpTimestamp = bb.getLong();
            lastMediaTime = bb.getInt() & 0xFFFFFFFFL;
        }
        lastPrftBox = data;
    }

    private void processMdatBox(byte[] mdatData) {
        logger.info("Processing mdat box (size: {} bytes)", mdatData.length);

        if (lastMoof.length == 0) {
            logger.warn("Received mdat without preceding moof, skipping");
            return;
        }

        try {
            // Use the track ID extracted from the moof
            int trackId = lastTrackId;
            TrackType trackType = trackMap.getOrDefault(trackId, TrackType.OTHER);

            // Get object counter for this track (or initialize to 1)
            long objectId = trackObjectCounters.getOrDefault(trackId, 1L);
            trackObjectCounters.put(trackId, objectId + 1);

            // Extract mdat payload (skip 8-byte box header)
            byte[] payload = new byte[mdatData.length - 8];
            System.arraycopy(mdatData, 8, payload, 0, payload.length);

            // Create frame data with actual payload
            ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
            String trackName = "track-" + trackId;  // Use "track-N" format expected by relay

            logger.info("Creating frame data: track={}, type={}, group={}, object={}, payload={} bytes",
                    trackName, trackType, keyframeCount, objectId, payload.length);
            FrameData frameData = new FrameData(
                    trackName,
                    trackType,
                    payloadBuf,
                    keyframeCount,
                    objectId
            );

            frameCount++;

            // Send appropriate event based on whether this is a keyframe
            if (lastFrameWasKeyframe) {
                logger.info("Queueing keyframe event: track={}, type={}, group={}, object={}, payload={} bytes",
                        trackName, trackType, keyframeCount, objectId, payload.length);

                // Create time event for keyframe
                TimeEventData timeEvent = new TimeEventData(
                        trackId,
                        lastNtpTimestamp,
                        lastMediaTime,
                        keyframeCount
                );

                try {
                    eventQueue.put(List.of(
                            new TrackEvent.Keyframe(frameData),
                            new TrackEvent.TimeEvent(timeEvent)
                    ));
                    logger.info("Successfully queued keyframe event to event queue (queue size: {})", eventQueue.size());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted while putting keyframe event", e);
                }
            } else {
                logger.info("Queueing frame event: track={}, type={}, group={}, object={}, payload={} bytes",
                        trackId, trackType, keyframeCount, objectId, payload.length);

                try {
                    eventQueue.put(List.of(new TrackEvent.Frame(frameData)));
                    logger.info("Successfully queued frame event to event queue (queue size: {})", eventQueue.size());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted while putting frame event", e);
                }
            }

        } catch (Exception e) {
            logger.error("Error processing mdat box", e);
        }
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
