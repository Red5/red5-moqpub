package org.red5.io.moq.model;

import io.netty.buffer.ByteBuf;

/**
 * Represents a single media frame with its metadata.
 */
public class FrameData implements TrackData {
    private final String trackName;
    private final TrackType trackType;
    private final ByteBuf payload;
    private final long groupId;
    private final long objectId;

    public FrameData(String trackName, TrackType trackType, ByteBuf payload, long groupId, long objectId) {
        this.trackName = trackName;
        this.trackType = trackType;
        this.payload = payload;
        this.groupId = groupId;
        this.objectId = objectId;
    }

    @Override
    public String getTrackName() {
        return trackName;
    }

    @Override
    public TrackType getTrackType() {
        return trackType;
    }

    @Override
    public ByteBuf getPayload() {
        return payload;
    }

    public long getGroupId() {
        return groupId;
    }

    public long getObjectId() {
        return objectId;
    }

    @Override
    public String toString() {
        return "FrameData{" +
                "trackName='" + trackName + '\'' +
                ", trackType=" + trackType +
                ", groupId=" + groupId +
                ", objectId=" + objectId +
                ", payloadSize=" + (payload != null ? payload.readableBytes() : 0) +
                '}';
    }
}
