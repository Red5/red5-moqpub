package org.red5.io.moq.model;

/**
 * Represents a time synchronization event containing NTP timestamp and media time.
 */
public class TimeEventData {
    private final int trackId;
    private final long ntpTimestamp;
    private final long mediaTime;
    private final long keyframeCount;

    public TimeEventData(int trackId, long ntpTimestamp, long mediaTime, long keyframeCount) {
        this.trackId = trackId;
        this.ntpTimestamp = ntpTimestamp;
        this.mediaTime = mediaTime;
        this.keyframeCount = keyframeCount;
    }

    public int getTrackId() {
        return trackId;
    }

    public long getNtpTimestamp() {
        return ntpTimestamp;
    }

    public long getMediaTime() {
        return mediaTime;
    }

    public long getKeyframeCount() {
        return keyframeCount;
    }

    @Override
    public String toString() {
        return "TimeEventData{" +
                "trackId=" + trackId +
                ", ntpTimestamp=" + ntpTimestamp +
                ", mediaTime=" + mediaTime +
                ", keyframeCount=" + keyframeCount +
                '}';
    }
}
