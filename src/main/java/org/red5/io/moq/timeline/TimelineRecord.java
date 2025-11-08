package org.red5.io.moq.timeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a single record in the timeline track.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TimelineRecord {
    @JsonProperty("media_pts")
    private long mediaPts;

    @JsonProperty("start")
    private Location start;

    @JsonProperty("end")
    private Location end;

    @JsonProperty("wallclock")
    private long wallclock;

    @JsonProperty("metadata")
    private String metadata;

    @JsonProperty("version")
    private int version;

    public TimelineRecord() {}

    public TimelineRecord(long mediaPts, Location start, Location end, long wallclock, String metadata) {
        this.mediaPts = mediaPts;
        this.start = start;
        this.end = end;
        this.wallclock = wallclock;
        this.metadata = metadata;
        this.version = 1;
    }

    public long getMediaPts() {
        return mediaPts;
    }

    public void setMediaPts(long mediaPts) {
        this.mediaPts = mediaPts;
    }

    public Location getStart() {
        return start;
    }

    public void setStart(Location start) {
        this.start = start;
    }

    public Location getEnd() {
        return end;
    }

    public void setEnd(Location end) {
        this.end = end;
    }

    public long getWallclock() {
        return wallclock;
    }

    public void setWallclock(long wallclock) {
        this.wallclock = wallclock;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
