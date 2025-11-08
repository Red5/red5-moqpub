package org.red5.io.moq.timeline;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.red5.io.moq.model.TrackData;
import org.red5.io.moq.model.TrackType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a timeline track with event records.
 */
public class Timeline implements TrackData {
    private static final Logger logger = LoggerFactory.getLogger(Timeline.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("track_name")
    private String trackName = "timeline";

    @JsonProperty("version")
    private int version = 0;

    @JsonProperty("format_version")
    private int formatVersion = 1;

    @JsonProperty("generated_at")
    private long generatedAt;

    @JsonProperty("records")
    @JsonAlias("timeline")
    private List<TimelineRecord> records;

    public Timeline() {
        this.records = new ArrayList<>();
        this.generatedAt = System.currentTimeMillis();
    }

    public Timeline(List<TimelineRecord> records) {
        this.records = records != null ? records : new ArrayList<>();
        this.generatedAt = System.currentTimeMillis();
    }

    @Override
    public String getTrackName() {
        return trackName;
    }

    @Override
    public TrackType getTrackType() {
        return TrackType.TIMELINE;
    }

    @Override
    @JsonIgnore
    public ByteBuf getPayload() {
        try {
            String json = MAPPER.writeValueAsString(this);
            return Unpooled.wrappedBuffer(json.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize timeline", e);
        }
    }

    public boolean addRecords(List<TimelineRecord> newRecords) {
        boolean changed = false;
        for (TimelineRecord record : newRecords) {
            // Skip if a record with the same media_pts already exists
            boolean exists = records.stream()
                    .anyMatch(r -> r.getMediaPts() == record.getMediaPts());

            if (exists) {
                logger.warn("Skipping record with media_pts {}", record.getMediaPts());
                continue;
            }

            changed = true;
            record.setVersion(this.version + 1);
            records.add(record);
        }

        if (changed) {
            this.version++;
            this.generatedAt = System.currentTimeMillis();
        }

        return changed;
    }

    public String getTrackNameField() {
        return trackName;
    }

    public void setTrackName(String trackName) {
        this.trackName = trackName;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
    }

    public long getGeneratedAt() {
        return generatedAt;
    }

    public void setGeneratedAt(long generatedAt) {
        this.generatedAt = generatedAt;
    }

    public List<TimelineRecord> getRecords() {
        return records;
    }

    public void setRecords(List<TimelineRecord> records) {
        this.records = records;
    }

    @Override
    public String toString() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "Timeline{error=" + e.getMessage() + "}";
        }
    }
}
