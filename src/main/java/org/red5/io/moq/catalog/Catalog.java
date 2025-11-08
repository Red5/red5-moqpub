package org.red5.io.moq.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the catalog containing all track information.
 * Per draft-ietf-moq-catalogformat-01
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Catalog {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Catalog version (REQUIRED)
     * Section 3.2.1
     */
    @JsonProperty("version")
    private int version = 1;

    /**
     * Streaming format identifier (REQUIRED)
     * Section 3.2.2 - Must be 1 for WARP
     */
    @JsonProperty("streamingFormat")
    private Integer streamingFormat;

    /**
     * Streaming format version string (REQUIRED)
     * Section 3.2.3 - e.g., "0.2" for WARP draft-01
     */
    @JsonProperty("streamingFormatVersion")
    private String streamingFormatVersion;

    /**
     * Whether catalog supports delta updates via JSON PATCH (OPTIONAL)
     * Section 3.2.4
     */
    @JsonProperty("supportsDeltaUpdates")
    private Boolean supportsDeltaUpdates;

    /**
     * Common fields shared by all tracks (OPTIONAL)
     * Section 3.2.5
     */
    @JsonProperty("commonTrackFields")
    private CommonTrackFields commonTrackFields;

    /**
     * List of tracks in this catalog (REQUIRED)
     * Section 3.2.6
     */
    @JsonProperty("tracks")
    private List<CatalogTrack> tracks;

    /**
     * Custom field: Timestamp when catalog was generated (milliseconds since epoch)
     * NOTE: Custom fields should use reverse domain notation per spec
     */
    @JsonProperty("org.red5-generatedAt")
    private Long generatedAt;

    public Catalog() {
        this.tracks = new ArrayList<>();
        this.generatedAt = System.currentTimeMillis();
    }

    public Catalog(List<CatalogTrack> tracks) {
        this.tracks = tracks != null ? tracks : new ArrayList<>();
        this.generatedAt = System.currentTimeMillis();
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getGeneratedAt() {
        return generatedAt;
    }

    public void setGeneratedAt(long generatedAt) {
        this.generatedAt = generatedAt;
    }

    public List<CatalogTrack> getTracks() {
        return tracks;
    }

    public void setTracks(List<CatalogTrack> tracks) {
        this.tracks = tracks;
    }

    public Integer getStreamingFormat() {
        return streamingFormat;
    }

    public void setStreamingFormat(Integer streamingFormat) {
        this.streamingFormat = streamingFormat;
    }

    public String getStreamingFormatVersion() {
        return streamingFormatVersion;
    }

    public void setStreamingFormatVersion(String streamingFormatVersion) {
        this.streamingFormatVersion = streamingFormatVersion;
    }

    public Boolean getSupportsDeltaUpdates() {
        return supportsDeltaUpdates;
    }

    public void setSupportsDeltaUpdates(Boolean supportsDeltaUpdates) {
        this.supportsDeltaUpdates = supportsDeltaUpdates;
    }

    public CommonTrackFields getCommonTrackFields() {
        return commonTrackFields;
    }

    public void setCommonTrackFields(CommonTrackFields commonTrackFields) {
        this.commonTrackFields = commonTrackFields;
    }

    @JsonIgnore
    public ByteBuf getPayload() {
        try {
            String json = MAPPER.writeValueAsString(this);
            return Unpooled.wrappedBuffer(json.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize catalog", e);
        }
    }

    @Override
    public String toString() {
        return "Catalog{version=" + version + ", tracks=" + tracks.size() + ", generatedAt=" + generatedAt + "}";
    }

    /**
     * Get JSON representation of this catalog.
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }
}
