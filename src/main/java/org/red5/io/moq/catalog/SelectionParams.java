package org.red5.io.moq.catalog;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Selection parameters for a track in MOQ catalog.
 * Contains codec-specific and quality-related parameters that help clients
 * choose appropriate tracks for playback.
 *
 * Per draft-ietf-moq-catalogformat-01 Section 3.2.17
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SelectionParams {

    /**
     * Codec string (e.g., "avc1.42E01E", "mp4a.40.2", "av01.0.08M.10.0.110.09")
     * Section 3.2.21
     */
    @JsonProperty("codec")
    private String codec;

    /**
     * MIME type (e.g., "video/mp4", "audio/mp4")
     * Section 3.2.22
     */
    @JsonProperty("mimeType")
    private String mimeType;

    /**
     * Frame rate in frames per second
     * Section 3.2.23
     */
    @JsonProperty("framerate")
    private Integer framerate;

    /**
     * Target bitrate in bits per second
     * Section 3.2.24
     */
    @JsonProperty("bitrate")
    private Integer bitrate;

    /**
     * Video width in pixels
     * Section 3.2.25
     */
    @JsonProperty("width")
    private Integer width;

    /**
     * Video height in pixels
     * Section 3.2.26
     */
    @JsonProperty("height")
    private Integer height;

    /**
     * Audio sample rate in Hz (e.g., 48000, 44100)
     * Section 3.2.27
     */
    @JsonProperty("samplerate")
    private Integer samplerate;

    /**
     * Audio channel configuration (e.g., "2", "5.1", "7.1")
     * Section 3.2.28
     */
    @JsonProperty("channelConfig")
    private String channelConfig;

    /**
     * Display width in pixels (may differ from encoded width due to aspect ratio)
     * Section 3.2.29
     */
    @JsonProperty("displayWidth")
    private Integer displayWidth;

    /**
     * Display height in pixels (may differ from encoded height due to aspect ratio)
     * Section 3.2.30
     */
    @JsonProperty("displayHeight")
    private Integer displayHeight;

    /**
     * Language tag (e.g., "en", "fr", "es")
     * Section 3.2.31
     */
    @JsonProperty("lang")
    private String lang;

    // Constructors

    public SelectionParams() {
    }

    // Getters and Setters

    public String getCodec() {
        return codec;
    }

    public void setCodec(String codec) {
        this.codec = codec;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public Integer getFramerate() {
        return framerate;
    }

    public void setFramerate(Integer framerate) {
        this.framerate = framerate;
    }

    public Integer getBitrate() {
        return bitrate;
    }

    public void setBitrate(Integer bitrate) {
        this.bitrate = bitrate;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public Integer getHeight() {
        return height;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

    public Integer getSamplerate() {
        return samplerate;
    }

    public void setSamplerate(Integer samplerate) {
        this.samplerate = samplerate;
    }

    public String getChannelConfig() {
        return channelConfig;
    }

    public void setChannelConfig(String channelConfig) {
        this.channelConfig = channelConfig;
    }

    public Integer getDisplayWidth() {
        return displayWidth;
    }

    public void setDisplayWidth(Integer displayWidth) {
        this.displayWidth = displayWidth;
    }

    public Integer getDisplayHeight() {
        return displayHeight;
    }

    public void setDisplayHeight(Integer displayHeight) {
        this.displayHeight = displayHeight;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }
}
