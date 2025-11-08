package org.red5.io.moq.model;

/**
 * Enum representing different types of media tracks.
 */
public enum TrackType {
    VIDEO("video"),
    AUDIO("audio"),
    TIMELINE("timeline"),
    CATALOG("catalog"),
    OTHER("other");

    private final String value;

    TrackType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public String getValue() {
        return value;
    }
}
