package org.red5.io.moq.catalog;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum representing the packaging type for MOQ tracks.
 * Per draft-ietf-moq-catalogformat-01 Section 3.2.10, Table 3
 * and draft-ietf-moq-warp-01 Section 5.2.10
 *
 * Only two packaging formats are currently defined:
 * - "loc": Low Overhead Container (see draft-ietf-moq-loc-01)
 * - "cmaf": Common Media Application Format
 */
public enum PackagingType {
    /**
     * Low Overhead Container format (draft-ietf-moq-loc-01)
     * Uses elementary bitstreams with minimal overhead
     */
    LOC("loc"),

    /**
     * Common Media Application Format
     * ISO BMFF-based format for HTTP streaming
     */
    CMAF("cmaf");

    private final String value;

    PackagingType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    /**
     * Parse packaging type from string value
     */
    public static PackagingType fromValue(String value) {
        for (PackagingType type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown packaging type: " + value);
    }
}
