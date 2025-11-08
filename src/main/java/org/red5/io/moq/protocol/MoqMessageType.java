package org.red5.io.moq.protocol;

/**
 * MOQ Transport message types as defined in draft-ietf-moq-transport-14.
 *
 * <p><strong>CRITICAL DRAFT-14 REQUIREMENTS:</strong></p>
 * <ul>
 *   <li>Publishers MUST use SUBGROUP_HEADER (0x10-0x1D) for unidirectional data streams</li>
 *   <li>Publishers MUST NOT use STREAM_HEADER_TRACK (0x50) or STREAM_HEADER_GROUP (0x51)</li>
 *   <li>These legacy headers (0x50, 0x51) are NOT part of draft-14 specification</li>
 *   <li>Servers WILL terminate connections with PROTOCOL_VIOLATION if legacy headers are used</li>
 * </ul>
 *
 * <p><strong>Draft-14 Unidirectional Stream Types (Section 10):</strong></p>
 * <ul>
 *   <li>0x10-0x1D: SUBGROUP_HEADER (Section 10.4.2) - REQUIRED for object delivery</li>
 *   <li>0x05: FETCH_HEADER (Section 10.4.4) - For on-demand fetch responses</li>
 * </ul>
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/draft-ietf-moq-transport-14">MOQ Transport Draft 14</a>
 */
public enum MoqMessageType {

    // Setup Messages (Section 6.2)
    CLIENT_SETUP(0x20, "CLIENT_SETUP"),
    SERVER_SETUP(0x21, "SERVER_SETUP"),

    // Subscription Messages (Section 6.3)
    SUBSCRIBE(0x03, "SUBSCRIBE"),
    SUBSCRIBE_OK(0x04, "SUBSCRIBE_OK"),
    SUBSCRIBE_ERROR(0x05, "SUBSCRIBE_ERROR"),
    UNSUBSCRIBE(0x0A, "UNSUBSCRIBE"),

    // Publishing Messages (Section 6.4)
    PUBLISH(0x1D, "PUBLISH"),
    PUBLISH_OK(0x1E, "PUBLISH_OK"),
    PUBLISH_ERROR(0x1F, "PUBLISH_ERROR"),
    UNPUBLISH(0x0C, "UNPUBLISH"),

    // Namespace Discovery (Section 6.5)
    PUBLISH_NAMESPACE(0x06, "PUBLISH_NAMESPACE"),
    PUBLISH_NAMESPACE_OK(0x07, "PUBLISH_NAMESPACE_OK"),
    PUBLISH_NAMESPACE_ERROR(0x08, "PUBLISH_NAMESPACE_ERROR"),
    UNPUBLISH_NAMESPACE(0x09, "UNPUBLISH_NAMESPACE"),

    SUBSCRIBE_NAMESPACE(0x11, "SUBSCRIBE_NAMESPACE"),
    SUBSCRIBE_NAMESPACE_OK(0x12, "SUBSCRIBE_NAMESPACE_OK"),
    SUBSCRIBE_NAMESPACE_ERROR(0x13, "SUBSCRIBE_NAMESPACE_ERROR"),
    UNSUBSCRIBE_NAMESPACE(0x14, "UNSUBSCRIBE_NAMESPACE"),
    SUBSCRIBE_NAMESPACE_DONE(0x15, "SUBSCRIBE_NAMESPACE_DONE"),

    // Fetch Messages (Section 6.6)
    FETCH(0x16, "FETCH"),
    FETCH_CANCEL(0x17, "FETCH_CANCEL"),
    FETCH_OK(0x18, "FETCH_OK"),
    FETCH_ERROR(0x19, "FETCH_ERROR"),

    // Stream Management (Section 6.7)
    SUBSCRIBE_DONE(0x0B, "SUBSCRIBE_DONE"),
    // Note: SUBSCRIBE_ANNOUNCES (0x0F) and SUBSCRIBE_ANNOUNCES_OK (0x10) were renamed to
    // SUBSCRIBE_NAMESPACE in draft-14. These constants are removed to avoid conflicts.

    // Track Status (Section 6.8)
    TRACK_STATUS_REQUEST(0x0D, "TRACK_STATUS_REQUEST"),
    TRACK_STATUS(0x0E, "TRACK_STATUS"),

    // Connection Management (Section 6.9)
    GOAWAY(0x10, "GOAWAY"),  // Draft-14: 0x10 (NOT 0x1A!)

    // Object Stream Messages (Section 7)
    OBJECT_STREAM(0x00, "OBJECT_STREAM"),
    OBJECT_DATAGRAM(0x01, "OBJECT_DATAGRAM"),

    // SUBGROUP_HEADER Stream Types (Section 10.4.2) - for unidirectional streams
    // *** DRAFT-14 REQUIREMENT: MUST use these for data streams (NOT 0x50/0x51!) ***
    // Draft-14 defines 0x10-0x1D as SUBGROUP_HEADER variants
    // Regular subgroups (no end-of-group marker)
    SUBGROUP_HEADER(0x10, "SUBGROUP_HEADER"),  // Subgroup, no extensions
    SUBGROUP_HEADER_WITH_PRIORITY(0x11, "SUBGROUP_HEADER_WITH_PRIORITY"),  // With priority extensions
    // Additional types 0x12-0x15 for various extension combinations
    // Subgroups with end-of-group marker (0x18-0x1D)

    // FETCH_HEADER (Section 10.4.4)
    FETCH_HEADER(0x05, "FETCH_HEADER");

    // *** WARNING: STREAM_HEADER_TRACK (0x50) and STREAM_HEADER_GROUP (0x51) ***
    // These are NOT part of draft-14! They were relay-specific extensions.
    // Using them will cause PROTOCOL_VIOLATION errors and connection termination.
    // Use SUBGROUP_HEADER (0x10-0x1D) instead.

    private final int value;
    private final String name;

    // Setup Parameters (Section 6.2.1)
    public static final int PARAM_ROLE = 0x00;
    public static final int PARAM_PATH = 0x01;
    public static final int PARAM_MAX_SUBSCRIBE_ID = 0x02;
    public static final int PARAM_AUTHORIZATION_TOKEN = 0x03;

    // Role Values
    public static final int ROLE_PUBLISHER = 0x01;
    public static final int ROLE_SUBSCRIBER = 0x02;
    public static final int ROLE_PUB_SUB = 0x03;

    // Version (using long to avoid signed int issues with 0xff prefix)
    // Format: 0xff0000XX where XX is the draft number in hex
    public static final long VERSION_DRAFT_14 = 0xff00000eL;

    // Previous drafts (NOT SUPPORTED - different message types and formats)
    // Draft 10-13 used CLIENT_SETUP=0x40, SERVER_SETUP=0x41 (vs 0x20/0x21 in draft-14)
    // public static final long VERSION_DRAFT_13 = 0xff00000dL;
    // public static final long VERSION_DRAFT_12 = 0xff00000cL;
    // public static final long VERSION_DRAFT_11 = 0xff00000bL;
    // public static final long VERSION_DRAFT_10 = 0xff00000aL;

    // Current version (this implementation ONLY supports draft-14)
    // WHEN DRAFT-14 IS NEGOTIATED:
    //   - Publishers MUST use SUBGROUP_HEADER (0x10-0x1D) for data streams
    //   - Publishers MUST NOT use STREAM_HEADER_TRACK (0x50) or STREAM_HEADER_GROUP (0x51)
    //   - Using legacy headers will result in PROTOCOL_VIOLATION and connection termination
    public static final long VERSION_CURRENT = VERSION_DRAFT_14;

    // Track Status Codes
    public static final int TRACK_STATUS_IN_PROGRESS = 0x00;
    public static final int TRACK_STATUS_NOT_EXIST = 0x01;
    public static final int TRACK_STATUS_NOT_STARTED = 0x02;
    public static final int TRACK_STATUS_FINISHED = 0x03;

    // Error Codes (Section 8)
    public static final int ERROR_NO_ERROR = 0x00;
    public static final int ERROR_INTERNAL = 0x01;
    public static final int ERROR_UNAUTHORIZED = 0x02;
    public static final int ERROR_PROTOCOL_VIOLATION = 0x03;
    public static final int ERROR_DUPLICATE_TRACK_ALIAS = 0x04;
    public static final int ERROR_PARAMETER_LENGTH_MISMATCH = 0x05;
    public static final int ERROR_TOO_MANY_SUBSCRIBES = 0x06;
    public static final int ERROR_GOAWAY_TIMEOUT = 0x10;

    MoqMessageType(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public String getTypeName() {
        return name;
    }

    /**
     * Get supported versions in order of preference (newest first).
     * Used in CLIENT_SETUP version negotiation.
     *
     * NOTE: This implementation ONLY supports draft-14 because message types
     * and formats changed significantly from earlier drafts:
     * - Draft 14: CLIENT_SETUP=0x20, SERVER_SETUP=0x21
     * - Draft 10-13: CLIENT_SETUP=0x40, SERVER_SETUP=0x41
     *
     * Supporting multiple drafts would require separate message codecs.
     */
    public static long[] getSupportedVersions() {
        return new long[] {
            VERSION_DRAFT_14
        };
    }

    /**
     * Looks up a message type by its integer value.
     *
     * @param value the message type value
     * @return the corresponding MoqMessageType, or null if not found
     */
    public static MoqMessageType fromValue(int value) {
        for (MoqMessageType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        return null;
    }

    /**
     * Checks if a message type value is recognized.
     *
     * @param value the message type value
     * @return true if recognized, false otherwise
     */
    public static boolean isRecognized(int value) {
        return fromValue(value) != null;
    }

    /**
     * Get human-readable name for message type value.
     */
    public static String getMessageTypeName(int messageType) {
        MoqMessageType type = fromValue(messageType);
        return type != null ? type.name : "UNKNOWN(" + messageType + ")";
    }

    @Override
    public String toString() {
        return String.format("%s(0x%02X)", name, value);
    }
}
