package org.red5.io.moq.model;

import io.netty.buffer.ByteBuf;

/**
 * Interface for track data that can be transmitted over MOQ.
 */
public interface TrackData {
    String getTrackName();
    TrackType getTrackType();
    ByteBuf getPayload();
}
