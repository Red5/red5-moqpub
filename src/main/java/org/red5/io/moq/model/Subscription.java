package org.red5.io.moq.model;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents an active subscription from a subscriber.
 * Tracks subscription state and manages object delivery.
 */
public class Subscription {
    private final long subscribeId;
    private final String trackNamespace;
    private final String trackName;
    private final long trackAlias;

    // Track delivery state
    private final AtomicLong lastGroupSent = new AtomicLong(-1);
    private final AtomicLong lastObjectSent = new AtomicLong(-1);

    // Subscription parameters
    private final long startGroup;
    private final long startObject;
    private final long endGroup;
    private final long endObject;

    public Subscription(long subscribeId, String trackNamespace, String trackName,
                       long trackAlias, long startGroup, long startObject,
                       long endGroup, long endObject) {
        this.subscribeId = subscribeId;
        this.trackNamespace = trackNamespace;
        this.trackName = trackName;
        this.trackAlias = trackAlias;
        this.startGroup = startGroup;
        this.startObject = startObject;
        this.endGroup = endGroup;
        this.endObject = endObject;
    }

    /**
     * Check if this subscription should receive the given group/object.
     */
    public boolean shouldSend(long groupId, long objectId) {
        // Check if within range
        if (groupId < startGroup) {
            return false;
        }

        if (endGroup != Long.MAX_VALUE && groupId > endGroup) {
            return false;
        }

        // For start group, check object range
        if (groupId == startGroup && objectId < startObject) {
            return false;
        }

        // For end group, check object range
        if (groupId == endGroup && objectId > endObject) {
            return false;
        }

        return true;
    }

    public void updateLastSent(long groupId, long objectId) {
        lastGroupSent.set(groupId);
        lastObjectSent.set(objectId);
    }

    // Getters
    public long getSubscribeId() { return subscribeId; }
    public String getTrackNamespace() { return trackNamespace; }
    public String getTrackName() { return trackName; }
    public long getTrackAlias() { return trackAlias; }
    public long getLastGroupSent() { return lastGroupSent.get(); }
    public long getLastObjectSent() { return lastObjectSent.get(); }

    @Override
    public String toString() {
        return String.format("Subscription{id=%d, namespace=%s, track=%s, alias=%d, range=[%d:%d]-[%d:%d]}",
            subscribeId, trackNamespace, trackName, trackAlias,
            startGroup, startObject, endGroup, endObject);
    }
}
