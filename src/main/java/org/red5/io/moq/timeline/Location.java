package org.red5.io.moq.timeline;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a location in the MOQ stream (group and object IDs).
 */
public class Location {
    @JsonProperty("group")
    private long group;

    @JsonProperty("object")
    private long object;

    public Location() {}

    public Location(long group, long object) {
        this.group = group;
        this.object = object;
    }

    public long getGroup() {
        return group;
    }

    public void setGroup(long group) {
        this.group = group;
    }

    public long getObject() {
        return object;
    }

    public void setObject(long object) {
        this.object = object;
    }

    @Override
    public String toString() {
        return "Location{" +
                "group=" + group +
                ", object=" + object +
                '}';
    }
}
