package org.red5.io.moq.model;

import org.red5.io.moq.catalog.Catalog;
import org.red5.io.moq.timeline.Timeline;

/**
 * Sealed interface representing different types of track events.
 */
public sealed interface TrackEvent permits
    TrackEvent.Keyframe,
    TrackEvent.Frame,
    TrackEvent.InitSegment,
    TrackEvent.TimelineEvent,
    TrackEvent.CatalogEvent,
    TrackEvent.TimeEvent {

    record Keyframe(FrameData data) implements TrackEvent {}
    record Frame(FrameData data) implements TrackEvent {}
    record InitSegment(FrameData data) implements TrackEvent {}
    record TimelineEvent(Timeline timeline) implements TrackEvent {}
    record CatalogEvent(Catalog catalog) implements TrackEvent {}
    record TimeEvent(TimeEventData data) implements TrackEvent {}
}
