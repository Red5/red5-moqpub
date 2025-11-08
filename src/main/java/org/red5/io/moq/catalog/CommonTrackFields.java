package org.red5.io.moq.catalog;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Common track fields that can be shared across multiple tracks in a MOQ catalog.
 * Fields defined here apply to all tracks unless overridden at the track level.
 *
 * Per draft-ietf-moq-catalogformat-01 Section 3.2.4
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CommonTrackFields {

    /**
     * Track namespace (tuple of strings)
     * Section 3.2.8
     */
    @JsonProperty("namespace")
    private String namespace;

    /**
     * Packaging format: "loc" or "cmaf"
     * Section 3.2.10
     */
    @JsonProperty("packaging")
    private String packaging;

    /**
     * Render group ID for synchronization
     * Section 3.2.12
     */
    @JsonProperty("renderGroup")
    private Integer renderGroup;

    /**
     * Human-readable label for the track
     * Section 3.2.13
     */
    @JsonProperty("label")
    private String label;

    /**
     * Alternative group ID for selecting between mutually exclusive tracks
     * Section 3.2.14
     */
    @JsonProperty("altGroup")
    private Integer altGroup;

    /**
     * Base64-encoded initialization data for the track
     * Section 3.2.15
     */
    @JsonProperty("initData")
    private String initData;

    /**
     * Reference to another track's initialization data
     * Section 3.2.16
     */
    @JsonProperty("initTrack")
    private String initTrack;

    /**
     * Common selection parameters shared by tracks
     * Section 3.2.17
     */
    @JsonProperty("selectionParams")
    private SelectionParams selectionParams;

    /**
     * List of track names this track depends on
     * Section 3.2.18
     */
    @JsonProperty("depends")
    private List<String> depends;

    /**
     * Temporal layer ID
     * Section 3.2.19
     */
    @JsonProperty("temporalId")
    private Integer temporalId;

    /**
     * Spatial layer ID
     * Section 3.2.20
     */
    @JsonProperty("spatialId")
    private Integer spatialId;

    // Constructors

    public CommonTrackFields() {
    }

    // Getters and Setters

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getPackaging() {
        return packaging;
    }

    public void setPackaging(String packaging) {
        this.packaging = packaging;
    }

    public Integer getRenderGroup() {
        return renderGroup;
    }

    public void setRenderGroup(Integer renderGroup) {
        this.renderGroup = renderGroup;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Integer getAltGroup() {
        return altGroup;
    }

    public void setAltGroup(Integer altGroup) {
        this.altGroup = altGroup;
    }

    public String getInitData() {
        return initData;
    }

    public void setInitData(String initData) {
        this.initData = initData;
    }

    public String getInitTrack() {
        return initTrack;
    }

    public void setInitTrack(String initTrack) {
        this.initTrack = initTrack;
    }

    public SelectionParams getSelectionParams() {
        return selectionParams;
    }

    public void setSelectionParams(SelectionParams selectionParams) {
        this.selectionParams = selectionParams;
    }

    public List<String> getDepends() {
        return depends;
    }

    public void setDepends(List<String> depends) {
        this.depends = depends;
    }

    public Integer getTemporalId() {
        return temporalId;
    }

    public void setTemporalId(Integer temporalId) {
        this.temporalId = temporalId;
    }

    public Integer getSpatialId() {
        return spatialId;
    }

    public void setSpatialId(Integer spatialId) {
        this.spatialId = spatialId;
    }
}
