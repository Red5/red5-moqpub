package org.red5.io.moq.catalog;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Represents a single track in the catalog.
 * Per draft-ietf-moq-catalogformat-01 Section 3.2.7
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CatalogTrack {

    /**
     * Track namespace (OPTIONAL if defined in commonTrackFields)
     * Section 3.2.8
     */
    @JsonProperty("namespace")
    private String namespace;

    /**
     * Track name (REQUIRED)
     * Section 3.2.9
     */
    @JsonProperty("name")
    private String name;

    /**
     * Packaging format: "loc" or "cmaf" (REQUIRED if not in commonTrackFields)
     * Section 3.2.10
     */
    @JsonProperty("packaging")
    private String packaging;

    /**
     * Human-readable label (OPTIONAL)
     * Section 3.2.13
     */
    @JsonProperty("label")
    private String label;

    /**
     * Render group ID for synchronization (OPTIONAL)
     * Section 3.2.12
     */
    @JsonProperty("renderGroup")
    private Integer renderGroup;

    /**
     * Alternative group ID for mutually exclusive tracks (OPTIONAL)
     * Section 3.2.14
     */
    @JsonProperty("altGroup")
    private Integer altGroup;

    /**
     * Base64-encoded initialization data (OPTIONAL)
     * Section 3.2.15
     */
    @JsonProperty("initData")
    private String initData;

    /**
     * Reference to another track's initialization data (OPTIONAL)
     * Section 3.2.16
     */
    @JsonProperty("initTrack")
    private String initTrack;

    /**
     * Selection parameters for this track (OPTIONAL)
     * Section 3.2.17
     */
    @JsonProperty("selectionParams")
    private SelectionParams selectionParams;

    /**
     * List of track names this track depends on (OPTIONAL)
     * Section 3.2.18
     */
    @JsonProperty("depends")
    private List<String> depends;

    /**
     * Temporal layer ID (OPTIONAL)
     * Section 3.2.19
     */
    @JsonProperty("temporalId")
    private Integer temporalId;

    /**
     * Spatial layer ID (OPTIONAL)
     * Section 3.2.20
     */
    @JsonProperty("spatialId")
    private Integer spatialId;

    // Constructors

    public CatalogTrack() {
    }

    public CatalogTrack(String name) {
        this.name = name;
    }

    // Getters and Setters

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPackaging() {
        return packaging;
    }

    public void setPackaging(String packaging) {
        this.packaging = packaging;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Integer getRenderGroup() {
        return renderGroup;
    }

    public void setRenderGroup(Integer renderGroup) {
        this.renderGroup = renderGroup;
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
