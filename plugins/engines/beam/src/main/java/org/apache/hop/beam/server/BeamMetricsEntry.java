package org.apache.hop.beam.server;

import org.apache.hop.www.SlaveServerPipelineStatus;

import java.util.Date;

public class BeamMetricsEntry {
  /** the carte Object ID of the transformation, some unique number */
  private String containerObjectId;

  /** the name of the transformation */
  private String pipelineName;

  /** The internal Job ID (Spark, Flink, ...)*/
  private String internalJobId;

  /** The update date */
  private Date updateDate;

  /**
   * The transformation status.
   */
  private SlaveServerPipelineStatus pipelineStatus;

  public BeamMetricsEntry() {
  }

  public BeamMetricsEntry( String containerObjectId, String pipelineName, String internalJobId, Date updateDate, SlaveServerPipelineStatus pipelineStatus ) {
    this.containerObjectId = containerObjectId;
    this.pipelineName = pipelineName;
    this.internalJobId = internalJobId;
    this.updateDate = updateDate;
    this.pipelineStatus = pipelineStatus;
  }

  /**
   * Gets containerObjectId
   *
   * @return value of containerObjectId
   */
  public String getContainerObjectId() {
    return containerObjectId;
  }

  /**
   * @param containerObjectId The containerObjectId to set
   */
  public void setContainerObjectId( String containerObjectId ) {
    this.containerObjectId = containerObjectId;
  }

  /**
   * Get the pipeline name
   *
   * @return The name of the pipeline
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * @param pipelineName The name of the pipeline to set
   */
  public void setPipelineName( String pipelineName ) {
    this.pipelineName = pipelineName;
  }

  /**
   * Gets internalJobId
   *
   * @return value of internalJobId
   */
  public String getInternalJobId() {
    return internalJobId;
  }

  /**
   * @param internalJobId The internalJobId to set
   */
  public void setInternalJobId( String internalJobId ) {
    this.internalJobId = internalJobId;
  }

  /**
   * Gets pipelineStatus
   *
   * @return value of pipelineStatus
   */
  public SlaveServerPipelineStatus getPipelineStatus() {
    return pipelineStatus;
  }

  /**
   * @param pipelineStatus The pipelineStatus to set
   */
  public void setPipelineStatus( SlaveServerPipelineStatus pipelineStatus ) {
    this.pipelineStatus = pipelineStatus;
  }

  /**
   * Gets updateDate
   *
   * @return value of updateDate
   */
  public Date getUpdateDate() {
    return updateDate;
  }

  /**
   * @param updateDate The updateDate to set
   */
  public void setUpdateDate( Date updateDate ) {
    this.updateDate = updateDate;
  }
}
