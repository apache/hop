package org.apache.hop.pipeline.engine;

public class PipelineEngineCapabilities {

  private boolean supportingPreview;
  private boolean supportingDebug;
  private boolean supportingSniffing;

  public PipelineEngineCapabilities() {
  }

  public PipelineEngineCapabilities( boolean supportingPreview, boolean supportingDebug, boolean supportingSniffing ) {
    this.supportingPreview = supportingPreview;
    this.supportingDebug = supportingDebug;
    this.supportingSniffing = supportingSniffing;
  }

  /**
   * Gets supportingPreview
   *
   * @return value of supportingPreview
   */
  public boolean isSupportingPreview() {
    return supportingPreview;
  }

  /**
   * @param supportingPreview The supportingPreview to set
   */
  public void setSupportingPreview( boolean supportingPreview ) {
    this.supportingPreview = supportingPreview;
  }

  /**
   * Gets supportingDebug
   *
   * @return value of supportingDebug
   */
  public boolean isSupportingDebug() {
    return supportingDebug;
  }

  /**
   * @param supportingDebug The supportingDebug to set
   */
  public void setSupportingDebug( boolean supportingDebug ) {
    this.supportingDebug = supportingDebug;
  }

  /**
   * Gets supportingSniffing
   *
   * @return value of supportingSniffing
   */
  public boolean isSupportingSniffing() {
    return supportingSniffing;
  }

  /**
   * @param supportingSniffing The supportingSniffing to set
   */
  public void setSupportingSniffing( boolean supportingSniffing ) {
    this.supportingSniffing = supportingSniffing;
  }
}
