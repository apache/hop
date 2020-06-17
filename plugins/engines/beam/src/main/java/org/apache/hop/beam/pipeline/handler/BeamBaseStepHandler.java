package org.apache.hop.beam.pipeline.handler;

import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;

import java.util.List;

public class BeamBaseStepHandler {

  protected IHopMetadataProvider metadataProvider;
  protected PipelineMeta pipelineMeta;
  protected List<String> transformPluginClasses;
  protected List<String> xpPluginClasses;
  protected boolean input;
  protected boolean output;
  protected IBeamPipelineEngineRunConfiguration runConfiguration;

  public BeamBaseStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, boolean input, boolean output, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.runConfiguration = runConfiguration;
    this.input = input;
    this.output = output;
    this.metadataProvider = metadataProvider;
    this.pipelineMeta = pipelineMeta;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * @param metadataProvider The metadataProvider to set
   */
  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }

  /**
   * Gets pipelineMeta
   *
   * @return value of pipelineMeta
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta The pipelineMeta to set
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * Gets transformPluginClasses
   *
   * @return value of transformPluginClasses
   */
  public List<String> getStepPluginClasses() {
    return transformPluginClasses;
  }

  /**
   * @param transformPluginClasses The transformPluginClasses to set
   */
  public void setStepPluginClasses( List<String> transformPluginClasses ) {
    this.transformPluginClasses = transformPluginClasses;
  }

  /**
   * Gets xpPluginClasses
   *
   * @return value of xpPluginClasses
   */
  public List<String> getXpPluginClasses() {
    return xpPluginClasses;
  }

  /**
   * @param xpPluginClasses The xpPluginClasses to set
   */
  public void setXpPluginClasses( List<String> xpPluginClasses ) {
    this.xpPluginClasses = xpPluginClasses;
  }

  /**
   * Gets input
   *
   * @return value of input
   */
  public boolean isInput() {
    return input;
  }

  /**
   * @param input The input to set
   */
  public void setInput( boolean input ) {
    this.input = input;
  }

  /**
   * Gets output
   *
   * @return value of output
   */
  public boolean isOutput() {
    return output;
  }

  /**
   * @param output The output to set
   */
  public void setOutput( boolean output ) {
    this.output = output;
  }
}
