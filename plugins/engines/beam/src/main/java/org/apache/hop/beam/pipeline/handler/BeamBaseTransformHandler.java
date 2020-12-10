/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.pipeline.handler;

import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

public class BeamBaseTransformHandler {

  protected IVariables variables;
  protected IHopMetadataProvider metadataProvider;
  protected PipelineMeta pipelineMeta;
  protected List<String> transformPluginClasses;
  protected List<String> xpPluginClasses;
  protected boolean input;
  protected boolean output;
  protected IBeamPipelineEngineRunConfiguration runConfiguration;

  public BeamBaseTransformHandler(
      IVariables variables,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      boolean input,
      boolean output,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      List<String> transformPluginClasses,
      List<String> xpPluginClasses) {
    this.variables = variables;
    this.runConfiguration = runConfiguration;
    this.input = input;
    this.output = output;
    this.metadataProvider = metadataProvider;
    this.pipelineMeta = pipelineMeta;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  protected Node getTransformXmlNode(TransformMeta transformMeta) throws HopException {
    String xml = transformMeta.getXml();
    Node transformNode =
        XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), TransformMeta.XML_TAG);
    return transformNode;
  }

  protected void loadTransformMetadata(
      ITransformMeta meta,
      TransformMeta transformMeta,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta)
      throws HopException {
    meta.loadXml(getTransformXmlNode(transformMeta), metadataProvider);
    meta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /** @param metadataProvider The metadataProvider to set */
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
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

  /** @param pipelineMeta The pipelineMeta to set */
  public void setPipelineMeta(PipelineMeta pipelineMeta) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * Gets transformPluginClasses
   *
   * @return value of transformPluginClasses
   */
  public List<String> getTransformPluginClasses() {
    return transformPluginClasses;
  }

  /** @param transformPluginClasses The transformPluginClasses to set */
  public void setTransformPluginClasses(List<String> transformPluginClasses) {
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

  /** @param xpPluginClasses The xpPluginClasses to set */
  public void setXpPluginClasses(List<String> xpPluginClasses) {
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

  /** @param input The input to set */
  public void setInput(boolean input) {
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

  /** @param output The output to set */
  public void setOutput(boolean output) {
    this.output = output;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables( IVariables variables ) {
    this.variables = variables;
  }

  /**
   * Gets runConfiguration
   *
   * @return value of runConfiguration
   */
  public IBeamPipelineEngineRunConfiguration getRunConfiguration() {
    return runConfiguration;
  }

  /**
   * @param runConfiguration The runConfiguration to set
   */
  public void setRunConfiguration( IBeamPipelineEngineRunConfiguration runConfiguration ) {
    this.runConfiguration = runConfiguration;
  }
}
