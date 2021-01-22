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

package org.apache.hop.workflow;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.server.HttpUtil;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;

public class WorkflowConfiguration {
  public static final String XML_TAG = "workflow_configuration";

  private WorkflowMeta workflowMeta;
  private WorkflowExecutionConfiguration workflowExecutionConfiguration;
  private SerializableMetadataProvider metadataProvider;

  /**
   * @param workflowMeta
   * @param workflowExecutionConfiguration
   */
  public WorkflowConfiguration( WorkflowMeta workflowMeta, WorkflowExecutionConfiguration workflowExecutionConfiguration, IHopMetadataProvider metadataProviderToEncode ) throws HopException {
    this.workflowMeta = workflowMeta;
    this.workflowExecutionConfiguration = workflowExecutionConfiguration;
    this.metadataProvider = new SerializableMetadataProvider(metadataProviderToEncode);
  }

  public String getXml() throws IOException, HopException {
    StringBuilder xml = new StringBuilder();

    xml.append( "<" + XML_TAG + ">" ).append( Const.CR );

    xml.append( workflowMeta.getXml() );
    xml.append( workflowExecutionConfiguration.getXml() );
    xml.append( XmlHandler.addTagValue( "metastore_json", HttpUtil.encodeBase64ZippedString( metadataProvider.toJson() ) ) );
    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );

    return xml.toString();
  }

  public WorkflowConfiguration( Node configNode, IVariables variables ) throws HopException, ParseException, IOException {
    Node workflowNode = XmlHandler.getSubNode( configNode, WorkflowMeta.XML_TAG );
    Node trecNode = XmlHandler.getSubNode( configNode, WorkflowExecutionConfiguration.XML_TAG );
    workflowExecutionConfiguration = new WorkflowExecutionConfiguration( trecNode );
    String metaStoreJson = HttpUtil.decodeBase64ZippedString(XmlHandler.getTagValue( configNode, "metastore_json" ));
    metadataProvider = new SerializableMetadataProvider( metaStoreJson );
    workflowMeta = new WorkflowMeta( workflowNode, metadataProvider, variables );
  }

  public static final WorkflowConfiguration fromXml( String xml, IVariables variables ) throws HopException, ParseException, IOException {
    Document document = XmlHandler.loadXmlString( xml );
    Node configNode = XmlHandler.getSubNode( document, XML_TAG );
    return new WorkflowConfiguration( configNode, variables);
  }

  /**
   * @return the workflowExecutionConfiguration
   */
  public WorkflowExecutionConfiguration getWorkflowExecutionConfiguration() {
    return workflowExecutionConfiguration;
  }

  /**
   * @param workflowExecutionConfiguration the workflowExecutionConfiguration to set
   */
  public void setWorkflowExecutionConfiguration( WorkflowExecutionConfiguration workflowExecutionConfiguration ) {
    this.workflowExecutionConfiguration = workflowExecutionConfiguration;
  }

  /**
   * @return the workflow metadata
   */
  public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  /**
   * @param workflowMeta the workflow meta data to set
   */
  public void setWorkflowMeta( WorkflowMeta workflowMeta ) {
    this.workflowMeta = workflowMeta;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public SerializableMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }
}
