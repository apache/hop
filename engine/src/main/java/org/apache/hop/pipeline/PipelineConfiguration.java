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

package org.apache.hop.pipeline;

import org.apache.hop.server.HttpUtil;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.xml.XmlHandler;
import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;

public class PipelineConfiguration {
  public static final String XML_TAG = "pipeline_configuration";

  private PipelineMeta pipelineMeta;
  private PipelineExecutionConfiguration pipelineExecutionConfiguration;
  private SerializableMetadataProvider metadataProvider;

  /**
   * @param pipelineMeta
   * @param pipelineExecutionConfiguration
   */
  public PipelineConfiguration( PipelineMeta pipelineMeta, PipelineExecutionConfiguration pipelineExecutionConfiguration, SerializableMetadataProvider metadataProvider ) {
    this.pipelineMeta = pipelineMeta;
    this.pipelineExecutionConfiguration = pipelineExecutionConfiguration;
    this.metadataProvider = metadataProvider;
  }

  public String getXml() throws IOException, HopException, HopException {
    StringBuilder xml = new StringBuilder( 200 );

    xml.append( "<" + XML_TAG + ">" ).append( Const.CR );

    xml.append( pipelineMeta.getXml() );
    xml.append( pipelineExecutionConfiguration.getXml() );

    String jsonString = HttpUtil.encodeBase64ZippedString(metadataProvider.toJson());
    xml.append( XmlHandler.addTagValue( "metastore_json", jsonString));

    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );

    return xml.toString();
  }

  public PipelineConfiguration( Node configNode ) throws HopException, HopException, ParseException, IOException {
    Node trecNode = XmlHandler.getSubNode( configNode, PipelineExecutionConfiguration.XML_TAG );
    pipelineExecutionConfiguration = new PipelineExecutionConfiguration( trecNode );
    String metaStoreJson = HttpUtil.decodeBase64ZippedString(XmlHandler.getTagValue( configNode, "metastore_json" ));
    metadataProvider = new SerializableMetadataProvider(metaStoreJson);
    Node pipelineNode = XmlHandler.getSubNode( configNode, PipelineMeta.XML_TAG );
    pipelineMeta = new PipelineMeta( pipelineNode, metadataProvider );
  }

  public static final PipelineConfiguration fromXml(String xml ) throws HopException, HopException, ParseException, IOException {
    Document document = XmlHandler.loadXmlString( xml );
    Node configNode = XmlHandler.getSubNode( document, XML_TAG );
    return new PipelineConfiguration( configNode );
  }

  /**
   * @return the pipelineExecutionConfiguration
   */
  public PipelineExecutionConfiguration getPipelineExecutionConfiguration() {
    return pipelineExecutionConfiguration;
  }

  /**
   * @param pipelineExecutionConfiguration the pipelineExecutionConfiguration to set
   */
  public void setPipelineExecutionConfiguration( PipelineExecutionConfiguration pipelineExecutionConfiguration ) {
    this.pipelineExecutionConfiguration = pipelineExecutionConfiguration;
  }

  /**
   * @return the pipelineMeta
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta the pipelineMeta to set
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
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
