/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline;

import org.apache.hop.cluster.HttpUtil;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metastore.SerializableMetaStore;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;

public class PipelineConfiguration {
  public static final String XML_TAG = "pipeline_configuration";

  private PipelineMeta pipelineMeta;
  private PipelineExecutionConfiguration pipelineExecutionConfiguration;
  private SerializableMetaStore metaStore;

  /**
   * @param pipelineMeta
   * @param pipelineExecutionConfiguration
   */
  public PipelineConfiguration( PipelineMeta pipelineMeta, PipelineExecutionConfiguration pipelineExecutionConfiguration, SerializableMetaStore metaStore ) {
    this.pipelineMeta = pipelineMeta;
    this.pipelineExecutionConfiguration = pipelineExecutionConfiguration;
    this.metaStore = metaStore;
  }

  public String getXml() throws IOException, HopException, MetaStoreException {
    StringBuilder xml = new StringBuilder( 200 );

    xml.append( "<" + XML_TAG + ">" ).append( Const.CR );

    xml.append( pipelineMeta.getXml() );
    xml.append( pipelineExecutionConfiguration.getXml() );

    String jsonString = HttpUtil.encodeBase64ZippedString(metaStore.toJson());
    xml.append( XmlHandler.addTagValue( "metastore_json", jsonString));

    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );

    return xml.toString();
  }

  public PipelineConfiguration( Node configNode ) throws HopException, MetaStoreException, ParseException, IOException {
    Node trecNode = XmlHandler.getSubNode( configNode, PipelineExecutionConfiguration.XML_TAG );
    pipelineExecutionConfiguration = new PipelineExecutionConfiguration( trecNode );
    String metaStoreJson = HttpUtil.decodeBase64ZippedString(XmlHandler.getTagValue( configNode, "metastore_json" ));
    metaStore = new SerializableMetaStore(metaStoreJson);
    Node pipelineNode = XmlHandler.getSubNode( configNode, PipelineMeta.XML_TAG );
    pipelineMeta = new PipelineMeta( pipelineNode, metaStore );
  }

  public static final PipelineConfiguration fromXml(String xml ) throws HopException, MetaStoreException, ParseException, IOException {
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
   * Gets metaStore
   *
   * @return value of metaStore
   */
  public SerializableMetaStore getMetaStore() {
    return metaStore;
  }
}
