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

package org.apache.hop.workflow;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;

public class WorkflowConfiguration {
  public static final String XML_TAG = "job_configuration";

  private WorkflowMeta workflowMeta;
  private WorkflowExecutionConfiguration workflowExecutionConfiguration;

  /**
   * @param workflowMeta
   * @param workflowExecutionConfiguration
   */
  public WorkflowConfiguration( WorkflowMeta workflowMeta, WorkflowExecutionConfiguration workflowExecutionConfiguration ) {
    this.workflowMeta = workflowMeta;
    this.workflowExecutionConfiguration = workflowExecutionConfiguration;
  }

  public String getXml() throws IOException {
    StringBuilder xml = new StringBuilder( 100 );

    xml.append( "<" + XML_TAG + ">" ).append( Const.CR );

    xml.append( workflowMeta.getXml() );
    xml.append( workflowExecutionConfiguration.getXml() );

    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );

    return xml.toString();
  }

  public WorkflowConfiguration( Node configNode ) throws HopException {
    Node workflowNode = XmlHandler.getSubNode( configNode, WorkflowMeta.XML_TAG );
    Node trecNode = XmlHandler.getSubNode( configNode, WorkflowExecutionConfiguration.XML_TAG );
    workflowExecutionConfiguration = new WorkflowExecutionConfiguration( trecNode );
    workflowMeta = new WorkflowMeta( workflowNode );
  }

  public static final WorkflowConfiguration fromXML( String xml ) throws HopException {
    Document document = XmlHandler.loadXMLString( xml );
    Node configNode = XmlHandler.getSubNode( document, XML_TAG );
    return new WorkflowConfiguration( configNode );
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
}
