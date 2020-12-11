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
package org.apache.hop.www;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

@HopServerServlet(id="registerPackage", name = "Upload a resources export file")
public class RegisterPackageServlet extends BaseWorkflowServlet {

  public static final String CONTEXT_PATH = "/hop/registerPackage";

  private static final long serialVersionUID = -7582587179862317791L;

  public static final String PARAMETER_LOAD = "load";
  public static final String PARAMETER_TYPE = "type";
  public static final String TYPE_WORKFLOW = "workflow";
  public static final String TYPE_PIPELINE = "pipeline";

  private static final String ZIP_CONT = "zip:{0}!{1}";

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override
  WebResult generateBody( HttpServletRequest request, HttpServletResponse response, boolean useXML, IVariables variables ) throws HopException, IOException, HopException, ParseException {
    FileObject tempFile = HopVfs.createTempFile( "export", ".zip", System.getProperty( "java.io.tmpdir" ) );
    OutputStream out = HopVfs.getOutputStream( tempFile, false );
    IOUtils.copy( request.getInputStream(), out );
    out.flush();
    IOUtils.closeQuietly( out );

    String archiveUrl = tempFile.getName().toString();
    String load = request.getParameter( PARAMETER_LOAD ); // the resource to load

    if ( !Utils.isEmpty( load ) ) {
      String fileUrl = MessageFormat.format( ZIP_CONT, archiveUrl, load );
      boolean isWorkflow = TYPE_WORKFLOW.equalsIgnoreCase( request.getParameter( PARAMETER_TYPE ) );
      String resultId;

      String metaStoreJson = getMetaStoreJsonFromZIP( archiveUrl );
      SerializableMetadataProvider metadataProvider = new SerializableMetadataProvider( metaStoreJson );

      if ( isWorkflow ) {
        Node node = getConfigNodeFromZIP( archiveUrl, Workflow.CONFIGURATION_IN_EXPORT_FILENAME, WorkflowExecutionConfiguration.XML_TAG );
        WorkflowExecutionConfiguration workflowExecutionConfiguration = new WorkflowExecutionConfiguration( node );

        WorkflowMeta workflowMeta = new WorkflowMeta( fileUrl );
        WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration( workflowMeta, workflowExecutionConfiguration, metadataProvider );

        IWorkflowEngine<WorkflowMeta> workflow = createWorkflow( workflowConfiguration );
        resultId = workflow.getContainerId();
      } else {
        Node node = getConfigNodeFromZIP( archiveUrl, Pipeline.CONFIGURATION_IN_EXPORT_FILENAME, PipelineExecutionConfiguration.XML_TAG );
        PipelineExecutionConfiguration pipelineExecutionConfiguration = new PipelineExecutionConfiguration( node );

        PipelineMeta pipelineMeta = new PipelineMeta( fileUrl, metadataProvider, true, Variables.getADefaultVariableSpace() );

        PipelineConfiguration pipelineConfiguration = new PipelineConfiguration( pipelineMeta, pipelineExecutionConfiguration, metadataProvider );

        IPipelineEngine<PipelineMeta> pipeline = createPipeline( pipelineConfiguration );
        resultId = pipeline.getContainerId();
      }

      return new WebResult( WebResult.STRING_OK, fileUrl, resultId );
    }

    return null;
  }

  @Override
  protected boolean useXML( HttpServletRequest request ) {
    // always XML
    return true;
  }

  protected Node getConfigNodeFromZIP( Object archiveUrl, Object fileName, String xml_tag ) throws HopXmlException {
    String configUrl = MessageFormat.format( ZIP_CONT, archiveUrl, fileName );
    Document configDoc = XmlHandler.loadXmlFile( configUrl );
    return XmlHandler.getSubNode( configDoc, xml_tag );
  }

  public static final String getMetaStoreJsonFromZIP( Object archiveUrl ) throws HopFileException, IOException {
    String filename = MessageFormat.format( ZIP_CONT, archiveUrl, "metadata.json" );
    InputStream inputStream = HopVfs.getInputStream( filename );
    String metaStoreJson = IOUtils.toString( inputStream, StandardCharsets.UTF_8.name() );
    return metaStoreJson;
  }
}
