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
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.json.simple.parser.ParseException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@HopServerServlet(id="registerPipeline", name = "Add a pipeline to the server")
public class RegisterPipelineServlet extends BaseWorkflowServlet {

  private static final long serialVersionUID = 468054102740138751L;
  public static final String CONTEXT_PATH = "/hop/registerPipeline";

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override
  WebResult generateBody( HttpServletRequest request, HttpServletResponse response, boolean useXML, IVariables variables ) throws IOException, HopException, HopException, ParseException {

    final String xml = IOUtils.toString( request.getInputStream() );

    // Parse the XML, create a pipeline configuration
    PipelineConfiguration pipelineConfiguration = PipelineConfiguration.fromXml( xml);

    IPipelineEngine<PipelineMeta> pipeline = createPipeline( pipelineConfiguration );

    String message = "Pipeline '" + pipeline.getPipelineMeta().getName() + "' was added to HopServer with id " + pipeline.getContainerId();
    return new WebResult( WebResult.STRING_OK, message, pipeline.getContainerId() );
  }
}
