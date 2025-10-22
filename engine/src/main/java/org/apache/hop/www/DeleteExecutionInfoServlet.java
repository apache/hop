/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

@HopServerServlet(id = "registerExecInfo", name = "Register execution information")
public class DeleteExecutionInfoServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final long serialVersionUID = -1901302231769020201L;

  public static final String CONTEXT_PATH = "/hop/deleteExecInfo";
  public static final String PARAMETER_ID = "id";
  public static final String PARAMETER_LOCATION = "location";

  public DeleteExecutionInfoServlet() {}

  public DeleteExecutionInfoServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (isJettyMode() && !request.getRequestURI().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug("Delete execution information");
    }

    // The ID of the execution to delete
    //
    String id = StringEscapeUtils.escapeHtml(request.getParameter(PARAMETER_ID));

    // The name of the location is also in a parameter
    //
    String locationName = StringEscapeUtils.escapeHtml(request.getParameter(PARAMETER_LOCATION));

    PrintWriter out = response.getWriter();
    BufferedReader in = request.getReader();

    response.setContentType("text/xml");
    out.print(XmlHandler.getXmlHeader());

    response.setStatus(HttpServletResponse.SC_OK);

    try {
      // validate the parameters
      //
      if (StringUtils.isEmpty(id)) {
        throw new HopException("Please specify the ID of the execution to delete.");
      }
      if (StringUtils.isEmpty(locationName)) {
        throw new HopException(
            "Please specify the name of the execution information location to delete in.");
      }

      // Look up the location in the metadata.
      //
      MultiMetadataProvider provider = pipelineMap.getHopServerConfig().getMetadataProvider();
      IHopMetadataSerializer<ExecutionInfoLocation> serializer =
          provider.getSerializer(ExecutionInfoLocation.class);
      ExecutionInfoLocation location = serializer.load(locationName);
      if (location == null) {
        throw new HopException("Unable to find execution information location " + locationName);
      }

      location.getExecutionInfoLocation().deleteExecution(id);

      // Return the log channel id as well
      //
      out.println(
          new WebResult(
              WebResult.STRING_OK,
              "Execution deletion was successful at location " + locationName));

    } catch (Exception ex) {

      out.println(new WebResult(WebResult.STRING_ERROR, Const.getStackTracker(ex)));
    }
  }

  public String toString() {
    return "Delete execution";
  }

  @Override
  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
