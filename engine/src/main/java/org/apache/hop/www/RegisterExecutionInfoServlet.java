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

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serial;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionInfoLocations;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

@HopServerServlet(id = "registerExecInfo", name = "Register execution information")
public class RegisterExecutionInfoServlet extends BaseHttpServlet implements IHopServerPlugin {
  @Serial private static final long serialVersionUID = -2817136625869923847L;

  public static final String CONTEXT_PATH = "/hop/registerExecInfo";
  public static final String TYPE_EXECUTION = "execution";
  public static final String TYPE_STATE = "state";
  public static final String TYPE_DATA = "data";
  public static final String PARAMETER_TYPE = "type";
  public static final String PARAMETER_LOCATION = "location";

  public RegisterExecutionInfoServlet() {}

  public RegisterExecutionInfoServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (isJettyMode() && !request.getRequestURI().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug("Register execution information");
    }

    // We receive some JSON as payload
    // The type parameter shows what type of information we get
    //
    String type = request.getParameter(PARAMETER_TYPE);

    // The name of the location is also in a parameter
    //
    String locationName = StringEscapeUtils.escapeHtml4(request.getParameter(PARAMETER_LOCATION));

    PrintWriter out = getSafeWriter(response);
    if (out == null) {
      return;
    }
    BufferedReader in = getSafeReader(request, response);
    if (in == null) {
      return;
    }

    response.setContentType("text/xml");
    out.print(XmlHandler.getXmlHeader());

    response.setStatus(HttpServletResponse.SC_OK);

    try {
      // validate the parameters
      //
      if (StringUtils.isEmpty(type)) {
        throw new HopException("Please specify the type of execution information to register.");
      }
      if (StringUtils.isEmpty(locationName)) {
        throw new HopException(
            "Please specify the name of the execution information location to register at.");
      }

      // Look up the location in the metadata.
      // No caching of the location state is done.
      // initialize() is always followed by a close().
      //
      MultiMetadataProvider provider = pipelineMap.getHopServerConfig().getMetadataProvider();

      // First read the complete JSON document in memory from the request.
      // This doesn't need the location, so we do it before opening one.
      //
      StringBuilder json = new StringBuilder(request.getContentLength());
      int c;
      while ((c = in.read()) != -1) {
        json.append((char) c);
      }
      String jsonString = json.toString();

      ExecutionInfoLocations.withLocation(
          locationName,
          variables,
          provider,
          log,
          iLocation -> {
            // What type of information are we receiving?
            //
            switch (type) {
              case TYPE_EXECUTION:
                iLocation.registerExecution(parseJson(jsonString, Execution.class));
                break;
              case TYPE_STATE:
                iLocation.updateExecutionState(parseJson(jsonString, ExecutionState.class));
                break;
              case TYPE_DATA:
                iLocation.registerData(parseJson(jsonString, ExecutionData.class));
                break;
              default:
                throw new HopException(
                    "Unknown update type: "
                        + type
                        + " allowed are: "
                        + TYPE_EXECUTION
                        + ", "
                        + TYPE_STATE
                        + ", "
                        + TYPE_DATA);
            }

            // Log successful registration of execution, state or data
            //
            out.println(
                new WebResult(
                    WebResult.STRING_OK, "Registration successful at location " + locationName));
            return null;
          });
    } catch (Exception ex) {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      out.println(new WebResult(WebResult.STRING_ERROR, Const.getStackTracker(ex)));
    }
  }

  private static <T> T parseJson(String json, Class<T> type) throws HopException {
    try {
      return HopJson.newMapper().readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new HopException("Error parsing " + type.getSimpleName() + " from the request", e);
    }
  }

  public String toString() {
    return "Register execution information";
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
