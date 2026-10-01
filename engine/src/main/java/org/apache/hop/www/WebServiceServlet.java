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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.www.service.PreparedWebService;
import org.apache.hop.www.service.ServletWebServiceOutput;
import org.apache.hop.www.service.WebServiceExecutor;
import org.apache.hop.www.service.WebServiceRequest;

@HopServerServlet(id = "webService", name = "Output the content of a field in a transform")
public class WebServiceServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final Class<?> PKG = WebServiceServlet.class;
  @Serial private static final long serialVersionUID = 3634806745373343432L;

  public static final String CONTEXT_PATH = "/hop/webService";

  public WebServiceServlet() {}

  public WebServiceServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      doGet(request, response);
    } catch (Exception e) {
      logError("Error handling web service POST request", e);
      sendSafeError(
          response,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Unable to process web service request.");
    }
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "WebServiceServlet.Log.WebServiceRequested"));
    }

    IVariables variables = pipelineMap.getHopServerConfig().getVariables();
    IHopMetadataProvider metadataProvider = pipelineMap.getHopServerConfig().getMetadataProvider();

    String webServiceName = request.getParameter(WebServiceExecutor.PARAMETER_SERVICE);
    if (StringUtils.isEmpty(webServiceName)) {
      sendSafeError(
          response,
          HttpServletResponse.SC_BAD_REQUEST,
          "Please specify a service parameter pointing to the name of the web service object");
      return;
    }

    try {
      WebServiceRequest webServiceRequest = new WebServiceRequest(webServiceName);
      webServiceRequest.setRunConfigurationName(
          request.getParameter(WebServiceExecutor.PARAMETER_RUN_CONFIG));
      webServiceRequest.setBodyContentSupplier(() -> readBody(request));
      webServiceRequest.setHeaders(collectHeaders(request));
      webServiceRequest.setParameters(collectParameters(request));

      PreparedWebService prepared =
          new WebServiceExecutor(variables, metadataProvider, getPipelineMap())
              .prepare(webServiceRequest);

      prepared.execute(new ServletWebServiceOutput(response));

    } catch (Exception e) {
      logError("Error producing web service output", e);
      sendSafeError(
          response,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Error producing web service output.");
    }
  }

  private static String readBody(HttpServletRequest request) throws HopException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(request.getInputStream(), out);
      return out.toString(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new HopException("Error reading the web service request body", e);
    }
  }

  private static Map<String, String> collectHeaders(HttpServletRequest request) {
    Map<String, String> headers = new LinkedHashMap<>();
    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String headerName = headerNames.nextElement();
      headers.put(headerName, request.getHeader(headerName));
    }
    return headers;
  }

  private static Map<String, String> collectParameters(HttpServletRequest request) {
    Map<String, String> parameters = new LinkedHashMap<>();
    for (String requestParameter : request.getParameterMap().keySet()) {
      parameters.put(requestParameter, request.getParameter(requestParameter));
    }
    return parameters;
  }

  public String toString() {
    return "Web Service Servlet";
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
