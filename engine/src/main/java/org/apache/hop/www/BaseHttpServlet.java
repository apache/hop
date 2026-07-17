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

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.Setter;
import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;

public class BaseHttpServlet extends HttpServlet {
  @Serial protected static final long serialVersionUID = -1348342810327662788L;

  @Setter @Getter protected PipelineMap pipelineMap;

  @Setter @Getter protected WorkflowMap workflowMap;

  @Setter @Getter protected HopServerConfig serverConfig;
  protected IVariables variables;

  @Setter @Getter protected boolean supportGraphicEnvironment;

  @Setter @Getter private boolean jettyMode = false;

  @Setter @Getter protected ILogChannel log = new LogChannel("Servlet");

  public String convertContextPath(String contextPath) {
    if (jettyMode) {
      return contextPath;
    }
    return contextPath.substring(contextPath.lastIndexOf("/") + 1);
  }

  /**
   * Returns the location under which the bundled static assets (icons, css) are served, made
   * relative to the deployment root. The servlet's own {@code contextPath} is stripped off the
   * request URI, so the result is root-based for a root deployment and carries the servlet
   * container context path / reverse-proxy prefix otherwise.
   *
   * <p>The assets ship at {@link StatusServletUtils#STATIC_PATH} in both the standalone hop-server
   * (served on the root Jetty context) and the Hop Web war (unpacked to the war root), so this
   * resolves correctly in every deployment - including behind a reverse proxy - without a
   * Jetty-vs-servlet-container branch.
   */
  protected String getStaticPath(HttpServletRequest request, String contextPath) {
    String requestUri = request.getRequestURI();
    String root = "";
    if (requestUri != null) {
      int index = requestUri.indexOf(contextPath);
      if (index > 0) {
        root = requestUri.substring(0, index);
      }
    }
    return root + StatusServletUtils.STATIC_PATH;
  }

  public BaseHttpServlet() {}

  public BaseHttpServlet(PipelineMap pipelineMap) {
    this.pipelineMap = pipelineMap;
    this.jettyMode = true;
    this.serverConfig = pipelineMap.getHopServerConfig();
    if (serverConfig == null) {
      this.variables = Variables.getADefaultVariableSpace();
    } else {
      this.variables = serverConfig.getVariables();
    }
  }

  public BaseHttpServlet(WorkflowMap workflowMap) {
    this.workflowMap = workflowMap;
    this.jettyMode = true;
    this.serverConfig = workflowMap.getHopServerConfig();
    if (serverConfig == null) {
      this.variables = Variables.getADefaultVariableSpace();
    } else {
      this.variables = serverConfig.getVariables();
    }
  }

  public BaseHttpServlet(PipelineMap pipelineMap, WorkflowMap workflowMap) {
    this.pipelineMap = pipelineMap;
    this.workflowMap = workflowMap;
    this.jettyMode = true;
    this.serverConfig = pipelineMap.getHopServerConfig();
    if (serverConfig == null) {
      this.variables = Variables.getADefaultVariableSpace();
    } else {
      this.variables = serverConfig.getVariables();
    }
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    if (req.getContentLength() > 0 && req.getContentType() != null) {
      String encoding = getContentEncoding(req.getContentType());
      if (encoding != null) {
        req.setCharacterEncoding(encoding);
      }
    }
    if ("GET".equals(req.getMethod())) {
      supportGraphicEnvironment =
          Boolean.TRUE.equals(req.getServletContext().getAttribute("GraphicsEnvironment"));
    }
    super.service(req, resp);
  }

  @Override
  protected void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      doGet(request, response);
    } catch (Exception e) {
      logError("Error handling PUT request", e);
      sendSafeError(
          response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unable to process PUT request.");
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      doGet(request, response);
    } catch (Exception e) {
      logError("Error handling POST request", e);
      sendSafeError(
          response,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Unable to process POST request.");
    }
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      doGet(req, resp);
    } catch (Exception e) {
      logError("Error handling DELETE request", e);
      sendSafeError(
          resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unable to process DELETE request.");
    }
  }

  protected boolean isJsonRequest(HttpServletRequest request) {
    return "Y".equalsIgnoreCase(request.getParameter("json"));
  }

  protected void setResponseFormat(HttpServletResponse response, boolean useXml, boolean useJson) {
    if (useXml) {
      response.setContentType("text/xml");
      response.setCharacterEncoding(Const.UTF_8);
    } else if (useJson) {
      response.setContentType("application/json");
      response.setCharacterEncoding(Const.UTF_8);
    } else {
      response.setContentType("text/html;charset=UTF-8");
    }
  }

  protected PrintWriter getSafeWriter(HttpServletResponse response) {
    try {
      return response.getWriter();
    } catch (IOException e) {
      log.logError("Failed to obtain response writer", e);
      sendSafeError(
          response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unable to process request.");
      return null;
    }
  }

  protected BufferedReader getSafeReader(HttpServletRequest request, HttpServletResponse response) {
    try {
      return request.getReader();
    } catch (IOException e) {
      log.logError("Failed to obtain request reader", e);
      sendSafeError(
          response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unable to process request.");
      return null;
    }
  }

  protected void sendSafeError(HttpServletResponse response, int status, String message) {
    if (response.isCommitted()) {
      response.setStatus(status);
      return;
    }
    try {
      response.sendError(status, message);
    } catch (IOException e) {
      log.logError("Failed to send error response (" + status + "): " + message, e);
      response.setStatus(status);
    }
  }

  /**
   * Refuse a request with HTTP 503 (Service Unavailable) while the server is performing a graceful
   * shutdown. Servlets that accept new work (register/add/start/execute pipelines and workflows)
   * should call this at the top of their handler and return immediately when it returns true.
   * Status and reporting servlets do not call it and keep working during shutdown.
   *
   * @return true when the server is shutting down and an error response has been sent; the caller
   *     must return immediately without performing any work.
   */
  protected boolean refuseIfShuttingDown(HttpServletResponse response) {
    if (!HopServerSingleton.isServerShuttingDown()) {
      return false;
    }
    logBasic("Refused a request: the Hop server is shutting down.");
    sendSafeError(
        response,
        HttpServletResponse.SC_SERVICE_UNAVAILABLE,
        "The Hop server is shutting down and is not accepting new work.");
    return true;
  }

  /**
   * Log server-side and return a {@link WebResult} error to the client in the requested XML or JSON
   * shape. Use {@code writer} when the response writer is already acquired for this request;
   * otherwise pass {@code null} and the output stream is used.
   */
  protected void writeXmlOrJsonApiError(
      HttpServletResponse response,
      PrintWriter writer,
      boolean useXml,
      boolean useJson,
      String logMessage,
      Throwable cause) {
    log.logError(logMessage, cause);
    final String clientMessage = "Unable to complete request.";
    if (response.isCommitted()) {
      return;
    }
    try {
      response.resetBuffer();
    } catch (IllegalStateException e) {
      sendSafeError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, clientMessage);
      return;
    }
    if (!useXml && !useJson) {
      sendSafeError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, clientMessage);
      return;
    }
    setResponseFormat(response, useXml, useJson);
    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    WebResult errorResult = new WebResult(WebResult.STRING_ERROR, clientMessage);
    try {
      if (useXml) {
        String payload = XmlHandler.getXmlHeader(Const.UTF_8) + errorResult.getXml();
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        if (writer != null) {
          writer.write(payload);
          writer.flush();
        } else {
          OutputStream os = response.getOutputStream();
          response.setContentLength(bytes.length);
          os.write(bytes);
          os.flush();
        }
      } else {
        String payload = errorResult.getJson();
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        if (writer != null) {
          writer.write(payload);
          writer.flush();
        } else {
          OutputStream os = response.getOutputStream();
          response.setContentLength(bytes.length);
          os.write(bytes);
          os.flush();
        }
      }
    } catch (IOException e) {
      log.logError("Failed to write API error response", e);
      sendSafeError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, clientMessage);
    }
  }

  public PipelineMap getPipelineMap() {
    if (pipelineMap == null) {
      return HopServerSingleton.getInstance().getPipelineMap();
    }
    return pipelineMap;
  }

  public WorkflowMap getWorkflowMap() {
    if (workflowMap == null) {
      return HopServerSingleton.getInstance().getWorkflowMap();
    }
    return workflowMap;
  }

  public void logMinimal(String s) {
    log.logMinimal(s);
  }

  public void logBasic(String s) {
    log.logBasic(s);
  }

  public void logError(String s) {
    log.logError(s);
  }

  public void logError(String s, Throwable e) {
    log.logError(s, e);
  }

  public void logBasic(String s, Object... arguments) {
    log.logBasic(s, arguments);
  }

  public void logDetailed(String s, Object... arguments) {
    log.logDetailed(s, arguments);
  }

  public void logError(String s, Object... arguments) {
    log.logError(s, arguments);
  }

  public void logDetailed(String s) {
    log.logDetailed(s);
  }

  public void logDebug(String s) {
    log.logDebug(s);
  }

  public void logRowlevel(String s) {
    log.logRowlevel(s);
  }

  public void setup(PipelineMap pipelineMap, WorkflowMap workflowMap) {
    this.pipelineMap = pipelineMap;
    this.workflowMap = workflowMap;
    this.serverConfig = pipelineMap.getHopServerConfig();
    this.variables = serverConfig.getVariables();
  }

  private String getContentEncoding(String contentTypeValue) {
    ContentType contentType = ContentType.parse(contentTypeValue);
    if ("text/xml".equals(contentType.getMimeType())) {
      if (contentType.getCharset() != null) {
        return contentType.getCharset().name();
      }
      return Const.UTF_8;
    }
    return null;
  }

  /**
   * The logging object a servlet hands to the pipeline or workflow it creates, as its logging
   * parent.
   *
   * <p>It is given a log channel of its own: everything the pipeline or workflow logs is a child of
   * it, so writing that log to a file hangs the file writer on this log channel. Without one there
   * is nothing to hang it on. See issue #4677.
   *
   * @param contextPath the path of the servlet, to name the logging object after
   * @param serverObjectId the id this server knows the pipeline or workflow by
   * @param level the log level that was asked for
   * @return the logging object to use as a parent
   */
  protected SimpleLoggingObject getServletLogging(
      final String contextPath, final String serverObjectId, final LogLevel level) {
    SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject(contextPath, LoggingObjectType.HOP_SERVER, null);
    servletLoggingObject.setContainerObjectId(serverObjectId);
    servletLoggingObject.setLogLevel(level);
    servletLoggingObject.setLogChannelId(
        LoggingRegistry.getInstance().registerLoggingSource(servletLoggingObject, true));
    return servletLoggingObject;
  }
}
