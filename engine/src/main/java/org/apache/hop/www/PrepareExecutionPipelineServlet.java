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

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serial;
import java.net.URLEncoder;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.owasp.encoder.Encode;

@HopServerServlet(id = "prepareExec", name = "Prepare the execution of a pipeline")
public class PrepareExecutionPipelineServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = PrepareExecutionPipelineServlet.class;
  private static final String CONST_NAME = "?name=";
  private static final String CONST_HEADER_CLOSE = "</H1>";
  private static final String CONST_HEADER_OPEN = "<H1>";
  private static final String CONST_LINK_OPEN = "<a href=\"";
  private static final String CONST_LINK_CLOSE = "</a><p>";
  private static final String CONST_CLOSE_TAG = "\">";
  @Serial private static final long serialVersionUID = 3634806745372015720L;
  public static final String CONTEXT_PATH = "/hop/prepareExec";

  public PrepareExecutionPipelineServlet() {}

  public PrepareExecutionPipelineServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug(
          BaseMessages.getString(
              PKG, "PrepareExecutionPipelineServlet.PipelinePrepareExecutionRequested"));
    }

    String pipelineName = request.getParameter("name");
    String id = request.getParameter("id");
    boolean useXML = "Y".equalsIgnoreCase(request.getParameter("xml"));

    response.setStatus(HttpServletResponse.SC_OK);

    PrintWriter out = response.getWriter();
    if (useXML) {
      response.setContentType("text/xml");
      out.print(XmlHandler.getXmlHeader(Const.XML_ENCODING));
    } else {

      response.setCharacterEncoding("UTF-8");
      response.setContentType("text/html;charset=UTF-8");

      out.println("<HTML>");
      out.println("<HEAD>");
      out.println(
          "<TITLE>"
              + BaseMessages.getString(
                  PKG, "PrepareExecutionPipelineServlet.PipelinePrepareExecution")
              + "</TITLE>");
      out.println(
          "<META http-equiv=\"Refresh\" content=\"2;url="
              + convertContextPath(GetPipelineStatusServlet.CONTEXT_PATH)
              + CONST_NAME
              + URLEncoder.encode(pipelineName, UTF_8)
              + CONST_CLOSE_TAG);
      out.println("<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">");
      out.println("<link rel=\"icon\" type=\"image/svg+xml\" href=\"/static/images/favicon.svg\">");
      out.println("</HEAD>");
      out.println("<BODY>");
    }

    try {
      // ID is optional...
      //
      IPipelineEngine<PipelineMeta> pipeline;
      HopServerObjectEntry entry;
      if (Utils.isEmpty(id)) {
        // get the first pipeline that matches...
        //
        entry = getPipelineMap().getFirstServerObjectEntry(pipelineName);
        if (entry == null) {
          pipeline = null;
        } else {
          id = entry.getId();
          pipeline = getPipelineMap().getPipeline(entry);
        }
      } else {
        // Take the ID into account!
        //
        entry = new HopServerObjectEntry(pipelineName, id);
        pipeline = getPipelineMap().getPipeline(entry);
      }

      PipelineConfiguration pipelineConfiguration = getPipelineMap().getConfiguration(entry);

      if (pipeline != null && pipelineConfiguration != null) {
        PipelineExecutionConfiguration executionConfiguration =
            pipelineConfiguration.getPipelineExecutionConfiguration();
        // Set the appropriate logging, variables, arguments, replay date, ...
        // etc.
        pipeline.setVariables(executionConfiguration.getVariablesMap());
        pipeline.setPreviousResult(executionConfiguration.getPreviousResult());

        try {
          pipeline.prepareExecution();

          if (useXML) {
            out.println(WebResult.OK.getXml());
          } else {

            out.println(
                CONST_HEADER_OPEN
                    + Encode.forHtml(
                        BaseMessages.getString(
                            PKG, "PrepareExecutionPipelineServlet.PipelinePrepared", pipelineName))
                    + CONST_HEADER_CLOSE);
            out.println(
                CONST_LINK_OPEN
                    + convertContextPath(GetPipelineStatusServlet.CONTEXT_PATH)
                    + CONST_NAME
                    + URLEncoder.encode(pipelineName, UTF_8)
                    + "&id="
                    + URLEncoder.encode(id, UTF_8)
                    + CONST_CLOSE_TAG
                    + BaseMessages.getString(PKG, "PipelineStatusServlet.BackToPipelineStatusPage")
                    + CONST_LINK_CLOSE);
          }
        } catch (Throwable e) {
          String logText =
              HopLogStore.getAppender()
                  .getBuffer(pipeline.getLogChannel().getLogChannelId(), true)
                  .toString();
          if (useXML) {
            out.println(
                new WebResult(
                    WebResult.STRING_ERROR,
                    BaseMessages.getString(
                        PKG,
                        "PrepareExecutionPipelineServlet.Error.PipelineInitFailed",
                        Const.CR
                            + logText
                            + Const.CR
                            + Const.getSimpleStackTrace(e)
                            + Const.CR
                            + Const.getStackTracker(e))));
          } else {
            out.println(
                CONST_HEADER_OPEN
                    + Encode.forHtml(
                        BaseMessages.getString(
                            PKG,
                            "PrepareExecutionPipelineServlet.Log.PipelineNotInit",
                            pipelineName))
                    + CONST_HEADER_CLOSE);
            out.println("<pre>");
            out.println(Encode.forHtml(logText));
            out.println(Encode.forHtml(Const.getStackTracker(e)));
            out.println("</pre>");
            out.println(
                CONST_LINK_OPEN
                    + convertContextPath(GetPipelineStatusServlet.CONTEXT_PATH)
                    + CONST_NAME
                    + URLEncoder.encode(pipelineName, UTF_8)
                    + "&id="
                    + URLEncoder.encode(id, UTF_8)
                    + CONST_CLOSE_TAG
                    + BaseMessages.getString(PKG, "PipelineStatusServlet.BackToPipelineStatusPage")
                    + CONST_LINK_CLOSE);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          }
        }
      } else {
        if (useXML) {
          out.println(
              new WebResult(
                  WebResult.STRING_ERROR,
                  BaseMessages.getString(
                      PKG, "PipelineStatusServlet.Log.CoundNotFindSpecPipeline", pipelineName)));
        } else {
          out.println(
              CONST_HEADER_OPEN
                  + Encode.forHtml(
                      BaseMessages.getString(
                          PKG, "PipelineStatusServlet.Log.CoundNotFindPipeline", pipelineName))
                  + CONST_HEADER_CLOSE);
          out.println(
              CONST_LINK_OPEN
                  + convertContextPath(GetStatusServlet.CONTEXT_PATH)
                  + CONST_CLOSE_TAG
                  + BaseMessages.getString(PKG, "PipelineStatusServlet.BackToStatusPage")
                  + CONST_LINK_CLOSE);
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
      }
    } catch (Exception ex) {
      if (useXML) {
        out.println(
            new WebResult(
                WebResult.STRING_ERROR,
                BaseMessages.getString(
                    PKG,
                    "PrepareExecutionPipelineServlet.Error.UnexpectedError",
                    Const.CR + Const.getStackTracker(ex))));
      } else {
        out.println("<p>");
        out.println("<pre>");
        out.println(Encode.forHtml(Const.getStackTracker(ex)));
        out.println("</pre>");
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      }
    }

    if (!useXML) {
      out.println("<p>");
      out.println("</BODY>");
      out.println("</HTML>");
    }
  }

  public String toString() {
    return "Start pipeline";
  }

  @Override
  public String getService() {
    return CONTEXT_PATH + " (" + this + ")";
  }

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
