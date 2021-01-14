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

import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineSvgPainter;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@HopServerServlet(id = "pipelineImage", name = "Generate a PNG image of a pipeline")
public class GetPipelineImageServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final long serialVersionUID = -4365372274638005929L;
  public static final float ZOOM_FACTOR = 1.0f;

  private static final Class<?> PKG = GetPipelineImageServlet.class; // For Translator

  public static final String CONTEXT_PATH = "/hop/pipelineImage";

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "GetPipelineImageServlet.Log.PipelineImageRequested"));
    }

    String pipelineName = request.getParameter("name");
    String id = request.getParameter("id");

    ByteArrayOutputStream svgStream = null;
    try {
      // ID is optional...
      //
      IPipelineEngine<PipelineMeta> pipeline;
      HopServerObjectEntry entry;
      if (Utils.isEmpty(id)) {
        throw new Exception("Please provide the ID of pipeline '" + pipelineName + "'");
      } else {
        entry = new HopServerObjectEntry(pipelineName, id);
        pipeline = getPipelineMap().getPipeline(entry);
      }

      if (pipeline != null) {

        response.setStatus(HttpServletResponse.SC_OK);

        response.setCharacterEncoding("UTF-8");
        response.setContentType("image/svg+xml");

        // Generate pipeline SVG image
        //
        String svgXml =
            PipelineSvgPainter.generatePipelineSvg(pipeline.getPipelineMeta(), 1.0f, variables);
        svgStream = new ByteArrayOutputStream();
        try {
          svgStream.write(svgXml.getBytes("UTF-8"));
        } finally {
          svgStream.flush();
        }
        response.setContentLength(svgStream.size());

        OutputStream out = response.getOutputStream();
        out.write(svgStream.toByteArray());
      }
    } catch (Exception e) {
      throw new IOException("Error building SVG image of pipleine", e);
    } finally {
      if (svgStream != null) {
        svgStream.close();
      }
    }
  }

  public String toString() {
    return "Pipeline Image IHandler";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
