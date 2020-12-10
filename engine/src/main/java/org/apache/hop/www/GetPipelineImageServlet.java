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
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SvgGc;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

@HopServerServlet(id="pipelineImage", name = "Generate a PNG image of a pipeline")
public class GetPipelineImageServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final long serialVersionUID = -4365372274638005929L;
  public static final float ZOOM_FACTOR = 1.0f;

  private static final Class<?> PKG = GetPipelineImageServlet.class; // Needed by Translator

  public static final String CONTEXT_PATH = "/hop/pipelineImage";

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "GetPipelineImageServlet.Log.PipelineImageRequested" ) );
    }

    String pipelineName = request.getParameter( "name" );
    String id = request.getParameter( "id" );

    ByteArrayOutputStream svgStream = null;
    try {
      // ID is optional...
      //
      IPipelineEngine<PipelineMeta> pipeline;
      HopServerObjectEntry entry;
      if ( Utils.isEmpty( id ) ) {
        throw new Exception("Please provide the ID of pipeline '"+pipelineName+"'");
      } else {
        entry = new HopServerObjectEntry( pipelineName, id );
        pipeline = getPipelineMap().getPipeline( entry );
      }

      if ( pipeline != null ) {

        response.setStatus( HttpServletResponse.SC_OK );

        response.setCharacterEncoding( "UTF-8" );
        response.setContentType( "image/svg+xml" );

        // Generate pipeline SVG image
        //
        String svgXml = generatePipelineSvgImage( pipeline.getPipelineMeta() );
        svgStream = new ByteArrayOutputStream();
        try {
          svgStream.write( svgXml.getBytes("UTF-8") );
        } finally {
          svgStream.flush();
        }
        response.setContentLength( svgStream.size() );

        OutputStream out = response.getOutputStream();
        out.write( svgStream.toByteArray() );
      }
    } catch ( Exception e ) {
      throw new IOException("Error building SVG image of pipleine", e);
    } finally {
      if (svgStream!=null) {
        svgStream.close();
      }
    }
  }

  private String generatePipelineSvgImage( PipelineMeta pipelineMeta ) throws Exception {
    float magnification = ZOOM_FACTOR;
    Point maximum = pipelineMeta.getMaximum();
    maximum.multiply( magnification );

    HopSvgGraphics2D graphics2D = HopSvgGraphics2D.newDocument();

    SvgGc gc = new SvgGc( graphics2D, new Point(maximum.x+100, maximum.y+100), 32, 0, 0 );
    PipelinePainter pipelinePainter = new PipelinePainter( gc, variables, pipelineMeta, maximum, null, null, null, null, null, new ArrayList<>(), 32, 1, 0, "Arial", 10, 1.0d );
    pipelinePainter.setMagnification( magnification );
    pipelinePainter.drawPipelineImage();

    // convert to SVG XML
    //
    return graphics2D.toXml();
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
