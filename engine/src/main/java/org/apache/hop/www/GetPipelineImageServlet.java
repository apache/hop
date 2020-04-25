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

package org.apache.hop.www;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SwingGc;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

public class GetPipelineImageServlet extends BaseHttpServlet implements IHopServerPlugin {

  private static final long serialVersionUID = -4365372274638005929L;

  private static Class<?> PKG = GetPipelineImageServlet.class; // for i18n purposes, needed by Translator!!

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

    // ID is optional...
    //
    IPipelineEngine<PipelineMeta> pipeline;
    HopServerObjectEntry entry;
    if ( Utils.isEmpty( id ) ) {
      // get the first pipeline that matches...
      //
      entry = getPipelineMap().getFirstServerObjectEntry( pipelineName );
      if ( entry == null ) {
        pipeline = null;
      } else {
        id = entry.getId();
        pipeline = getPipelineMap().getPipeline( entry );
      }
    } else {
      // Take the ID into account!
      //
      entry = new HopServerObjectEntry( pipelineName, id );
      pipeline = getPipelineMap().getPipeline( entry );
    }

    try {
      if ( pipeline != null ) {

        response.setStatus( HttpServletResponse.SC_OK );

        response.setCharacterEncoding( "UTF-8" );
        response.setContentType( "image/png" );

        // Generate xform image
        //
        BufferedImage image = generatePipelineImage( pipeline.getSubject() );
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
          ImageIO.write( image, "png", os );
        } finally {
          os.flush();
        }
        response.setContentLength( os.size() );

        OutputStream out = response.getOutputStream();
        out.write( os.toByteArray() );

      }
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  private BufferedImage generatePipelineImage( PipelineMeta pipelineMeta ) throws Exception {
    float magnification = 1.0f;
    Point maximum = pipelineMeta.getMaximum();
    maximum.multiply( magnification );

    SwingGc gc = new SwingGc( null, maximum, 32, 0, 0 );
    PipelinePainter pipelinePainter = new PipelinePainter( gc, pipelineMeta, maximum, null, null, null, null, null, new ArrayList<>(), 32, 1, 0, "Arial", 10, 1.0d );
    pipelinePainter.setMagnification( magnification );
    pipelinePainter.buildPipelineImage();

    return (BufferedImage) gc.getImage();
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
