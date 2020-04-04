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

import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SwingGC;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.pipeline.Pipeline;

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

  /**
   * <div id="mindtouch">
   * <h1>/hop/pipelineImage</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Generates PNG image of the specified pipeline currently present on HopServer server.
   * Pipeline name and HopServer pipeline ID (optional) are used for specifying which
   * pipeline to get information for. Response is a binary of the PNG image.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/pipelineImage?name=dummy-pipeline
   * </pre>
   *
   * </p>
   * <h3>Parameters</h3>
   * <table class="pentaho-table">
   * <tbody>
   * <tr>
   * <th>name</th>
   * <th>description</th>
   * <th>type</th>
   * </tr>
   * <tr>
   * <td>name</td>
   * <td>Name of the pipeline to be used for image generation.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>id</td>
   * <td>HopServer id of the pipeline to be used for image generation.</td>
   * <td>query, optional</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * <h3>Response Body</h3>
   *
   * <table class="pentaho-table">
   * <tbody>
   * <tr>
   * <td align="right">binary streak:</td>
   * <td>image</td>
   * </tr>
   * <tr>
   * <td align="right">media types:</td>
   * <td>image/png</td>
   * </tr>
   * </tbody>
   * </table>
   * <p>A binary PNG image or empty response is presented if no pipeline is found.</p>
   *
   * <h3>Status Codes</h3>
   * <table class="pentaho-table">
   * <tbody>
   * <tr>
   * <th>code</th>
   * <th>description</th>
   * </tr>
   * <tr>
   * <td>200</td>
   * <td>Request was processed.</td>
   * </tr>
   * <tr>
   * <td>500</td>
   * <td>Internal server error occurs during request processing.</td>
   * </tr>
   * </tbody>
   * </table>
   * </div>
   */
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
    Pipeline pipeline;
    HopServerObjectEntry entry;
    if ( Utils.isEmpty( id ) ) {
      // get the first pipeline that matches...
      //
      entry = getPipelineMap().getFirstCarteObjectEntry( pipelineName );
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
        BufferedImage image = generatePipelineImage( pipeline.getPipelineMeta() );
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

    SwingGC gc = new SwingGC( null, maximum, 32, 0, 0 );
    PipelinePainter pipelinePainter =
      new PipelinePainter(
        gc, pipelineMeta, maximum, null, null, null, null, null, new ArrayList<AreaOwner>(), 32, 1, 0, 0, true, "Arial", 10, 1.0d );
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
