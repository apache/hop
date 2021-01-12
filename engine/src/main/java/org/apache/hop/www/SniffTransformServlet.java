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

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.IRowListener;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.List;

@HopServerServlet(id="sniffTransform", name = "Sniff test a pipeline transform")
public class SniffTransformServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static final Class<?> PKG = GetPipelineStatusServlet.class; // For Translator

  private static final long serialVersionUID = 3634806745372015720L;
  public static final String CONTEXT_PATH = "/hop/sniffTransform";

  public static final String TYPE_INPUT = "input";
  public static final String TYPE_OUTPUT = "output";

  public SniffTransformServlet() {
  }

  public SniffTransformServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }


  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException,
    IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "PipelineStatusServlet.Log.SniffTransformRequested" ) );
    }

    String pipelineName = request.getParameter( "pipeline" );
    String id = request.getParameter( "id" );
    String transformName = request.getParameter( "transform" );
    int copyNr = Const.toInt( request.getParameter( "copynr" ), 0 );
    final int nrLines = Const.toInt( request.getParameter( "lines" ), 0 );
    String type = Const.NVL( request.getParameter( "type" ), TYPE_OUTPUT );
    boolean useXML = "Y".equalsIgnoreCase( request.getParameter( "xml" ) );

    response.setStatus( HttpServletResponse.SC_OK );

    if ( useXML ) {
      response.setContentType( "text/xml" );
      response.setCharacterEncoding( Const.XML_ENCODING );
    } else {
      response.setContentType( "text/html;charset=UTF-8" );
    }

    PrintWriter out = response.getWriter();

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

    if ( pipeline != null ) {

      // Find the transform to look at...
      //
      IEngineComponent component = null;
      List<IEngineComponent> componentCopies = pipeline.getComponentCopies( transformName );
      for ( IEngineComponent componentCopy : componentCopies ) {
        if ( componentCopy.getCopyNr() == copyNr ) {
          component = componentCopy;
        }
      }
      final RowBuffer rowBuffer = new RowBuffer();

      if ( component != null ) {

        // Wait until the pipeline is running...
        //
        while (!(pipeline.isRunning() || pipeline.isReadyToStart()) && !pipeline.isStopped()) {
          try {
            Thread.sleep(10);
          } catch ( InterruptedException e ) {
            // ignore
          }
        }

        if (!pipeline.isStopped()) {
          // Add a listener to the pipeline transform...
          //
          final boolean read = type.equalsIgnoreCase( TYPE_INPUT );
          final boolean written = type.equalsIgnoreCase( TYPE_OUTPUT ) || !read;

          IRowListener rowListener = new IRowListener() {
            public void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
              if ( read && rowBuffer.getBuffer().size() < nrLines ) {
                rowBuffer.setRowMeta( rowMeta );
                rowBuffer.getBuffer().add( row );
              }
            }

            public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
              if ( written && rowBuffer.getBuffer().size() < nrLines ) {
                rowBuffer.setRowMeta( rowMeta );
                rowBuffer.getBuffer().add( row );
              }
            }

            public void errorRowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
            }
          };

          component.addRowListener( rowListener );

          // Wait until we have enough rows...
          //
          while ( rowBuffer.getBuffer().size() < nrLines && component.isRunning() && !pipeline.isFinished() && !pipeline.isStopped() ) {
            try {
              Thread.sleep( 10 );
            } catch ( InterruptedException e ) {
              // Ignore
              //
              break;
            }
          }

          // Remove the row listener
          //
          component.removeRowListener( rowListener );
        }

        // Pass along the rows of data...
        //
        if ( useXML ) {

          // Send the result back as XML
          //
          response.setContentType( "text/xml" );
          response.setCharacterEncoding( Const.XML_ENCODING );
          out.print( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
          out.println( rowBuffer.getXml() );

        } else {
          response.setContentType( "text/html;charset=UTF-8" );

          out.println( "<HTML>" );
          out.println( "<HEAD>" );
          out.println( "<TITLE>" + BaseMessages.getString( PKG, "SniffTransformServlet.SniffResults" ) + "</TITLE>" );
          out.println( "<META http-equiv=\"Refresh\" content=\"10;url="
            + convertContextPath( CONTEXT_PATH ) + "?name=" + URLEncoder.encode( pipelineName, "UTF-8" ) + "&id="
            + URLEncoder.encode( id, "UTF-8" ) + "\">" );
          out.println( "<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">" );
          out.println( "</HEAD>" );
          out.println( "<BODY>" );
          out.println( "<H1>"
            + Encode.forHtml( BaseMessages.getString(
            PKG, "SniffTransformServlet.SniffResultsForTransform", transformName ) ) + "</H1>" );

          try {
            out.println( "<table border=\"1\">" );

            if ( rowBuffer.getRowMeta() != null ) {
              // Print a header row containing all the field names...
              //
              out.print( "<tr><th>#</th>" );
              for ( IValueMeta valueMeta : rowBuffer.getRowMeta().getValueMetaList() ) {
                out.print( "<th>" + valueMeta.getName() + "</th>" );
              }
              out.println( "</tr>" );

              // Now output the data rows...
              //
              for ( int r = 0; r < rowBuffer.getBuffer().size(); r++ ) {
                Object[] rowData = rowBuffer.getBuffer().get( r );
                out.print( "<tr>" );
                out.println( "<td>" + ( r + 1 ) + "</td>" );
                for ( int v = 0; v < rowBuffer.getRowMeta().size(); v++ ) {
                  IValueMeta valueMeta = rowBuffer.getRowMeta().getValueMeta( v );
                  Object valueData = rowData[ v ];
                  out.println( "<td>" + valueMeta.getString( valueData ) + "</td>" );
                }
                out.println( "</tr>" );
              }
            }

            out.println( "</table>" );

            out.println( "<p>" );

          } catch ( Exception ex ) {
            out.println( "<p>" );
            out.println( "<pre>" );
            out.println( Encode.forHtml( Const.getStackTracker( ex ) ) );
            out.println( "</pre>" );
          }

          out.println( "<p>" );
          out.println( "</BODY>" );
          out.println( "</HTML>" );
        }
      } else {
        if ( useXML ) {
          out.println( new WebResult( WebResult.STRING_ERROR, BaseMessages.getString(
            PKG, "SniffTransformServlet.Log.CoundNotFindSpecTransform", transformName ) ).getXml() );
        } else {
          out.println( "<H1>"
            + Encode.forHtml( BaseMessages.getString(
            PKG, "SniffTransformServlet.Log.CoundNotFindSpecTransform", transformName ) ) + "</H1>" );
          out.println( "<a href=\""
            + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
            + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
        }
      }
    } else {
      if ( useXML ) {
        out.println( new WebResult( WebResult.STRING_ERROR, BaseMessages.getString(
          PKG, "SniffTransformServlet.Log.CoundNotFindSpecPipeline", pipelineName ) ).getXml() );
      } else {
        out.println( "<H1>"
          + Encode.forHtml( BaseMessages.getString(
          PKG, "SniffTransformServlet.Log.CoundNotFindPipeline", pipelineName ) ) + "</H1>" );
        out.println( "<a href=\""
          + convertContextPath( GetStatusServlet.CONTEXT_PATH ) + "\">"
          + BaseMessages.getString( PKG, "PipelineStatusServlet.BackToStatusPage" ) + "</a><p>" );
      }
    }
  }

  public String toString() {
    return "Sniff Transform";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
