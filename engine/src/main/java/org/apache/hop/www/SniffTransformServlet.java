/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransform;
import org.owasp.encoder.Encode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;


public class SniffTransformServlet extends BaseHttpServlet implements IHopServerPlugin {
  private static Class<?> PKG = GetPipelineStatusServlet.class; // for i18n purposes, needed by Translator!!

  private static final long serialVersionUID = 3634806745372015720L;
  public static final String CONTEXT_PATH = "/hop/sniffTransform";

  public static final String TYPE_INPUT = "input";
  public static final String TYPE_OUTPUT = "output";

  public static final String XML_TAG = "transform-sniff";

  final class MetaAndData {
    public IRowMeta bufferRowMeta;
    public List<Object[]> bufferRowData;
  }

  public SniffTransformServlet() {
  }

  public SniffTransformServlet( PipelineMap pipelineMap ) {
    super( pipelineMap );
  }

  /**
   * <div id="mindtouch">
   * <h1>/hop/sniffTransform</h1>
   * <a name="GET"></a>
   * <h2>GET</h2>
   * <p>Sniff metadata and data from the specified transform of the specified pipeline.</p>
   *
   * <p><b>Example Request:</b><br />
   * <pre function="syntax.xml">
   * GET /hop/sniffTransform?pipeline=dummy-pipeline&transform=tf&xml=Y&lines=10
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
   * <td>pipeline</td>
   * <td>Name of the pipeline containing required transform.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>transformName</td>
   * <td>Name of the pipeline transform to collect data for.</td>
   * <td>query</td>
   * </tr>
   * <tr>
   * <td>copynr</td>
   * <td>Copy number of the transform to be used for collecting data. If not provided 0 is used.</td>
   * <td>integer, optional</td>
   * </tr>
   * <tr>
   * <td>type</td>
   * <td>Type of the data to be collected (<code>input</code> or <code>output</code>).
   * If not provided output data is collected.</td>
   * <td>query, optional</td>
   * </tr>
   * <tr>
   * <td>xml</td>
   * <td>Boolean flag which defines output format <code>Y</code> forces XML output to be generated.
   * HTML is returned otherwise.</td>
   * <td>boolean, optional</td>
   * </tr>
   * <tr>
   * <td>id</td>
   * <td>HopServer id of the pipeline to be used for transform lookup.</td>
   * <td>query, optional</td>
   * </tr>
   * <tr>
   * <td>lines</td>
   * <td>Number of lines to collect and include into response. If not provided 0 lines will be collected.</td>
   * <td>integer, optional</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * <h3>Response Body</h3>
   *
   * <table class="pentaho-table">
   * <tbody>
   * <tr>
   * <td align="right">element:</td>
   * <td>(custom)</td>
   * </tr>
   * <tr>
   * <td align="right">media types:</td>
   * <td>text/xml, text/html</td>
   * </tr>
   * </tbody>
   * </table>
   * <p>Response XML or HTML response containing data and metadata of the transform.
   * If an error occurs during method invocation <code>result</code> field of the response
   * will contain <code>ERROR</code> status.</p>
   *
   * <p><b>Example Response:</b></p>
   * <pre function="syntax.xml">
   * <?xml version="1.0" encoding="UTF-8"?>
   * <transform-sniff>
   * <row-meta>
   * <value-meta><type>String</type>
   * <storagetype>normal</storagetype>
   * <name>Field1</name>
   * <length>0</length>
   * <precision>-1</precision>
   * <origin>tf</origin>
   * <comments/>
   * <conversion_Mask/>
   * <decimal_symbol>.</decimal_symbol>
   * <grouping_symbol>,</grouping_symbol>
   * <currency_symbol>&#x24;</currency_symbol>
   * <trim_type>none</trim_type>
   * <case_insensitive>N</case_insensitive>
   * <sort_descending>N</sort_descending>
   * <output_padding>N</output_padding>
   * <date_format_lenient>Y</date_format_lenient>
   * <date_format_locale>en_US</date_format_locale>
   * <date_format_timezone>America&#x2f;Bahia</date_format_timezone>
   * <lenient_string_to_number>N</lenient_string_to_number>
   * </value-meta>
   * </row-meta>
   * <nr_rows>10</nr_rows>
   *
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * <row-data><value-data>my-data </value-data>
   * </row-data>
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * <row-data><value-data>my-data</value-data>
   * </row-data>
   * </transform-sniff>
   * </pre>
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

    if ( pipeline != null ) {

      // Find the transform to look at...
      //
      ITransform transform = null;
      List<ITransform> transformInterfaces = pipeline.findBaseTransforms( transformName );
      for ( int i = 0; i < transformInterfaces.size(); i++ ) {
        ITransform look = transformInterfaces.get( i );
        if ( look.getCopy() == copyNr ) {
          transform = look;
        }
      }
      if ( transform != null ) {

        // Add a listener to the pipeline transform...
        //
        final boolean read = type.equalsIgnoreCase( TYPE_INPUT );
        final boolean written = type.equalsIgnoreCase( TYPE_OUTPUT ) || !read;
        final MetaAndData metaData = new MetaAndData();

        metaData.bufferRowMeta = null;
        metaData.bufferRowData = new ArrayList<Object[]>();

        IRowListener rowListener = new IRowListener() {
          public void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
            if ( read && metaData.bufferRowData.size() < nrLines ) {
              metaData.bufferRowMeta = rowMeta;
              metaData.bufferRowData.add( row );
            }
          }

          public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
            if ( written && metaData.bufferRowData.size() < nrLines ) {
              metaData.bufferRowMeta = rowMeta;
              metaData.bufferRowData.add( row );
            }
          }

          public void errorRowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
          }
        };

        transform.addRowListener( rowListener );

        // Wait until we have enough rows...
        //
        while ( metaData.bufferRowData.size() < nrLines
          && transform.isRunning() && !pipeline.isFinished() && !pipeline.isStopped() ) {

          try {
            Thread.sleep( 100 );
          } catch ( InterruptedException e ) {
            // Ignore
            //
            break;
          }
        }

        // Remove the row listener
        //
        transform.removeRowListener( rowListener );

        // Pass along the rows of data...
        //
        if ( useXML ) {

          // Send the result back as XML
          //
          response.setContentType( "text/xml" );
          response.setCharacterEncoding( Const.XML_ENCODING );
          out.print( XMLHandler.getXMLHeader( Const.XML_ENCODING ) );

          out.println( XMLHandler.openTag( XML_TAG ) );

          if ( metaData.bufferRowMeta != null ) {

            // Row Meta data
            //
            out.println( metaData.bufferRowMeta.getMetaXML() );

            // Nr of lines
            //
            out.println( XMLHandler.addTagValue( "nr_rows", metaData.bufferRowData.size() ) );

            // Rows of data
            //
            for ( int i = 0; i < metaData.bufferRowData.size(); i++ ) {
              Object[] rowData = metaData.bufferRowData.get( i );
              out.println( metaData.bufferRowMeta.getDataXML( rowData ) );
            }
          }

          out.println( XMLHandler.closeTag( XML_TAG ) );

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

            if ( metaData.bufferRowMeta != null ) {
              // Print a header row containing all the field names...
              //
              out.print( "<tr><th>#</th>" );
              for ( IValueMeta valueMeta : metaData.bufferRowMeta.getValueMetaList() ) {
                out.print( "<th>" + valueMeta.getName() + "</th>" );
              }
              out.println( "</tr>" );

              // Now output the data rows...
              //
              for ( int r = 0; r < metaData.bufferRowData.size(); r++ ) {
                Object[] rowData = metaData.bufferRowData.get( r );
                out.print( "<tr>" );
                out.println( "<td>" + ( r + 1 ) + "</td>" );
                for ( int v = 0; v < metaData.bufferRowMeta.size(); v++ ) {
                  IValueMeta valueMeta = metaData.bufferRowMeta.getValueMeta( v );
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
            PKG, "SniffTransformServlet.Log.CoundNotFindSpecTransform", transformName ) ).getXML() );
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
          PKG, "SniffTransformServlet.Log.CoundNotFindSpecPipeline", pipelineName ) ).getXML() );
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
    return "Pipeline Status IHandler";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
