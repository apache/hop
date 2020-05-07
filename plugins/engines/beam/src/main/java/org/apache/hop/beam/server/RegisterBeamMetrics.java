package org.apache.hop.beam.server;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.www.BaseHttpServlet;
import org.apache.hop.www.IHopServerPlugin;
import org.apache.hop.www.SlaveServerPipelineStatus;
import org.apache.hop.www.WebResult;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

@HopServerServlet(
  id = "registerBeamMetrics",
  name = "Register Beam Metrics",
  description = "Captures Apache Beam Metrics regarding a running transformation"
)
public class RegisterBeamMetrics extends BaseHttpServlet implements IHopServerPlugin {

  private static final long serialVersionUIDL = 348324987293472947L;

  public static final String CONTEXT_PATH = "/kettle/registerBeamMetrics";

  public RegisterBeamMetrics() {
  }

  public String toString() {
    return "Register Beam Metrics";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override
  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {

    if ( isJettyMode() && !request.getRequestURI().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    if ( log.isDebug() ) {
      logDebug( "Registration of Apache Beam Metrics" );
    }

    // The Object ID
    //
    String carteObjectId = request.getParameter( "id" ); // the carte object id

    // Transformation name
    //
    String pipeline = request.getParameter( "pipeline" ); // the name of the transformation

    // Internal Job ID
    //
    String internalJobId = request.getParameter( "internalJobId" ); // the Id of the Spark/Flink job

    // Update date
    //
    Date updateDate = new Date();

    PrintWriter out = response.getWriter();
    BufferedReader in = request.getReader();

    WebResult webResult = new WebResult( WebResult.STRING_OK, "registration success", "" );

    try {

      // First read the complete SlaveServerTransStatus object XML in memory from the request
      //
      StringBuilder xml = new StringBuilder( request.getContentLength() );
      int c;
      while ( ( c = in.read() ) != -1 ) {
        xml.append( (char) c );
      }

      SlaveServerPipelineStatus transStatus = SlaveServerPipelineStatus.fromXml( xml.toString() );

      MetricsRegistrationQueue registry = MetricsRegistrationQueue.getInstance();
      BeamMetricsEntry entry = new BeamMetricsEntry( carteObjectId, pipeline, internalJobId, updateDate, transStatus );
      registry.addNodeRegistryEntry( entry );

      response.setContentType( "text/xml" );
      response.setStatus( HttpServletResponse.SC_OK );
      response.setCharacterEncoding( Const.XML_ENCODING );
      out.println( XmlHandler.getXmlHeader() );
    } catch ( Exception e ) {
      webResult.setResult( WebResult.STRING_ERROR );
      webResult.setMessage( Const.getStackTracker( e ) );
    }
    out.println( webResult.getXml() );
  }
}