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

package org.apache.hop.job.entries.webserviceavailable;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * This defines a webservice available job entry.
 *
 * @author Samatar
 * @since 05-11-2009
 */

public class JobEntryWebServiceAvailable extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryWebServiceAvailable.class; // for i18n purposes, needed by Translator2!!

  private String url;
  private String connectTimeOut;
  private String readTimeOut;

  public JobEntryWebServiceAvailable( String n ) {
    super( n, "" );
    url = null;
    connectTimeOut = "0";
    readTimeOut = "0";
  }

  public JobEntryWebServiceAvailable() {
    this( "" );
  }

  public Object clone() {
    JobEntryWebServiceAvailable je = (JobEntryWebServiceAvailable) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "url", url ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "connectTimeOut", connectTimeOut ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "readTimeOut", readTimeOut ) );
    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      url = XMLHandler.getTagValue( entrynode, "url" );
      connectTimeOut = XMLHandler.getTagValue( entrynode, "connectTimeOut" );
      readTimeOut = XMLHandler.getTagValue( entrynode, "readTimeOut" );
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobEntryWebServiceAvailable.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node" ), xe );
    }
  }

  public void setURL( String url ) {
    this.url = url;
  }

  public String getURL() {
    return url;
  }

  public void setConnectTimeOut( String timeout ) {
    this.connectTimeOut = timeout;
  }

  public String getConnectTimeOut() {
    return connectTimeOut;
  }

  public void setReadTimeOut( String timeout ) {
    this.readTimeOut = timeout;
  }

  public String getReadTimeOut() {
    return readTimeOut;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );

    String realURL = environmentSubstitute( getURL() );

    if ( !Utils.isEmpty( realURL ) ) {
      int connectTimeOut = Const.toInt( environmentSubstitute( getConnectTimeOut() ), 0 );
      int readTimeOut = Const.toInt( environmentSubstitute( getReadTimeOut() ), 0 );
      InputStream in = null;
      try {

        URLConnection conn = new URL( realURL ).openConnection();
        conn.setConnectTimeout( connectTimeOut );
        conn.setReadTimeout( readTimeOut );
        in = conn.getInputStream();
        // Web service is available
        result.setResult( true );
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        String message =
          BaseMessages
            .getString( PKG, "JobEntryWebServiceAvailable.ERROR_0004_Exception", realURL, e.toString() );
        logError( message );
        result.setLogText( message );
      } finally {
        if ( in != null ) {
          try {
            in.close();
          } catch ( Exception e ) { /* Ignore */
          }
        }
      }
    } else {
      result.setNrErrors( 1 );
      String message = BaseMessages.getString( PKG, "JobEntryWebServiceAvailable.ERROR_0005_No_URL_Defined" );
      logError( message );
      result.setLogText( message );
    }

    return result;
  }

  public boolean evaluates() {
    return true;
  }

}
