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

package org.apache.hop.trans.steps.http;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 10-dec-2006
 *
 */
public class HTTPMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = HTTPMeta.class; // for i18n purposes, needed by Translator2!!

  // the timeout for waiting for data (milliseconds)
  public static final int DEFAULT_SOCKET_TIMEOUT = 10000;

  // the timeout until a connection is established (milliseconds)
  public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

  // the time to wait till a connection is closed (milliseconds)? -1 is no not close.
  public static final int DEFAULT_CLOSE_CONNECTIONS_TIME = -1;

  private String socketTimeout;
  private String connectionTimeout;
  private String closeIdleConnectionsTime;

  /**
   * URL / service to be called
   */
  private String url;

  /**
   * function arguments : fieldname
   */
  private String[] argumentField;

  /**
   * IN / OUT / INOUT
   */
  private String[] argumentParameter;

  /**
   * function result: new value name
   */
  private String fieldName;

  /**
   * The encoding to use for retrieval of the data
   */
  private String encoding;

  private boolean urlInField;

  private String urlField;

  private String proxyHost;

  private String proxyPort;

  private String httpLogin;

  private String httpPassword;

  private String resultCodeFieldName;
  private String responseTimeFieldName;
  private String responseHeaderFieldName;

  private String[] headerParameter;
  private String[] headerField;

  public HTTPMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the connectionTimeout.
   */
  public String getConnectionTimeout() {
    return connectionTimeout;
  }

  /**
   * @param connectionTimeout The connectionTimeout to set.
   */
  public void setConnectionTimeout( String connectionTimeout ) {
    this.connectionTimeout = connectionTimeout;
  }

  /**
   * @return Returns the closeIdleConnectionsTime.
   */
  public String getCloseIdleConnectionsTime() {
    return closeIdleConnectionsTime;
  }

  /**
   * @param closeIdleConnectionsTime The connectionTimeout to set.
   */
  public void setCloseIdleConnectionsTime( String closeIdleConnectionsTime ) {
    this.closeIdleConnectionsTime = closeIdleConnectionsTime;
  }

  /**
   * @return Returns the socketTimeout.
   */
  public String getSocketTimeout() {
    return socketTimeout;
  }

  /**
   * @param socketTimeout The socketTimeout to set.
   */
  public void setSocketTimeout( String socketTimeout ) {
    this.socketTimeout = socketTimeout;
  }

  /**
   * @return Returns the argument.
   */
  public String[] getArgumentField() {
    return argumentField;
  }

  /**
   * @param argument The argument to set.
   */
  public void setArgumentField( String[] argument ) {
    this.argumentField = argument;
  }

  /**
   * @return Returns the headerFields.
   */

  public String[] getHeaderField() {

    return headerField;
  }

  /**
   * @param headerField The headerField to set.
   */

  public void setHeaderField( String[] headerField ) {

    this.headerField = headerField;
  }

  /**
   * @return Returns the argumentDirection.
   */
  public String[] getArgumentParameter() {
    return argumentParameter;
  }

  /**
   * @param argumentDirection The argumentDirection to set.
   */
  public void setArgumentParameter( String[] argumentDirection ) {
    this.argumentParameter = argumentDirection;
  }

  /**
   * @return Returns the headerParameter.
   */
  public String[] getHeaderParameter() {
    return headerParameter;
  }

  /**
   * @param headerParameter The headerParameter to set.
   */
  public void setHeaderParameter( String[] headerParameter ) {
    this.headerParameter = headerParameter;
  }

  /**
   * @return Returns the procedure.
   */
  public String getUrl() {
    return url;
  }

  /**
   * @param procedure The procedure to set.
   */
  public void setUrl( String procedure ) {
    this.url = procedure;
  }

  /**
   * @return Returns the resultName.
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param resultName The resultName to set.
   */
  public void setFieldName( String resultName ) {
    this.fieldName = resultName;
  }

  /**
   * @return Is the url coded in a field?
   */
  public boolean isUrlInField() {
    return urlInField;
  }

  /**
   * @param urlInField Is the url coded in a field?
   */
  public void setUrlInField( boolean urlInField ) {
    this.urlInField = urlInField;
  }

  /**
   * @return The field name that contains the url.
   */
  public String getUrlField() {
    return urlField;
  }

  /**
   * @param urlField name of the field that contains the url
   */
  public void setUrlField( String urlField ) {
    this.urlField = urlField;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  public void allocate( int nrargs, int nrqueryparams ) {
    argumentField = new String[ nrargs ];
    argumentParameter = new String[ nrargs ];
    headerField = new String[ nrqueryparams ];
    headerParameter = new String[ nrqueryparams ];
  }

  public Object clone() {
    HTTPMeta retval = (HTTPMeta) super.clone();
    int nrargs = argumentField.length;
    int nrheaderparams = headerField.length;

    retval.allocate( nrargs, nrheaderparams );

    System.arraycopy( argumentField, 0, retval.argumentField, 0, nrargs );
    System.arraycopy( argumentParameter, 0, retval.argumentParameter, 0, nrargs );
    System.arraycopy( headerField, 0, retval.headerField, 0, nrheaderparams );
    System.arraycopy( headerParameter, 0, retval.headerParameter, 0, nrheaderparams );

    return retval;
  }

  public void setDefault() {
    socketTimeout = String.valueOf( DEFAULT_SOCKET_TIMEOUT );
    connectionTimeout = String.valueOf( DEFAULT_CONNECTION_TIMEOUT );
    closeIdleConnectionsTime = String.valueOf( DEFAULT_CLOSE_CONNECTIONS_TIME );
    int i;
    int nrargs;
    int nrquery;
    nrargs = 0;
    nrquery = 0;

    allocate( nrargs, nrquery );

    for ( i = 0; i < nrargs; i++ ) {
      argumentField[ i ] = "arg" + i;
      argumentParameter[ i ] = "arg";
    }

    for ( i = 0; i < nrquery; i++ ) {
      headerField[ i ] = "header" + i;
      headerParameter[ i ] = "header";
    }

    fieldName = "result";
    resultCodeFieldName = "";
    responseTimeFieldName = "";
    responseHeaderFieldName = "";
    encoding = "UTF-8";
  }

  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    if ( !Utils.isEmpty( fieldName ) ) {
      ValueMetaInterface v = new ValueMetaString( fieldName );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
    if ( !Utils.isEmpty( resultCodeFieldName ) ) {
      ValueMetaInterface v =
        new ValueMetaInteger( space.environmentSubstitute( resultCodeFieldName ) );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
    if ( !Utils.isEmpty( responseTimeFieldName ) ) {
      ValueMetaInterface v =
        new ValueMetaInteger( space.environmentSubstitute( responseTimeFieldName ) );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
    String headerFieldName = space.environmentSubstitute( responseHeaderFieldName );
    if ( !Utils.isEmpty( headerFieldName ) ) {
      ValueMetaInterface v =
        new ValueMetaString( headerFieldName );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "url", url ) );
    retval.append( "    " + XMLHandler.addTagValue( "urlInField", urlInField ) );
    retval.append( "    " + XMLHandler.addTagValue( "urlField", urlField ) );
    retval.append( "    " + XMLHandler.addTagValue( "encoding", encoding ) );
    retval.append( "    " + XMLHandler.addTagValue( "httpLogin", httpLogin ) );
    retval.append( "    "
      + XMLHandler.addTagValue( "httpPassword", Encr.encryptPasswordIfNotUsingVariables( httpPassword ) ) );
    retval.append( "    " + XMLHandler.addTagValue( "proxyHost", proxyHost ) );
    retval.append( "    " + XMLHandler.addTagValue( "proxyPort", proxyPort ) );
    retval.append( "    " + XMLHandler.addTagValue( "socketTimeout", socketTimeout ) );
    retval.append( "    " + XMLHandler.addTagValue( "connectionTimeout", connectionTimeout ) );
    retval.append( "    " + XMLHandler.addTagValue( "closeIdleConnectionsTime", closeIdleConnectionsTime ) );

    retval.append( "    <lookup>" ).append( Const.CR );

    for ( int i = 0; i < argumentField.length; i++ ) {
      retval.append( "      <arg>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", argumentField[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "parameter", argumentParameter[ i ] ) );
      retval.append( "      </arg>" ).append( Const.CR );
    }
    for ( int i = 0; i < headerField.length; i++ ) {
      retval.append( "      <header>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", headerField[ i ] ) );
      retval.append( "        " + XMLHandler.addTagValue( "parameter", headerParameter[ i ] ) );
      retval.append( "      </header>" + Const.CR );
    }

    retval.append( "    </lookup>" ).append( Const.CR );

    retval.append( "    <result>" ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.addTagValue( "name", fieldName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "code", resultCodeFieldName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "response_time", responseTimeFieldName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "response_header", responseHeaderFieldName ) );
    retval.append( "    </result>" ).append( Const.CR );

    return retval.toString();
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      int nrargs;

      url = XMLHandler.getTagValue( stepnode, "url" );
      urlInField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "urlInField" ) );
      urlField = XMLHandler.getTagValue( stepnode, "urlField" );
      encoding = XMLHandler.getTagValue( stepnode, "encoding" );
      httpLogin = XMLHandler.getTagValue( stepnode, "httpLogin" );
      httpPassword = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode, "httpPassword" ) );
      proxyHost = XMLHandler.getTagValue( stepnode, "proxyHost" );
      proxyPort = XMLHandler.getTagValue( stepnode, "proxyPort" );

      socketTimeout = XMLHandler.getTagValue( stepnode, "socketTimeout" );
      connectionTimeout = XMLHandler.getTagValue( stepnode, "connectionTimeout" );
      closeIdleConnectionsTime = XMLHandler.getTagValue( stepnode, "closeIdleConnectionsTime" );

      Node lookup = XMLHandler.getSubNode( stepnode, "lookup" );
      nrargs = XMLHandler.countNodes( lookup, "arg" );

      int nrheaders = XMLHandler.countNodes( lookup, "header" );
      allocate( nrargs, nrheaders );

      for ( int i = 0; i < nrargs; i++ ) {
        Node anode = XMLHandler.getSubNodeByNr( lookup, "arg", i );

        argumentField[ i ] = XMLHandler.getTagValue( anode, "name" );
        argumentParameter[ i ] = XMLHandler.getTagValue( anode, "parameter" );
      }

      for ( int i = 0; i < nrheaders; i++ ) {
        Node anode = XMLHandler.getSubNodeByNr( lookup, "header", i );
        headerField[ i ] = XMLHandler.getTagValue( anode, "name" );
        headerParameter[ i ] = XMLHandler.getTagValue( anode, "parameter" );
      }

      fieldName = XMLHandler.getTagValue( stepnode, "result", "name" );
      resultCodeFieldName = XMLHandler.getTagValue( stepnode, "result", "code" );
      responseTimeFieldName = XMLHandler.getTagValue( stepnode, "result", "response_time" );
      responseHeaderFieldName = XMLHandler.getTagValue( stepnode, "result", "response_header" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "HTTPMeta.Exception.UnableToReadStepInfo" ), e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "HTTPMeta.CheckResult.ReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "HTTPMeta.CheckResult.NoInpuReceived" ), stepMeta );
      remarks.add( cr );
    }
    // check Url
    if ( urlInField ) {
      if ( Utils.isEmpty( urlField ) ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "HTTPMeta.CheckResult.UrlfieldMissing" ), stepMeta );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "HTTPMeta.CheckResult.UrlfieldOk" ), stepMeta );
      }

    } else {
      if ( Utils.isEmpty( url ) ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "HTTPMeta.CheckResult.UrlMissing" ), stepMeta );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "HTTPMeta.CheckResult.UrlOk" ), stepMeta );
      }
    }
    remarks.add( cr );
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new HTTP( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new HTTPData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * @return the encoding
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding the encoding to set
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * Setter
   *
   * @param proxyHost
   */
  public void setProxyHost( String proxyHost ) {
    this.proxyHost = proxyHost;
  }

  /**
   * Getter
   *
   * @return
   */
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * Setter
   *
   * @param proxyPort
   */
  public void setProxyPort( String proxyPort ) {
    this.proxyPort = proxyPort;
  }

  /**
   * Getter
   *
   * @return
   */
  public String getProxyPort() {
    return this.proxyPort;
  }

  /**
   * Setter
   *
   * @param httpLogin
   */
  public void setHttpLogin( String httpLogin ) {
    this.httpLogin = httpLogin;
  }

  /**
   * Getter
   *
   * @return
   */
  public String getHttpLogin() {
    return httpLogin;
  }

  /**
   * Setter
   *
   * @param httpPassword
   */
  public void setHttpPassword( String httpPassword ) {
    this.httpPassword = httpPassword;
  }

  /**
   * @return
   */
  public String getHttpPassword() {
    return httpPassword;
  }

  /**
   * @return the resultCodeFieldName
   */
  public String getResultCodeFieldName() {
    return resultCodeFieldName;
  }

  /**
   * @param resultCodeFieldName the resultCodeFieldName to set
   */
  public void setResultCodeFieldName( String resultCodeFieldName ) {
    this.resultCodeFieldName = resultCodeFieldName;
  }

  public String getResponseTimeFieldName() {
    return responseTimeFieldName;
  }

  public void setResponseTimeFieldName( String responseTimeFieldName ) {
    this.responseTimeFieldName = responseTimeFieldName;
  }

  public String getResponseHeaderFieldName() {
    return responseHeaderFieldName;
  }

  public void setResponseHeaderFieldName( String responseHeaderFieldName ) {
    this.responseHeaderFieldName = responseHeaderFieldName;
  }

}
