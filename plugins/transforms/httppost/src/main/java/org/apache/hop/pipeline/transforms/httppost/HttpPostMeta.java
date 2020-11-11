/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.httppost;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 15-jan-2009
 *
 */

@Transform(
        id = "HttpPost",
        image = "httppost.svg",
        i18nPackageName = "i18n:org.apache.hop.pipeline.transforms.httppost",
        name = "BaseTransform.TypeLongDesc.HTTPPOST",
        description = "BaseTransform.TypeTooltipDesc.HTTPPOST",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/httppost.html"
)
public class HttpPostMeta extends BaseTransformMeta implements ITransformMeta<HttpPost, HttpPostData> {
  private static final Class<?> PKG = HttpPostMeta.class; // Needed by Translator

  // the timeout for waiting for data (milliseconds)
  public static final int DEFAULT_SOCKET_TIMEOUT = 10000;

  // the timeout until a connection is established (milliseconds)
  public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

  // the time to wait till a connection is closed (milliseconds)? -1 is no not close.
  public static final int DEFAULT_CLOSE_CONNECTIONS_TIME = -1;

  public static final String DEFAULT_ENCODING = "UTF-8";

  private String socketTimeout;
  private String connectionTimeout;
  private String closeIdleConnectionsTime;

  private static final String YES = "Y";

  /**
   * URL / service to be called
   */
  private String url;

  /**
   * function arguments : fieldname
   */
  private String[] argumentField;

  /**
   * function query field : queryField
   */
  private String[] queryField;

  /**
   * IN / OUT / INOUT
   */
  private String[] argumentParameter;
  private boolean[] argumentHeader;

  private String[] queryParameter;

  /**
   * function result: new value name
   */
  private String fieldName;
  private String resultCodeFieldName;
  private String responseHeaderFieldName;
  private boolean urlInField;

  private String urlField;

  private String requestEntity;

  private String encoding;

  private boolean postafile;

  private String proxyHost;

  private String proxyPort;

  private String httpLogin;

  private String httpPassword;

  private String responseTimeFieldName;

  public HttpPostMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding( String encoding ) {
    this.encoding = encoding;
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
   * @return Returns the argument.
   */
  public String[] getQueryField() {
    return queryField;
  }

  /**
   * @param queryfield The queryfield to set.
   */
  public void setQueryField( String[] queryfield ) {
    this.queryField = queryfield;
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
   * @return Returns the queryParameter.
   */
  public String[] getQueryParameter() {
    return queryParameter;
  }

  /**
   * @param queryParameter The queryParameter to set.
   */
  public void setQueryParameter( String[] queryParameter ) {
    this.queryParameter = queryParameter;
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
   * @return Is the url coded in a field?
   */
  public boolean isUrlInField() {
    return urlInField;
  }

  public boolean isPostAFile() {
    return postafile;
  }

  public void setPostAFile( boolean postafile ) {
    this.postafile = postafile;
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

  /**
   * @param requestEntity the requestEntity to set
   */
  public void setRequestEntity( String requestEntity ) {
    this.requestEntity = requestEntity;
  }

  /**
   * @return requestEntity
   */
  public String getRequestEntity() {
    return requestEntity;
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

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode, metadataProvider );
  }

  public void allocate( int nrargs ) {
    argumentField = new String[ nrargs ];
    argumentParameter = new String[ nrargs ];
    argumentHeader = new boolean[ nrargs ];
  }

  public void allocateQuery( int nrqueryparams ) {
    queryField = new String[ nrqueryparams ];
    queryParameter = new String[ nrqueryparams ];
  }

  public Object clone() {
    HttpPostMeta retval = (HttpPostMeta) super.clone();

    int nrargs = argumentField.length;
    retval.allocate( nrargs );
    System.arraycopy( argumentField, 0, retval.argumentField, 0, nrargs );
    System.arraycopy( argumentParameter, 0, retval.argumentParameter, 0, nrargs );
    System.arraycopy( argumentHeader, 0, retval.argumentHeader, 0, nrargs );


    int nrqueryparams = queryField.length;
    retval.allocateQuery( nrqueryparams );
    System.arraycopy( queryField, 0, retval.queryField, 0, nrqueryparams );
    System.arraycopy( queryParameter, 0, retval.queryParameter, 0, nrqueryparams );

    return retval;
  }

  public void setDefault() {
    int i;
    int nrargs;
    nrargs = 0;
    allocate( nrargs );
    for ( i = 0; i < nrargs; i++ ) {
      argumentField[ i ] = "arg" + i;
      argumentParameter[ i ] = "arg";
      argumentHeader[ i ] = false;
    }

    int nrquery;
    nrquery = 0;
    allocateQuery( nrquery );
    for ( i = 0; i < nrquery; i++ ) {
      queryField[ i ] = "query" + i;
      queryParameter[ i ] = "query";
    }

    fieldName = "result";
    resultCodeFieldName = "";
    responseTimeFieldName = "";
    responseHeaderFieldName = "";
    encoding = DEFAULT_ENCODING;
    postafile = false;

    socketTimeout = String.valueOf( DEFAULT_SOCKET_TIMEOUT );
    connectionTimeout = String.valueOf( DEFAULT_CONNECTION_TIMEOUT );
    closeIdleConnectionsTime = String.valueOf( DEFAULT_CLOSE_CONNECTIONS_TIME );
  }

  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    if ( !Utils.isEmpty( fieldName ) ) {
      IValueMeta v = new ValueMetaString( variables.environmentSubstitute( fieldName ) );
      inputRowMeta.addValueMeta( v );
    }

    if ( !Utils.isEmpty( resultCodeFieldName ) ) {
      IValueMeta v =
        new ValueMetaInteger( variables.environmentSubstitute( resultCodeFieldName ) );
      inputRowMeta.addValueMeta( v );
    }
    if ( !Utils.isEmpty( responseTimeFieldName ) ) {
      IValueMeta v =
        new ValueMetaInteger( variables.environmentSubstitute( responseTimeFieldName ) );
      inputRowMeta.addValueMeta( v );
    }
    String headerFieldName = variables.environmentSubstitute( responseHeaderFieldName );
    if ( !Utils.isEmpty( headerFieldName ) ) {
      IValueMeta v =
        new ValueMetaString( headerFieldName );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XmlHandler.addTagValue( "postafile", postafile ) );
    retval.append( "    " + XmlHandler.addTagValue( "encoding", encoding ) );
    retval.append( "    " + XmlHandler.addTagValue( "url", url ) );
    retval.append( "    " + XmlHandler.addTagValue( "urlInField", urlInField ) );
    retval.append( "    " + XmlHandler.addTagValue( "urlField", urlField ) );
    retval.append( "    " + XmlHandler.addTagValue( "requestEntity", requestEntity ) );
    retval.append( "    " + XmlHandler.addTagValue( "httpLogin", httpLogin ) );
    retval.append( "    "
      + XmlHandler.addTagValue( "httpPassword", Encr.encryptPasswordIfNotUsingVariables( httpPassword ) ) );
    retval.append( "    " + XmlHandler.addTagValue( "proxyHost", proxyHost ) );
    retval.append( "    " + XmlHandler.addTagValue( "proxyPort", proxyPort ) );
    retval.append( "    " + XmlHandler.addTagValue( "socketTimeout", socketTimeout ) );
    retval.append( "    " + XmlHandler.addTagValue( "connectionTimeout", connectionTimeout ) );
    retval.append( "    " + XmlHandler.addTagValue( "closeIdleConnectionsTime", closeIdleConnectionsTime ) );

    retval.append( "    <lookup>" + Const.CR );

    for ( int i = 0; i < argumentField.length; i++ ) {
      retval.append( "      <arg>" + Const.CR );
      retval.append( "        " + XmlHandler.addTagValue( "name", argumentField[ i ] ) );
      retval.append( "        " + XmlHandler.addTagValue( "parameter", argumentParameter[ i ] ) );
      retval.append( "        " + XmlHandler.addTagValue( "header", argumentHeader[ i ], false ) );
      retval.append( "        </arg>" + Const.CR );
    }
    for ( int i = 0; i < queryField.length; i++ ) {
      retval.append( "      <query>" + Const.CR );
      retval.append( "        " + XmlHandler.addTagValue( "name", queryField[ i ] ) );
      retval.append( "        " + XmlHandler.addTagValue( "parameter", queryParameter[ i ] ) );
      retval.append( "        </query>" + Const.CR );
    }

    retval.append( "      </lookup>" + Const.CR );

    retval.append( "    <result>" + Const.CR );
    retval.append( "      " + XmlHandler.addTagValue( "name", fieldName ) );
    retval.append( "      " + XmlHandler.addTagValue( "code", resultCodeFieldName ) );
    retval.append( "      " + XmlHandler.addTagValue( "response_time", responseTimeFieldName ) );
    retval.append( "      " + XmlHandler.addTagValue( "response_header", responseHeaderFieldName ) );
    retval.append( "      </result>" + Const.CR );

    return retval.toString();
  }

  private void readData( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      postafile = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "postafile" ) );
      encoding = XmlHandler.getTagValue( transformNode, "encoding" );
      url = XmlHandler.getTagValue( transformNode, "url" );
      urlInField = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "urlInField" ) );
      urlField = XmlHandler.getTagValue( transformNode, "urlField" );
      requestEntity = XmlHandler.getTagValue( transformNode, "requestEntity" );
      httpLogin = XmlHandler.getTagValue( transformNode, "httpLogin" );
      httpPassword = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( transformNode, "httpPassword" ) );
      proxyHost = XmlHandler.getTagValue( transformNode, "proxyHost" );
      proxyPort = XmlHandler.getTagValue( transformNode, "proxyPort" );

      socketTimeout = XmlHandler.getTagValue( transformNode, "socketTimeout" );
      connectionTimeout = XmlHandler.getTagValue( transformNode, "connectionTimeout" );
      closeIdleConnectionsTime = XmlHandler.getTagValue( transformNode, "closeIdleConnectionsTime" );

      Node lookup = XmlHandler.getSubNode( transformNode, "lookup" );

      int nrargs = XmlHandler.countNodes( lookup, "arg" );
      allocate( nrargs );
      for ( int i = 0; i < nrargs; i++ ) {
        Node anode = XmlHandler.getSubNodeByNr( lookup, "arg", i );
        argumentField[ i ] = XmlHandler.getTagValue( anode, "name" );
        argumentParameter[ i ] = XmlHandler.getTagValue( anode, "parameter" );
        argumentHeader[ i ] = YES.equalsIgnoreCase( XmlHandler.getTagValue( anode, "header" ) );
      }

      int nrquery = XmlHandler.countNodes( lookup, "query" );
      allocateQuery( nrquery );

      for ( int i = 0; i < nrquery; i++ ) {
        Node anode = XmlHandler.getSubNodeByNr( lookup, "query", i );
        queryField[ i ] = XmlHandler.getTagValue( anode, "name" );
        queryParameter[ i ] = XmlHandler.getTagValue( anode, "parameter" );
      }

      fieldName = XmlHandler.getTagValue( transformNode, "result", "name" ); // Optional, can be null
      resultCodeFieldName = XmlHandler.getTagValue( transformNode, "result", "code" ); // Optional, can be null
      responseTimeFieldName = XmlHandler.getTagValue( transformNode, "result", "response_time" ); // Optional, can be null
      responseHeaderFieldName =
        XmlHandler.getTagValue( transformNode, "result", "response_header" ); // Optional, can be null
    } catch ( Exception e ) {
      throw new HopXmlException(
        BaseMessages.getString( PKG, "HTTPPOSTMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    CheckResult cr;

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "HTTPPOSTMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "HTTPPOSTMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

    // check Url
    if ( urlInField ) {
      if ( Utils.isEmpty( urlField ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "HTTPPOSTMeta.CheckResult.UrlfieldMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "HTTPPOSTMeta.CheckResult.UrlfieldOk" ), transformMeta );
      }

    } else {
      if ( Utils.isEmpty( url ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "HTTPPOSTMeta.CheckResult.UrlMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "HTTPPOSTMeta.CheckResult.UrlOk" ), transformMeta );
      }
    }
    remarks.add( cr );

  }

  public HttpPost createTransform( TransformMeta transformMeta, HttpPostData data, int cnr,
                                   PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new HttpPost( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public HttpPostData getTransformData() {
    return new HttpPostData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * @return the argumentHeader
   */
  public boolean[] getArgumentHeader() {
    return argumentHeader;
  }

  /**
   * @param argumentHeader the argumentHeader to set
   */
  public void setArgumentHeader( boolean[] argumentHeader ) {
    this.argumentHeader = argumentHeader;
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

  /**
   * ISetter
   *
   * @param proxyHost
   */
  public void setProxyHost( String proxyHost ) {
    this.proxyHost = proxyHost;
  }

  /**
   * IGetter
   *
   * @return
   */
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * ISetter
   *
   * @param proxyPort
   */
  public void setProxyPort( String proxyPort ) {
    this.proxyPort = proxyPort;
  }

  /**
   * IGetter
   *
   * @return
   */
  public String getProxyPort() {
    return this.proxyPort;
  }

  /**
   * ISetter
   *
   * @param httpLogin
   */
  public void setHttpLogin( String httpLogin ) {
    this.httpLogin = httpLogin;
  }

  /**
   * IGetter
   *
   * @return
   */
  public String getHttpLogin() {
    return httpLogin;
  }

  /**
   * ISetter
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
