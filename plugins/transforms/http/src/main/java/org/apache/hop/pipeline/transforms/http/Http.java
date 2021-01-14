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

package org.apache.hop.pipeline.transforms.http;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.AuthCache;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Retrieves data from an Http endpoint
 *
 * @author Matt
 * @since 26-apr-2003
 */
public class Http extends BaseTransform<HttpMeta, HttpData> implements ITransform<HttpMeta, HttpData> {

  private static final Class<?> PKG = HttpMeta.class; // For Translator

  public Http( TransformMeta transformMeta, HttpMeta meta, HttpData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private Object[] execHttp( IRowMeta rowMeta, Object[] row ) throws HopException {
    if ( first ) {
      first = false;
      data.argnrs = new int[ meta.getArgumentField().length ];

      for ( int i = 0; i < meta.getArgumentField().length; i++ ) {
        data.argnrs[ i ] = rowMeta.indexOfValue( meta.getArgumentField()[ i ] );
        if ( data.argnrs[ i ] < 0 ) {
          logError( BaseMessages.getString( PKG, "HTTP.Log.ErrorFindingField" ) + meta.getArgumentField()[ i ] + "]" );
          throw new HopTransformException( BaseMessages.getString( PKG, "HTTP.Exception.CouldnotFindField", meta
            .getArgumentField()[ i ] ) );
        }
      }
    }

    return callHttpService( rowMeta, row );
  }

  @VisibleForTesting
  Object[] callHttpService( IRowMeta rowMeta, Object[] rowData ) throws HopException {
    HttpClientManager.HttpClientBuilderFacade clientBuilder = HttpClientManager.getInstance().createBuilder();

    if ( data.realConnectionTimeout > -1 ) {
      clientBuilder.setConnectionTimeout( data.realConnectionTimeout );
    }
    if ( data.realSocketTimeout > -1 ) {
      clientBuilder.setSocketTimeout( data.realSocketTimeout );
    }
    if ( StringUtils.isNotBlank( data.realHttpLogin ) ) {
      clientBuilder.setCredentials( data.realHttpLogin, data.realHttpPassword );
    }
    if ( StringUtils.isNotBlank( data.realProxyHost ) ) {
      clientBuilder.setProxy( data.realProxyHost, data.realProxyPort );
    }

    CloseableHttpClient httpClient = clientBuilder.build();

    // Prepare Http get
    URI uri = null;
    try {
      URIBuilder uriBuilder = constructUrlBuilder( rowMeta, rowData );

      uri = uriBuilder.build();
      HttpGet method = new HttpGet( uri );

      // Add Custom Http headers
      if ( data.useHeaderParameters ) {
        for ( int i = 0; i < data.header_parameters_nrs.length; i++ ) {
          method.addHeader( data.headerParameters[ i ].getName(), data.inputRowMeta.getString( rowData,
            data.header_parameters_nrs[ i ] ) );
          if ( isDebug() ) {
            log.logDebug( BaseMessages.getString( PKG, "HTTPDialog.Log.HeaderValue",
              data.headerParameters[ i ].getName(), data.inputRowMeta
                .getString( rowData, data.header_parameters_nrs[ i ] ) ) );
          }
        }
      }

      Object[] newRow = null;
      if ( rowData != null ) {
        newRow = rowData.clone();
      }
      // Execute request
      CloseableHttpResponse httpResponse = null;
      try {
        // used for calculating the responseTime
        long startTime = System.currentTimeMillis();

        // Preemptive authentication
        if ( StringUtils.isNotBlank( data.realProxyHost ) ) {
          HttpHost target = new HttpHost( data.realProxyHost, data.realProxyPort, "http" );
          // Create AuthCache instance
          AuthCache authCache = new BasicAuthCache();
          // Generate BASIC scheme object and add it to the local
          // auth cache
          BasicScheme basicAuth = new BasicScheme();
          authCache.put( target, basicAuth );
          // Add AuthCache to the execution context
          HttpClientContext localContext = HttpClientContext.create();
          localContext.setAuthCache( authCache );
          httpResponse = httpClient.execute( target, method, localContext );
        } else {
          httpResponse = httpClient.execute( method );
        }
        // calculate the responseTime
        long responseTime = System.currentTimeMillis() - startTime;
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "HTTP.Log.ResponseTime", responseTime, uri ) );
        }
        int statusCode = requestStatusCode( httpResponse );
        // The status code
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "HTTP.Log.ResponseStatusCode", "" + statusCode ) );
        }

        String body;
        switch ( statusCode ) {
          case HttpURLConnection.HTTP_UNAUTHORIZED:
            throw new HopTransformException( BaseMessages
              .getString( PKG, "HTTP.Exception.Authentication", data.realUrl ) );
          case -1:
            throw new HopTransformException( BaseMessages
              .getString( PKG, "HTTP.Exception.IllegalStatusCode", data.realUrl ) );
          case HttpURLConnection.HTTP_NO_CONTENT:
            body = "";
            break;
          default:
            HttpEntity entity = httpResponse.getEntity();
            if ( entity != null ) {
              body = StringUtils.isEmpty( meta.getEncoding() ) ? EntityUtils.toString( entity ) : EntityUtils.toString( entity, meta.getEncoding() );
            } else {
              body = "";
            }
            break;
        }

        Header[] headers = searchForHeaders( httpResponse );

        JSONObject json = new JSONObject();
        for ( Header header : headers ) {
          Object previousValue = json.get( header.getName() );
          if ( previousValue == null ) {
            json.put( header.getName(), header.getValue() );
          } else if ( previousValue instanceof List ) {
            List<String> list = (List<String>) previousValue;
            list.add( header.getValue() );
          } else {
            ArrayList<String> list = new ArrayList<>();
            list.add( (String) previousValue );
            list.add( header.getValue() );
            json.put( header.getName(), list );
          }
        }
        String headerString = json.toJSONString();

        int returnFieldsOffset = rowMeta.size();
        if ( !Utils.isEmpty( meta.getFieldName() ) ) {
          newRow = RowDataUtil.addValueData( newRow, returnFieldsOffset, body );
          returnFieldsOffset++;
        }

        if ( !Utils.isEmpty( meta.getResultCodeFieldName() ) ) {
          newRow = RowDataUtil.addValueData( newRow, returnFieldsOffset, new Long( statusCode ) );
          returnFieldsOffset++;
        }
        if ( !Utils.isEmpty( meta.getResponseTimeFieldName() ) ) {
          newRow = RowDataUtil.addValueData( newRow, returnFieldsOffset, new Long( responseTime ) );
          returnFieldsOffset++;
        }
        if ( !Utils.isEmpty( meta.getResponseHeaderFieldName() ) ) {
          newRow = RowDataUtil.addValueData( newRow, returnFieldsOffset, headerString );
        }

      } finally {
        if ( httpResponse != null ) {
          httpResponse.close();
        }
        // Release current connection to the connection pool once you are done
        method.releaseConnection();
      }
      return newRow;
    } catch ( UnknownHostException uhe ) {
      throw new HopException( BaseMessages.getString( PKG, "HTTP.Error.UnknownHostException", uhe.getMessage() ) );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "HTTP.Log.UnableGetResult", uri ), e );
    }
  }

  private URIBuilder constructUrlBuilder( IRowMeta outputRowMeta, Object[] row ) throws HopValueException,
    HopException {
    URIBuilder uriBuilder;
    try {
      String baseUrl = data.realUrl;
      if ( meta.isUrlInField() ) {
        // get dynamic url
        baseUrl = outputRowMeta.getString( row, data.indexOfUrlField );
      }

      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "HTTP.Log.Connecting", baseUrl ) );
      }

      uriBuilder = new URIBuilder( baseUrl ); // the base URL with variable substitution
      List<NameValuePair> queryParams = uriBuilder.getQueryParams();

      for ( int i = 0; i < data.argnrs.length; i++ ) {
        String key = meta.getArgumentParameter()[ i ];
        String value = outputRowMeta.getString( row, data.argnrs[ i ] );
        BasicNameValuePair basicNameValuePair = new BasicNameValuePair( key, value );
        queryParams.add( basicNameValuePair );
      }
      if ( !queryParams.isEmpty() ) {
        uriBuilder.setParameters( queryParams );
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "HTTP.Log.UnableCreateUrl" ), e );
    }
    return uriBuilder;
  }

  protected int requestStatusCode( HttpResponse httpResponse ) {
    return httpResponse.getStatusLine().getStatusCode();
  }

  protected Header[] searchForHeaders( CloseableHttpResponse response ) {
    return response.getAllHeaders();
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      data.outputRowMeta = getInputRowMeta().clone();
      data.inputRowMeta = getInputRowMeta();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      if ( meta.isUrlInField() ) {
        if ( Utils.isEmpty( meta.getUrlField() ) ) {
          logError( BaseMessages.getString( PKG, "HTTP.Log.NoField" ) );
          throw new HopException( BaseMessages.getString( PKG, "HTTP.Log.NoField" ) );
        }

        // cache the position of the field
        if ( data.indexOfUrlField < 0 ) {
          String realUrlfieldName = resolve( meta.getUrlField() );
          data.indexOfUrlField = getInputRowMeta().indexOfValue( realUrlfieldName );
          if ( data.indexOfUrlField < 0 ) {
            // The field is unreachable !
            logError( BaseMessages.getString( PKG, "HTTP.Log.ErrorFindingField", realUrlfieldName ) );
            throw new HopException( BaseMessages.getString( PKG, "HTTP.Exception.ErrorFindingField",
              realUrlfieldName ) );
          }
        }
      } else {
        data.realUrl = resolve( meta.getUrl() );
      }

      // check for headers
      int nrHeaders = meta.getHeaderField().length;
      if ( nrHeaders > 0 ) {
        data.useHeaderParameters = true;
      }

      data.header_parameters_nrs = new int[ nrHeaders ];
      data.headerParameters = new NameValuePair[ nrHeaders ];

      // get the headers
      for ( int i = 0; i < nrHeaders; i++ ) {
        int fieldIndex = data.inputRowMeta.indexOfValue( meta.getHeaderField()[ i ] );
        if ( fieldIndex < 0 ) {
          logError( BaseMessages.getString( PKG,
            "HTTP.Exception.ErrorFindingField" ) + meta.getHeaderField()[ i ] + "]" );
          throw new HopTransformException( BaseMessages.getString( PKG, "HTTP.Exception.ErrorFindingField", meta
            .getHeaderField()[ i ] ) );
        }

        data.header_parameters_nrs[ i ] = fieldIndex;
        data.headerParameters[ i ] =
          new BasicNameValuePair( resolve( meta.getHeaderParameter()[ i ] ),
            data.outputRowMeta.getString( r,
              data.header_parameters_nrs[ i ] ) );
      }

    } // end if first

    try {
      Object[] outputRowData = execHttp( getInputRowMeta(), r ); // add new values to the row
      putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);

      if ( checkFeedback( getLinesRead() ) ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "HTTP.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "HTTP.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "HTTP001" );
      }
    }

    return true;
  }

  public boolean init(){

    if ( super.init() ) {
      // get authentication settings once
      data.realProxyHost = resolve( meta.getProxyHost() );
      data.realProxyPort = Const.toInt( resolve( meta.getProxyPort() ), 8080 );
      data.realHttpLogin = resolve( meta.getHttpLogin() );
      data.realHttpPassword = Utils.resolvePassword( variables, meta.getHttpPassword() );

      data.realSocketTimeout = Const.toInt( resolve( meta.getSocketTimeout() ), -1 );
      data.realConnectionTimeout = Const.toInt( resolve( meta.getSocketTimeout() ), -1 );

      return true;
    }
    return false;
  }
}
