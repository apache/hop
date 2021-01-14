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

package org.apache.hop.pipeline.transforms.rest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.uri.UriComponent;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.config.ApacheHttpClient4Config;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.json.simple.JSONObject;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;


public class Rest extends BaseTransform<RestMeta, RestData> implements ITransform<RestMeta, RestData> {
  private static final Class<?> PKG = RestMeta.class; // For Translator

  public Rest( TransformMeta transformMeta, RestMeta meta, RestData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /* for unit test*/
  MultivaluedMapImpl createMultivalueMap( String paramName, String paramValue ) {
    MultivaluedMapImpl queryParams = new MultivaluedMapImpl();
    queryParams.add( paramName, UriComponent.encode( paramValue, UriComponent.Type.QUERY_PARAM ) );
    return queryParams;
  }

  protected Object[] callRest( Object[] rowData ) throws HopException {
    // get dynamic url ?
    if ( meta.isUrlInField() ) {
      data.realUrl = data.inputRowMeta.getString( rowData, data.indexOfUrlField );
    }
    // get dynamic method?
    if ( meta.isDynamicMethod() ) {
      data.method = data.inputRowMeta.getString( rowData, data.indexOfMethod );
      if ( Utils.isEmpty( data.method ) ) {
        throw new HopException( BaseMessages.getString( PKG, "Rest.Error.MethodMissing" ) );
      }
    }
    WebResource webResource = null;
    Client client = null;
    Object[] newRow = null;
    if ( rowData != null ) {
      newRow = rowData.clone();
    }
    try {
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "Rest.Log.ConnectingToURL", data.realUrl ) );
      }
      // create an instance of the com.sun.jersey.api.client.Client class
      client = ApacheHttpClient4.create( data.config );
      if ( data.basicAuthentication != null ) {
        client.addFilter( data.basicAuthentication );
      }
      // create a WebResource object, which encapsulates a web resource for the client
      webResource = client.resource( data.realUrl );

      // used for calculating the responseTime
      long startTime = System.currentTimeMillis();

      if ( data.useMatrixParams ) {
        // Add matrix parameters
        UriBuilder builder = webResource.getUriBuilder();
        for ( int i = 0; i < data.nrMatrixParams; i++ ) {
          String value = data.inputRowMeta.getString( rowData, data.indexOfMatrixParamFields[ i ] );
          if ( isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "Rest.Log.matrixParameterValue", data.matrixParamNames[ i ], value ) );
          }
          builder = builder.matrixParam( data.matrixParamNames[ i ], UriComponent.encode( value, UriComponent.Type.QUERY_PARAM ) );
        }
        webResource = client.resource( builder.build() );
      }

      if ( data.useParams ) {
        // Add query parameters
        for ( int i = 0; i < data.nrParams; i++ ) {
          String value = data.inputRowMeta.getString( rowData, data.indexOfParamFields[ i ] );
          if ( isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "Rest.Log.queryParameterValue", data.paramNames[ i ], value ) );
          }
          webResource = webResource.queryParams( createMultivalueMap( data.paramNames[ i ], value ) );
        }
      }
      if ( isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "Rest.Log.ConnectingToURL", webResource.getURI() ) );
      }
      WebResource.Builder builder = webResource.getRequestBuilder();
      String contentType = null; // media type override, if not null
      if ( data.useHeaders ) {
        // Add headers
        for ( int i = 0; i < data.nrheader; i++ ) {
          String value = data.inputRowMeta.getString( rowData, data.indexOfHeaderFields[ i ] );

          // unsure if an already set header will be returned to builder
          builder = builder.header( data.headerNames[ i ], value );
          if ( "Content-Type".equals( data.headerNames[ i ] ) ) {
            contentType = value;
          }
          if ( isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "Rest.Log.HeaderValue", data.headerNames[ i ], value ) );
          }
        }
      }

      ClientResponse response = null;
      String entityString = null;
      if ( data.useBody ) {
        // Set Http request entity
        entityString = Const.NVL( data.inputRowMeta.getString( rowData, data.indexOfBodyField ), null );
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "Rest.Log.BodyValue", entityString ) );
        }
      }
      try {
        if ( data.method.equals( RestMeta.HTTP_METHOD_GET ) ) {
          response = builder.get( ClientResponse.class );
        } else if ( data.method.equals( RestMeta.HTTP_METHOD_POST ) ) {
          if ( null != contentType ) {
            response = builder.type( contentType ).post( ClientResponse.class, entityString );
          } else {
            response = builder.type( data.mediaType ).post( ClientResponse.class, entityString );
          }
        } else if ( data.method.equals( RestMeta.HTTP_METHOD_PUT ) ) {
          if ( null != contentType ) {
            response = builder.type( contentType ).put( ClientResponse.class, entityString );
          } else {
            response = builder.type( data.mediaType ).put( ClientResponse.class, entityString );
          }
        } else if ( data.method.equals( RestMeta.HTTP_METHOD_DELETE ) ) {
          response = builder.delete( ClientResponse.class );
        } else if ( data.method.equals( RestMeta.HTTP_METHOD_HEAD ) ) {
          response = builder.head();
        } else if ( data.method.equals( RestMeta.HTTP_METHOD_OPTIONS ) ) {
          response = builder.options( ClientResponse.class );
        } else if ( data.method.equals( RestMeta.HTTP_METHOD_PATCH ) ) {
          if ( null != contentType ) {
            response = builder.type( contentType ).method( RestMeta.HTTP_METHOD_PATCH, ClientResponse.class, entityString );
          } else {
            response = builder.type( data.mediaType ).method( RestMeta.HTTP_METHOD_PATCH, ClientResponse.class,
              entityString );
          }
        } else {
          throw new HopException( BaseMessages.getString( PKG, "Rest.Error.UnknownMethod", data.method ) );
        }
      } catch ( UniformInterfaceException u ) {
        response = u.getResponse();
      }
      // Get response time
      long responseTime = System.currentTimeMillis() - startTime;
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "Rest.Log.ResponseTime", String.valueOf( responseTime ), data.realUrl ) );
      }

      // Get status
      int status = response.getStatus();
      // Display status code
      if ( isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "Rest.Log.ResponseCode", "" + status ) );
      }

      // Get Response
      String body;
      String headerString = null;
      try {
        body = response.getEntity( String.class );
      } catch ( UniformInterfaceException ex ) {
        body = "";
      }
      // get Header
      MultivaluedMap<String, String> headers = searchForHeaders( response );
      JSONObject json = new JSONObject();
      for ( java.util.Map.Entry<String, List<String>> entry : headers.entrySet() ) {
        String name = entry.getKey();
        List<String> value = entry.getValue();
        if ( value.size() > 1 ) {
          json.put( name, value );
        } else {
          json.put( name, value.get( 0 ) );
        }
      }
      headerString = json.toJSONString();
      // for output
      int returnFieldsOffset = data.inputRowMeta.size();
      // add response to output
      if ( !Utils.isEmpty( data.resultFieldName ) ) {
        newRow = RowDataUtil.addValueData( newRow, returnFieldsOffset, body );
        returnFieldsOffset++;
      }

      // add status to output
      if ( !Utils.isEmpty( data.resultCodeFieldName ) ) {
        newRow = RowDataUtil.addValueData( newRow, returnFieldsOffset, new Long( status ) );
        returnFieldsOffset++;
      }

      // add response time to output
      if ( !Utils.isEmpty( data.resultResponseFieldName ) ) {
        newRow = RowDataUtil.addValueData( newRow, returnFieldsOffset, new Long( responseTime ) );
        returnFieldsOffset++;
      }
      // add response header to output
      if ( !Utils.isEmpty( data.resultHeaderFieldName ) ) {
        newRow = RowDataUtil.addValueData( newRow, returnFieldsOffset, headerString );
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "Rest.Error.CanNotReadURL", data.realUrl ), e );
    } finally {
      if ( webResource != null ) {
        webResource = null;
      }
      if ( client != null ) {
        client.destroy();
      }
    }
    return newRow;
  }

  private void setConfig() throws HopException {
    if ( data.config == null ) {
      // Use ApacheHttpClient for supporting proxy authentication.
      data.config = new DefaultApacheHttpClient4Config();
      if ( !Utils.isEmpty( data.realProxyHost ) ) {
        // PROXY CONFIGURATION
        data.config.getProperties().put( ApacheHttpClient4Config.PROPERTY_PROXY_URI, "http://" + data.realProxyHost + ":" + data.realProxyPort );
        if ( !Utils.isEmpty( data.realHttpLogin ) && !Utils.isEmpty( data.realHttpPassword ) ) {
          AuthScope authScope = new AuthScope( data.realProxyHost, data.realProxyPort );
          UsernamePasswordCredentials credentials = new UsernamePasswordCredentials( data.realHttpLogin, data.realHttpPassword );
          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials( authScope, credentials );
          data.config.getProperties().put( ApacheHttpClient4Config.PROPERTY_CREDENTIALS_PROVIDER, credentialsProvider );
        }
      } else {
        if ( !Utils.isEmpty( data.realHttpLogin ) ) {
          // Basic authentication
          data.basicAuthentication = new HTTPBasicAuthFilter( data.realHttpLogin, data.realHttpPassword );
        }
      }
      if ( meta.isPreemptive() ) {
        data.config.getProperties().put( ApacheHttpClient4Config.PROPERTY_PREEMPTIVE_BASIC_AUTHENTICATION, true );
      }
      // SSL TRUST STORE CONFIGURATION
      if ( !Utils.isEmpty( data.trustStoreFile ) ) {
        try ( FileInputStream trustFileStream = new FileInputStream( data.trustStoreFile ) ) {
          KeyStore trustStore = KeyStore.getInstance( "JKS" );
          trustStore.load( trustFileStream, data.trustStorePassword.toCharArray() );
          TrustManagerFactory tmf = TrustManagerFactory.getInstance( "SunX509" );
          tmf.init( trustStore );

          SSLContext ctx = SSLContext.getInstance( "SSL" );
          ctx.init( null, tmf.getTrustManagers(), null );

          HostnameVerifier hv = ( hostname, session ) -> {
            if ( isDebug() ) {
              logDebug( "Warning: URL Host: " + hostname + " vs. " + session.getPeerHost() );
            }
            return true;
          };
          data.config.getProperties().put( HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties( hv, ctx ) );
        } catch ( NoSuchAlgorithmException e ) {
          throw new HopException( BaseMessages.getString( PKG, "Rest.Error.NoSuchAlgorithm" ), e );
        } catch ( KeyStoreException e ) {
          throw new HopException( BaseMessages.getString( PKG, "Rest.Error.KeyStoreException" ), e );
        } catch ( CertificateException e ) {
          throw new HopException( BaseMessages.getString( PKG, "Rest.Error.CertificateException" ), e );
        } catch ( FileNotFoundException e ) {
          throw new HopException( BaseMessages.getString( PKG, "Rest.Error.FileNotFound", data.trustStoreFile ), e );
        } catch ( IOException e ) {
          throw new HopException( BaseMessages.getString( PKG, "Rest.Error.IOException" ), e );
        } catch ( KeyManagementException e ) {
          throw new HopException( BaseMessages.getString( PKG, "Rest.Error.KeyManagementException" ), e );
        }
      }
    }
  }

  protected MultivaluedMap<String, String> searchForHeaders( ClientResponse response ) {
    return response.getHeaders();
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!

    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }
    if ( first ) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Let's set URL
      if ( meta.isUrlInField() ) {
        if ( Utils.isEmpty( meta.getUrlField() ) ) {
          logError( BaseMessages.getString( PKG, "Rest.Log.NoField" ) );
          throw new HopException( BaseMessages.getString( PKG, "Rest.Log.NoField" ) );
        }
        // cache the position of the field
        if ( data.indexOfUrlField < 0 ) {
          String realUrlfieldName = resolve( meta.getUrlField() );
          data.indexOfUrlField = data.inputRowMeta.indexOfValue( realUrlfieldName );
          if ( data.indexOfUrlField < 0 ) {
            // The field is unreachable !
            throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.ErrorFindingField", realUrlfieldName ) );
          }
        }
      } else {
        // Static URL
        data.realUrl = resolve( meta.getUrl() );
      }
      // Check Method
      if ( meta.isDynamicMethod() ) {
        String field = resolve( meta.getMethodFieldName() );
        if ( Utils.isEmpty( field ) ) {
          throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.MethodFieldMissing" ) );
        }
        data.indexOfMethod = data.inputRowMeta.indexOfValue( field );
        if ( data.indexOfMethod < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.ErrorFindingField", field ) );
        }
      }
      // set Headers
      int nrargs = meta.getHeaderName() == null ? 0 : meta.getHeaderName().length;
      if ( nrargs > 0 ) {
        data.nrheader = nrargs;
        data.indexOfHeaderFields = new int[ nrargs ];
        data.headerNames = new String[ nrargs ];
        for ( int i = 0; i < nrargs; i++ ) {
          // split into body / header
          data.headerNames[ i ] = resolve( meta.getHeaderName()[ i ] );
          String field = resolve( meta.getHeaderField()[ i ] );
          if ( Utils.isEmpty( field ) ) {
            throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.HeaderFieldEmpty" ) );
          }
          data.indexOfHeaderFields[ i ] = data.inputRowMeta.indexOfValue( field );
          if ( data.indexOfHeaderFields[ i ] < 0 ) {
            throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.ErrorFindingField", field ) );
          }
        }
        data.useHeaders = true;
      }
      if ( RestMeta.isActiveParameters( meta.getMethod() ) ) {
        // Parameters
        int nrparams = meta.getParameterField() == null ? 0 : meta.getParameterField().length;
        if ( nrparams > 0 ) {
          data.nrParams = nrparams;
          data.paramNames = new String[ nrparams ];
          data.indexOfParamFields = new int[ nrparams ];
          for ( int i = 0; i < nrparams; i++ ) {
            data.paramNames[ i ] = resolve( meta.getParameterName()[ i ] );
            String field = resolve( meta.getParameterField()[ i ] );
            if ( Utils.isEmpty( field ) ) {
              throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.ParamFieldEmpty" ) );
            }
            data.indexOfParamFields[ i ] = data.inputRowMeta.indexOfValue( field );
            if ( data.indexOfParamFields[ i ] < 0 ) {
              throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.ErrorFindingField", field ) );
            }
          }
          data.useParams = true;
        }
        int nrmatrixparams = meta.getMatrixParameterField() == null ? 0 : meta.getMatrixParameterField().length;
        if ( nrmatrixparams > 0 ) {
          data.nrMatrixParams = nrmatrixparams;
          data.matrixParamNames = new String[ nrmatrixparams ];
          data.indexOfMatrixParamFields = new int[ nrmatrixparams ];
          for ( int i = 0; i < nrmatrixparams; i++ ) {
            data.matrixParamNames[ i ] = resolve( meta.getMatrixParameterName()[ i ] );
            String field = resolve( meta.getMatrixParameterField()[ i ] );
            if ( Utils.isEmpty( field ) ) {
              throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.MatrixParamFieldEmpty" ) );
            }
            data.indexOfMatrixParamFields[ i ] = data.inputRowMeta.indexOfValue( field );
            if ( data.indexOfMatrixParamFields[ i ] < 0 ) {
              throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.ErrorFindingField", field ) );
            }
          }
          data.useMatrixParams = true;
        }
      }

      // Do we need to set body
      if ( RestMeta.isActiveBody( meta.getMethod() ) ) {
        String field = resolve( meta.getBodyField() );
        if ( !Utils.isEmpty( field ) ) {
          data.indexOfBodyField = data.inputRowMeta.indexOfValue( field );
          if ( data.indexOfBodyField < 0 ) {
            throw new HopException( BaseMessages.getString( PKG, "Rest.Exception.ErrorFindingField", field ) );
          }
          data.useBody = true;
        }
      }
    } // end if first
    try {
      Object[] outputRowData = callRest( r );
      putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);
      if ( checkFeedback( getLinesRead() ) ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "Rest.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "Rest.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        logError( Const.getStackTracker( e ) );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "Rest001" );
      }
    }
    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      data.resultFieldName = resolve( meta.getFieldName() );
      data.resultCodeFieldName = resolve( meta.getResultCodeFieldName() );
      data.resultResponseFieldName = resolve( meta.getResponseTimeFieldName() );
      data.resultHeaderFieldName = resolve( meta.getResponseHeaderFieldName() );

      // get authentication settings once
      data.realProxyHost = resolve( meta.getProxyHost() );
      data.realProxyPort = Const.toInt( resolve( meta.getProxyPort() ), 8080 );
      data.realHttpLogin = resolve( meta.getHttpLogin() );
      data.realHttpPassword = Encr.decryptPasswordOptionallyEncrypted( resolve( meta.getHttpPassword() ) );

      if ( !meta.isDynamicMethod() ) {
        data.method = resolve( meta.getMethod() );
        if ( Utils.isEmpty( data.method ) ) {
          logError( BaseMessages.getString( PKG, "Rest.Error.MethodMissing" ) );
          return false;
        }
      }

      data.trustStoreFile = resolve( meta.getTrustStoreFile() );
      data.trustStorePassword = resolve( meta.getTrustStorePassword() );

      String applicationType = Const.NVL( meta.getApplicationType(), "" );
      if ( applicationType.equals( RestMeta.APPLICATION_TYPE_XML ) ) {
        data.mediaType = MediaType.APPLICATION_XML_TYPE;
      } else if ( applicationType.equals( RestMeta.APPLICATION_TYPE_JSON ) ) {
        data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      } else if ( applicationType.equals( RestMeta.APPLICATION_TYPE_OCTET_STREAM ) ) {
        data.mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE;
      } else if ( applicationType.equals( RestMeta.APPLICATION_TYPE_XHTML ) ) {
        data.mediaType = MediaType.APPLICATION_XHTML_XML_TYPE;
      } else if ( applicationType.equals( RestMeta.APPLICATION_TYPE_FORM_URLENCODED ) ) {
        data.mediaType = MediaType.APPLICATION_FORM_URLENCODED_TYPE;
      } else if ( applicationType.equals( RestMeta.APPLICATION_TYPE_ATOM_XML ) ) {
        data.mediaType = MediaType.APPLICATION_ATOM_XML_TYPE;
      } else if ( applicationType.equals( RestMeta.APPLICATION_TYPE_SVG_XML ) ) {
        data.mediaType = MediaType.APPLICATION_SVG_XML_TYPE;
      } else if ( applicationType.equals( RestMeta.APPLICATION_TYPE_TEXT_XML ) ) {
        data.mediaType = MediaType.TEXT_XML_TYPE;
      } else {
        data.mediaType = MediaType.TEXT_PLAIN_TYPE;
      }
      try {
        setConfig();
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "Rest.Error.Config" ), e );
        return false;
      }
      return true;
    }
    return false;
  }

  public void dispose() {

    data.config = null;
    data.headerNames = null;
    data.indexOfHeaderFields = null;
    data.paramNames = null;
    super.dispose();
  }

}
