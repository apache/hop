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

package org.apache.hop.server;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class HttpUtil {

  public static final int ZIP_BUFFER_SIZE = 8192;
  private static final String PROTOCOL_UNSECURE = "http";
  private static final String PROTOCOL_SECURE = "https";

  private HttpUtil() {
  }

  /**
   * Returns http GET request string using specified parameters.
   *
   * @param variables
   * @param hostname
   * @param port
   * @param webAppName
   * @param serviceAndArguments
   * @return
   * @throws UnsupportedEncodingException
   */
  public static String constructUrl( IVariables variables, String hostname, String port, String webAppName,
                                     String serviceAndArguments ) throws UnsupportedEncodingException {
    return constructUrl( variables, hostname, port, webAppName, serviceAndArguments, false );
  }

  public static String constructUrl( IVariables variables, String hostname, String port, String webAppName,
                                     String serviceAndArguments, boolean isSecure )
    throws UnsupportedEncodingException {
    String realHostname = variables.resolve( hostname );
    if ( !StringUtils.isEmpty( webAppName ) ) {
      serviceAndArguments = "/" + variables.resolve( webAppName ) + serviceAndArguments;
    }
    String protocol = isSecure ? PROTOCOL_SECURE : PROTOCOL_UNSECURE;
    String retval = protocol + "://" + realHostname + getPortSpecification( variables, port ) + serviceAndArguments;
    retval = Const.replace( retval, " ", "%20" );
    return retval;
  }

  public static String getPortSpecification( IVariables variables, String port ) {
    String realPort = variables.resolve( port );
    String portSpec = ":" + realPort;
    if ( Utils.isEmpty( realPort ) || port.equals( "80" ) ) {
      portSpec = "";
    }
    return portSpec;
  }

  /**
   * Base 64 decode, unzip and extract text using UTF-8 charset value for byte-wise
   * multi-byte character handling.
   *
   * @param loggingString64 base64 zip archive string representation
   * @return text from zip archive
   * @throws IOException
   */
  public static String decodeBase64ZippedString( String loggingString64 ) throws IOException {
    if ( loggingString64 == null || loggingString64.isEmpty() ) {
      return "";
    }
    StringWriter writer = new StringWriter();
    // base 64 decode
    byte[] bytes64 = Base64.decodeBase64( loggingString64.getBytes() );
    // unzip to string encoding-wise
    ByteArrayInputStream zip = new ByteArrayInputStream( bytes64 );


    // PDI-4325 originally used xml encoding in servlet
    try ( GZIPInputStream unzip = new GZIPInputStream( zip, HttpUtil.ZIP_BUFFER_SIZE );
          BufferedInputStream in = new BufferedInputStream( unzip, HttpUtil.ZIP_BUFFER_SIZE );
          InputStreamReader reader = new InputStreamReader( in, StandardCharsets.UTF_8 ) ) {

      writer = new StringWriter();

      // use same buffer size
      char[] buff = new char[ HttpUtil.ZIP_BUFFER_SIZE ];
      for ( int length = 0; ( length = reader.read( buff ) ) > 0; ) {
        writer.write( buff, 0, length );
      }
    }
    return writer.toString();
  }

  public static String encodeBase64ZippedString( String in ) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream( 1024 );
    try ( Base64OutputStream base64OutputStream = new Base64OutputStream( baos );
          GZIPOutputStream gzos = new GZIPOutputStream( base64OutputStream ) ) {
      gzos.write( in.getBytes( StandardCharsets.UTF_8 ) );
    }
    return baos.toString();
  }
}
