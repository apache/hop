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
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class WebResult {
  private static final Class<?> PKG = WebResult.class; // For Translator

  public static final String XML_TAG = "webresult";

  public static final String STRING_OK = "OK";
  public static final String STRING_ERROR = "ERROR";

  public static final WebResult OK = new WebResult( STRING_OK );

  private String result;
  private String message;
  private String id;

  public WebResult( String result ) {
    this( result, null, null );
  }

  public WebResult( String result, String message ) {
    this( result, message, null );
  }

  public WebResult( String result, String message, String id ) {
    this.result = result;
    this.message = message;
    this.id = id;
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append( "<" + XML_TAG + ">" ).append( Const.CR );

    xml.append( "  " ).append( XmlHandler.addTagValue( "result", result ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "message", message ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "id", id ) );

    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );

    return xml.toString();
  }

  @Override
  public String toString() {
    return getXml();
  }

  public WebResult( Node webResultNode ) {
    result = XmlHandler.getTagValue( webResultNode, "result" );
    message = XmlHandler.getTagValue( webResultNode, "message" );
    id = XmlHandler.getTagValue( webResultNode, "id" );
  }

  public String getResult() {
    return result;
  }

  public void setResult( String result ) {
    this.result = result;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage( String message ) {
    this.message = message;
  }

  public static WebResult fromXmlString(String xml ) throws HopXmlException {
    try {
      Document doc = XmlHandler.loadXmlString( xml );
      Node node = XmlHandler.getSubNode( doc, XML_TAG );

      return new WebResult( node );
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "WebResult.Error.UnableCreateResult" ), e );
    }
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId( String id ) {
    this.id = id;
  }
}
