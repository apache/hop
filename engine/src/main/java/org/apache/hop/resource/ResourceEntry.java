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

package org.apache.hop.resource;

import org.apache.hop.core.util.StringUtil;

public class ResourceEntry {
  public enum ResourceType {
    FILE, CONNECTION, SERVER, URL, DATABASENAME, ACTIONFILE, OTHER
  }

  private String resource;
  private ResourceType resourcetype;

  /**
   * @param resource
   * @param resourcetype
   */
  public ResourceEntry( String resource, ResourceType resourcetype ) {
    super();
    this.resource = resource;
    this.resourcetype = resourcetype;
  }

  /**
   * @return the resource
   */
  public String getResource() {
    return resource;
  }

  /**
   * @param resource the resource to set
   */
  public void setResource( String resource ) {
    this.resource = resource;
  }

  /**
   * @return the resourcetype
   */
  public ResourceType getResourcetype() {
    return resourcetype;
  }

  /**
   * @param resourcetype the resourcetype to set
   */
  public void setResourcetype( ResourceType resourcetype ) {
    this.resourcetype = resourcetype;
  }

  public String toXml( int indentLevel ) {
    StringBuilder buff = new StringBuilder( 30 );
    buff
      .append( StringUtil.getIndent( indentLevel ) )
      .append( "<Resource type='" )
      .append( this.getResourcetype() )
      .append( "'><![CDATA[" ).append( this.getResource() ).append( "]]>" ).append( "</Resource>" ).append(
      StringUtil.CRLF );
    return buff.toString();
  }

}
