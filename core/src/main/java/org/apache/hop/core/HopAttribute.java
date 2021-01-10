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

package org.apache.hop.core;

public class HopAttribute implements IHopAttribute {
  private String key;

  private String xmlCode;
  private String description;
  private String tooltip;
  private int type;
  private IHopAttribute parent;

  /**
   * @param key
   * @param xmlCode
   * @param description
   * @param tooltip
   * @param type
   */
  public HopAttribute( String key, String xmlCode, String description, String tooltip,
                       int type, IHopAttribute parent ) {
    this.key = key;
    this.xmlCode = xmlCode;
    this.description = description;
    this.tooltip = tooltip;
    this.type = type;
    this.parent = parent;
  }

  @Override
  public String getKey() {
    return key;
  }

  public void setKey( String key ) {
    this.key = key;
  }

  /**
   * @return the xmlCode
   */
  @Override
  public String getXmlCode() {
    return xmlCode;
  }

  /**
   * @param xmlCode the xmlCode to set
   */
  public void setXmlCode( String xmlCode ) {
    this.xmlCode = xmlCode;
  }

  /**
   * @return the description
   */
  @Override
  public String getDescription() {
    return description;
  }

  /**
   * @param description the description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * @return the tooltip
   */
  @Override
  public String getTooltip() {
    return tooltip;
  }

  /**
   * @param tooltip the tooltip to set
   */
  public void setTooltip( String tooltip ) {
    this.tooltip = tooltip;
  }

  /**
   * @return the type
   */
  @Override
  public int getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType( int type ) {
    this.type = type;
  }

  /**
   * @return the parent
   */
  @Override
  public IHopAttribute getParent() {
    return parent;
  }

  /**
   * @param parent the parent to set
   */
  public void setParent( IHopAttribute parent ) {
    this.parent = parent;
  }
}
