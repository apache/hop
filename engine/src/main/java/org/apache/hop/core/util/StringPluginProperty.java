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

package org.apache.hop.core.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.util.prefs.Preferences;

public class StringPluginProperty extends KeyValue<String> implements IPluginProperty {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = -2990345692552430357L;

  /**
   * Constructor. Value is null.
   *
   * @param key key to set.
   * @throws IllegalArgumentException if key is invalid.
   */
  public StringPluginProperty( final String key ) throws IllegalArgumentException {
    super( key, DEFAULT_STRING_VALUE );
  }

  /**
   *
   */
  public boolean evaluate() {
    return StringUtils.isNotBlank( this.getValue() );
  }

  /**
   *
   */
  public void appendXml( final StringBuilder builder ) {
    builder.append( XmlHandler.addTagValue( this.getKey(), this.getValue() ) );
  }

  /**
   *
   */
  public void loadXml( final Node node ) {
    final String value = XmlHandler.getTagValue( node, this.getKey() );
    this.setValue( value );
  }

  /**
   *
   */
  public void saveToPreferences( final Preferences node ) {
    node.put( this.getKey(), this.getValue() );
  }

  /**
   *
   */
  public void readFromPreferences( final Preferences node ) {
    this.setValue( node.get( this.getKey(), this.getValue() ) );
  }
}
