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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.prefs.Preferences;

public class StringListPluginProperty extends KeyValue<List<String>> implements IPluginProperty, Iterable<String> {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = 2003662016166396542L;

  /**
   * Value XML tag name.
   */
  public static final String VALUE_XML_TAG_NAME = "value";

  /**
   * The separator character.
   */
  public static final char SEPARATOR_CHAR = ',';

  /**
   * @param key key to use.
   */
  public StringListPluginProperty( final String key ) {
    super( key, new ArrayList<>() );
  }

  /**
   * @param list list to transform, maybe null.
   * @return string, never null.
   */
  public static String asString( final List<String> list ) {
    if ( list == null ) {
      return "";
    }
    return StringUtils.join( list, SEPARATOR_CHAR );
  }

  /**
   * @param input the input.
   * @return new list, never null.
   */
  public static List<String> fromString( final String input ) {
    final List<String> result = new ArrayList<>();
    if ( StringUtils.isBlank( input ) ) {
      return result;
    }
    for ( String value : StringUtils.split( input, SEPARATOR_CHAR ) ) {
      result.add( value );
    }
    return result;
  }

  /**
   *
   */
  public void appendXml( final StringBuilder builder ) {
    if ( !this.evaluate() ) {
      return;
    }
    final String value = asString( this.getValue() );
    builder.append( XmlHandler.addTagValue( this.getKey(), value ) );
  }

  /**
   *
   */
  public boolean evaluate() {
    return CollectionPredicates.NOT_NULL_OR_EMPTY_COLLECTION.evaluate( this.getValue() );
  }

  /**
   *
   */
  public void loadXml( final Node node ) {
    final String stringValue = XmlHandler.getTagValue( node, this.getKey() );
    final List<String> values = fromString( stringValue );
    this.setValue( values );
  }

  /**
   *
   */
  public void readFromPreferences( final Preferences node ) {
    final String stringValue = node.get( this.getKey(), asString( this.getValue() ) );
    this.setValue( fromString( stringValue ) );
  }


  /**
   *
   */
  public void saveToPreferences( final Preferences node ) {
    node.put( this.getKey(), asString( this.getValue() ) );
  }


  /**
   * @param values values to set, no validation.
   */
  public void setValues( final String... values ) {
    if ( this.getValue() == null ) {
      this.setValue( new ArrayList<>() );
    }
    for ( String value : values ) {
      this.getValue().add( value );
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see java.lang.Iterable#iterator()
   */
  public Iterator<String> iterator() throws IllegalStateException {
    this.assertValueNotNull();
    return this.getValue().iterator();
  }

  /**
   * @return true if list is empty .
   */
  public boolean isEmpty() {
    this.assertValueNotNull();
    return this.getValue().isEmpty();
  }

  /**
   * @return size
   * @throws IllegalStateException if value is null.
   */
  public int size() throws IllegalStateException {
    this.assertValueNotNull();
    return this.getValue().size();
  }

  /**
   * Assert state, value not null.
   *
   * @throws IllegalStateException if this.value is null.
   */
  public void assertValueNotNull() throws IllegalStateException {
    if ( this.getValue() == null ) {
      throw new IllegalStateException( "Value is null" );
    }
  }

}
