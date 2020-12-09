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

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.ArrayList;
import java.util.List;

/**
 * @param <T> type of key value.
 */
public class KeyValueFactory<T> {

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<String> STRING = new KeyValueFactory<>( "" );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Integer> INTEGER = new KeyValueFactory<>( 0 );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Integer> INTEGER_ONE = new KeyValueFactory<>( 1 );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Boolean> BOOLEAN = new KeyValueFactory<>( Boolean.FALSE );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Boolean> BOOLEAN_TRUE = new KeyValueFactory<>( Boolean.TRUE );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Float> FLOAT = new KeyValueFactory<>( 0.0f );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Float> FLOAT_ONE = new KeyValueFactory<>( 1.0f );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Double> DOUBLE = new KeyValueFactory<>( 0.0 );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Double> DOUBLE_ONE = new KeyValueFactory<>( 1.0 );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Long> LONG = new KeyValueFactory<>( 0L );

  /**
   * Default instance for ...
   */
  public static final KeyValueFactory<Long> LONG_ONE = new KeyValueFactory<>( 1L );

  private final T defaultValue;

  /**
   * Constructor.
   *
   * @param defaultValue default value to set.
   */
  public KeyValueFactory( final T defaultValue ) {
    this.defaultValue = defaultValue;
  }

  /**
   * @return the defaultValue
   */
  public T getDefaultValue() {
    return this.defaultValue;
  }

  /**
   * @param key key to set.
   * @return new key value initialized with default value.
   * @throws IllegalArgumentException if key is blank.
   */
  public KeyValue<T> create( final String key ) throws IllegalArgumentException {
    return new KeyValue<>( key, this.defaultValue );
  }

  /**
   * @param keys keys to use.
   * @return new instances.
   * @throws IllegalArgumentException if one key is blank.
   */
  public List<KeyValue<T>> createAll( final String... keys ) throws IllegalArgumentException {
    final List<KeyValue<T>> instances = new ArrayList<>();
    for ( String key : keys ) {
      instances.add( create( key ) );
    }
    return instances;
  }

  /**
   * {@inheritDoc}
   *
   * @see Object#toString()
   */
  @Override
  public String toString() {
    final ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );
    builder.append( "defaultValue", this.defaultValue );
    return builder.toString();
  }

}
