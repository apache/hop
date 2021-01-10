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
import org.apache.hop.i18n.BaseMessages;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple utility for messages. Usage: <code>
 * [...]
 * private static final PluginMessages MESSAGES = PluginMessages.getMessages([class]).
 * [...]
 * <p>
 * MESSAGES.getString([key])
 * </code>
 *
 */
public final class PluginMessages {

  private static final ConcurrentHashMap<String, PluginMessages> MESSAGES_MAP = new ConcurrentHashMap<>();

  /**
   * Factory method.
   *
   * @param packageName package name.
   * @return messages.
   * @throws IllegalArgumentException if package name is blank.
   */
  public static PluginMessages getMessages( final String packageName ) throws IllegalArgumentException {
    Assert.assertNotBlank( packageName, "Package name cannot be blank" );

    PluginMessages pm = MESSAGES_MAP.get( packageName );
    if ( pm == null ) {
      return MESSAGES_MAP.putIfAbsent( packageName, new PluginMessages( packageName ) );
    }
    return pm;
  }

  /**
   * Factory method.
   *
   * @param someClassInPackage some class in package.
   * @return messages.
   * @throws IllegalArgumentException if class is null
   */
  public static PluginMessages getMessages( final Class<?> someClassInPackage ) throws IllegalArgumentException {
    Assert.assertNotNull( someClassInPackage, "Class cannot be null" );
    return getMessages( someClassInPackage.getPackage().getName() );
  }

  private final String packageName;

  /**
   * @param packageName package name to set.
   */
  private PluginMessages( final String packageName ) {
    this.packageName = packageName;
  }

  /**
   * @return the packageName
   */
  public String getPackageName() {
    return this.packageName;
  }

  /**
   * @param key the key.
   * @return the message.
   */
  public String getString( final String key ) {
    return BaseMessages.getString( this.packageName, key );
  }

  /**
   * @param key    the key.
   * @param param1 the param1.
   * @return the message.
   */
  public String getString( final String key, final String param1 ) {
    return BaseMessages.getString( this.packageName, key, param1 );
  }

  /**
   * @param key    the key.
   * @param param1 the param1.
   * @param param2 the param2.
   * @return the message.
   */
  public String getString( final String key, final String param1, final String param2 ) {
    return BaseMessages.getString( this.packageName, key, param1, param2 );
  }

  /**
   * @param key    the key.
   * @param param1 the param1.
   * @param param2 the param2.
   * @param param3 the param3.
   * @return the message.
   */
  public String getString( final String key, final String param1, final String param2, final String param3 ) {
    return BaseMessages.getString( this.packageName, key, param1, param2, param3 );
  }

  /**
   * @param key    the key.
   * @param param1 the param1.
   * @param param2 the param2.
   * @param param3 the param3.
   * @param param4 the param4.
   * @return the message.
   */
  public String getString( final String key, final String param1, final String param2, final String param3,
                           final String param4 ) {
    return BaseMessages.getString( this.packageName, key, param1, param2, param3, param4 );
  }

  /**
   * @param key    the key.
   * @param param1 the param1.
   * @param param2 the param2.
   * @param param3 the param3.
   * @param param4 the param4.
   * @param param5 the param5.
   * @return the message.
   */
  public String getString( final String key, final String param1, final String param2, final String param3,
                           final String param4, final String param5 ) {
    return BaseMessages.getString( this.packageName, key, param1, param2, param3, param4, param5 );
  }

  /**
   * @param key    the key.
   * @param param1 the param1.
   * @param param2 the param2.
   * @param param3 the param3.
   * @param param4 the param4.
   * @param param5 the param5.
   * @param param6 the param6.
   * @return the message.
   */
  public String getString( final String key, final String param1, final String param2, final String param3,
                           final String param4, final String param5, final String param6 ) {
    return BaseMessages.getString( this.packageName, key, param1, param2, param3, param4, param5, param6 );
  }

  /**
   * {@inheritDoc}
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    final ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );
    builder.append( "packageName", this.packageName );
    return builder.toString();
  }

}
