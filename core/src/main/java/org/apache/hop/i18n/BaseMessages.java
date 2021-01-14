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

package org.apache.hop.i18n;

import org.apache.hop.laf.ILafChangeListener;
import org.apache.hop.laf.LafFactory;

/**
 * BaseMessage is called by all Message classes to enable the delegation of message delivery, by key to be delegated to
 * the appropriately authoritative supplier as registered in the LafFactory enabling both i18n as well as pluggable look
 * and feel (LAF)
 *
 * @author dhushon
 */
public class BaseMessages implements ILafChangeListener<IMessageHandler> {
  static BaseMessages instance = null;
  protected IMessageHandler handler = null;
  Class<IMessageHandler> clazz = IMessageHandler.class;

  static {
    getInstance();
  }

  private BaseMessages() {
    init();
  }

  private void init() {
    // counting on LafFactory to return a class conforming to @see IMessageHandler
    handler = LafFactory.getHandler( clazz );
  }

  public static BaseMessages getInstance() {
    if ( instance == null ) {
      instance = new BaseMessages();
    }
    return instance;
  }

  protected IMessageHandler getHandler() {
    return handler;
  }

  protected static IMessageHandler getInstanceHandler() {
    return getInstance().getHandler();
  }

  public static String getString( String key ) {
    return getInstanceHandler().getString( key );
  }

  public static String getString( String packageName, String key ) {
    return getInstanceHandler().getString( packageName, key, new String[] {} );
  }

  public static String getString( String packageName, String key, String... parameters ) {
    return getInstanceHandler().getString( packageName, key, parameters );
  }

  public static String getString( String packageName, String key, Class<?> resourceClass, String... parameters ) {
    return getInstanceHandler().getString( packageName, key, resourceClass, parameters );
  }

  public static String getString( Class<?> packageClass, String key, String... parameters ) {
    return getInstanceHandler().getString( packageClass.getPackage().getName(), key, packageClass, parameters );
  }

  public static String getString( Class<?> packageClass, String key, Class<?> resourceClass, String... parameters ) {
    return getInstanceHandler().getString( packageClass.getPackage().getName(), key, packageClass, parameters );
  }

  public static String getString( Class<?> packageClass, String key, Object... parameters ) {
    String[] strings = new String[ parameters.length ];
    for ( int i = 0; i < strings.length; i++ ) {
      strings[ i ] = parameters[ i ] != null ? parameters[ i ].toString() : "";
    }
    return getString( packageClass, key, strings );
  }

  @Override
  public void notify( IMessageHandler changedObject ) {
    handler = changedObject;
  }
}
