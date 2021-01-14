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

package org.apache.hop.laf;

/**
 * This is a static accessor for the dynamic property loader and should be used by all classes requiring access to
 * property files. The static accessor provides a notification from the LafFactory when the concrete handler is changed
 * at runtime should the LAF be changed.
 *
 * @author dhushon
 */
public class BasePropertyHandler implements ILafChangeListener<IPropertyHandler> {

  static BasePropertyHandler instance = null;
  protected IPropertyHandler handler = null;
  Class<IPropertyHandler> clazz = IPropertyHandler.class;

  static {
    getInstance();
  }

  private BasePropertyHandler() {
    init();
  }

  private void init() {
    // counting on LafFactory to return a class conforming to @see IMessageHandler
    handler = LafFactory.getHandler( clazz );
  }

  public static BasePropertyHandler getInstance() {
    if ( instance == null ) {
      instance = new BasePropertyHandler();
    }
    return instance;
  }

  protected IPropertyHandler getHandler() {
    return handler;
  }

  protected static IPropertyHandler getInstanceHandler() {
    return getInstance().getHandler();
  }

  /**
   * return the value of a given key from the properties list
   *
   * @param key
   * @return null if the key is not found
   */
  public static String getProperty( String key ) {
    return getInstanceHandler().getProperty( key );
  }

  /**
   * return the value of a given key from the properties list, returning the defValue string should the key not be found
   *
   * @param key
   * @param defValue
   * @return a string representing either the value associated with the passed key or defValue should that key not be
   * found
   */
  public static String getProperty( String key, String defValue ) {
    return getInstanceHandler().getProperty( key, defValue );
  }

  @Override
  public void notify( IPropertyHandler changedObject ) {
    handler = changedObject;
  }

}
