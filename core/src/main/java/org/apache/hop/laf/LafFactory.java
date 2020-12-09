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

import org.apache.hop.i18n.IMessageHandler;
import org.apache.hop.i18n.LafMessageHandler;

import java.util.HashMap;

/**
 * the LafFactory provides a mechanism whereby @see IHandler s can be dynamically replaced to enable user driven
 * replacement of dynamic resolvers whether ImageHandlers, MessageHandlers, or other elements of Look and Feel.
 *
 * @author dhushon
 */
public class LafFactory {

  static Class<? extends IHandler> _defMessageHandler = LafMessageHandler.class;
  static Class<? extends IHandler> _defPropertyHandler = OverlayPropertyHandler.class;

  // Registry of Delegates that know how to load the appropriate handlers
  private static HashMap<Class<? extends IHandler>, LafDelegate<? extends IHandler>> delegateRegistry =
    new HashMap<>();

  // Map an abstract ClassName (by String) to an implementing Class
  private static HashMap<String, Class<? extends IHandler>> handlerRef =
    new HashMap<>();

  static {
    // handlers.put(IMessageHandler.class.), (IHandler)_defMessageHandler.newInstance());
    handlerRef.put( IMessageHandler.class.getName(), _defMessageHandler );
    handlerRef.put( IPropertyHandler.class.getName(), _defPropertyHandler );
  }

  @SuppressWarnings( "unchecked" )
  protected static synchronized <V extends IHandler> LafDelegate<V> getDelegate( Class<V> handler ) {
    LafDelegate<V> l = (LafDelegate<V>) delegateRegistry.get( handler );
    if ( l == null ) {
      // TODO: check subclasses
      Class<V> defaultHandler = (Class<V>) handlerRef.get( handler.getName() );
      l = new LafDelegate<>( handler, defaultHandler );
      delegateRegistry.put( handler, l );
    }
    return l;
  }

  /**
   * Return an instance of the class that has been designated as the implementor of the requested Interface, will return
   * null if there is no implementor.
   *
   * @param <V>
   * @param handler
   * @return
   */
  public static <V extends IHandler> V getHandler( Class<V> handler ) {
    LafDelegate<V> l = getDelegate( handler );
    return l.getHandler();
  }

}
