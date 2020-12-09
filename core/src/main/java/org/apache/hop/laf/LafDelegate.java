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

import java.util.HashSet;
import java.util.Iterator;

/**
 * A factory delegate for a specific kind of LAFHandler
 *
 * @param <E>
 * @author dhushon
 */
public class LafDelegate<E extends IHandler> {

  E handler;
  Class<E> handlerClass = null;
  Class<E> defaultHandlerClass = null;

  // Set of Listeners for a concrete handler - intended use... getListeners for a given class
  private HashSet<ILafChangeListener<E>> registry = new HashSet<>();

  /**
   *
   */
  public LafDelegate( Class<E> handler, Class<E> defaultHandler ) {
    handlerClass = handler;
    this.defaultHandlerClass = defaultHandler;
    // TODO: Remove this... needed because spoon hasn't yet been init'ed, fulfilling static initializers...
    init();

  }

  private void init() {
    if ( handler != null ) {
      handler = loadHandler( handlerClass );
    } else {
      handler = loadHandler( defaultHandlerClass );
    }
  }

  /**
   * load a concrete IHandler for a given Interface (by String classname) if the class is not instantiable, will fallback
   * to default, and then fallback to an abstract implementation. Will always return non-null.
   *
   * @param classname
   * @return
   */
  @SuppressWarnings( "unchecked" )
  public E newHandlerInstance( String classname ) throws ClassNotFoundException {
    E h = null;
    Class<E> c = null;
    c = (Class<E>) Class.forName( classname );
    h = loadHandler( c );
    return h;
  }

  private E loadHandler( Class<E> c ) {
    E h = null;
    try {
      if ( handlerClass.isAssignableFrom( c ) ) {
        h = c.newInstance();
      }
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    if ( h != null ) {
      changeHandler( h );
    }
    return h;
  }

  public E registerChangeListener( ILafChangeListener<E> listener ) {
    // see if a handler has been instantiated for the requested Interface
    registry.add( listener );
    return handler;
  }

  /**
   * unregister a @see ILafChangeListener from the Map which will prevent notification on @see IHandler change
   *
   * @param listener
   */
  public void unregisterChangeListener( ILafChangeListener<E> listener ) {
    registry.remove( listener );
  }

  public HashSet<ILafChangeListener<E>> getListeners() {
    return registry;
  }

  public void loadListeners( HashSet<ILafChangeListener<E>> listeners ) {
    registry = listeners;
  }

  public void changeHandler( E handler ) {
    this.handler = handler;
    notifyListeners();
  }

  protected void notifyListeners() {
    Iterator<ILafChangeListener<E>> iterator = registry.iterator();
    while ( iterator.hasNext() ) {
      iterator.next().notify( handler );
    }
  }

  public E getHandler() {
    return handler;
  }

}
