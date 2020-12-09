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

package org.apache.hop.core.changed;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChangedFlag implements IChanged {

  @JsonIgnore
  private Set<IHopObserver> obs = Collections.newSetFromMap( new ConcurrentHashMap<>() );

  @JsonIgnore
  private AtomicBoolean changed = new AtomicBoolean();

  public void addObserver( IHopObserver o ) {
    if ( o == null ) {
      throw new NullPointerException();
    }

    validateAdd( o );
  }

  private synchronized void validateAdd( IHopObserver o ) {
    if ( !obs.contains( o ) ) {
      obs.add( o );
    }
  }

  public void deleteObserver( IHopObserver o ) {
    obs.remove( o );
  }

  public void notifyObservers( Object arg ) {

    IHopObserver[] lobs;
    if ( !changed.get() ) {
      return;
    }
    lobs = obs.toArray( new IHopObserver[ obs.size() ] );
    clearChanged();
    for ( int i = lobs.length - 1; i >= 0; i-- ) {
      lobs[ i ].update( this, arg );
    }
  }

  /**
   * Sets this as being changed.
   */
  public void setChanged() {
    changed.set( true );
  }

  /**
   * Sets whether or not this has changed.
   *
   * @param ch true if you want to mark this as changed, false otherwise
   */
  public void setChanged( boolean b ) {
    changed.set( b );
  }

  /**
   * Clears the changed flags.
   */
  public void clearChanged() {
    changed.set( false );
  }

  /**
   * Checks whether or not this has changed.
   *
   * @return true if the this has changed, false otherwise
   */
  public boolean hasChanged() {
    return changed.get();
  }

}
