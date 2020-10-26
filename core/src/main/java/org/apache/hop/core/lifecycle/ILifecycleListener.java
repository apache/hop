/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.core.lifecycle;

/**
 * A callback interface that listens to specific lifecycle events triggered when HopGui starts and stops.
 * <p>
 * Listeners are loaded dynamically by PDI. In order to register a listener with HopGui, a class that implements this
 * interface must be placed in the "org.apache.hop.core.listeners.pdi" package, and it will be loaded automatically when
 * HopGui starts.
 *
 * @author Alex Silva
 */
public interface ILifecycleListener {
  /**
   * Called when the application starts.
   *
   * @throws LifecycleException Whenever this listener is unable to start succesfully.
   */
  public void onStart( ILifeEventHandler handler ) throws LifecycleException;

  /**
   * Called when the application ends
   *
   * @throws LifecycleException If a problem prevents this listener from shutting down.
   */
  public void onExit( ILifeEventHandler handler ) throws LifecycleException;

}
