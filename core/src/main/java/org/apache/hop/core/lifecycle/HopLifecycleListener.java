/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
 * A callback to be notified when the Hop environment is initialized and shut down.
 */
public interface HopLifecycleListener {
  /**
   * Called during HopEnvironment initialization.
   *
   * @throws LifecycleException
   *           to indicate the listener did not complete successfully. Severe {@link LifecycleException}s will stop the
   *           initialization of the HopEnvironment.
   */
  void onEnvironmentInit() throws org.apache.hop.core.lifecycle.LifecycleException;

  /**
   * Called when the VM that initialized HopEnvironment terminates.
   */
  void onEnvironmentShutdown();
}
