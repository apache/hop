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

package org.apache.hop.core.extension;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;

public class ExtensionPointHandler {

  /**
   * This method looks up the extension point plugins with the given ID in the plugin registry. If one or more are
   * found, their corresponding interfaces are instantiated and the callExtensionPoint() method is invoked.
   *
   * @param log    the logging channel to write debugging information to
   * @param variables
   * @param id     The ID of the extension point to call
   * @param object The parent object that is passed to the plugin
   * @throws HopException In case something goes wrong in the plugin and we need to stop what we're doing.
   */
  public static void callExtensionPoint( final ILogChannel log, IVariables variables, final String id, final Object object )
    throws HopException {
    ExtensionPointMap.getInstance().callExtensionPoint( log, variables, id, object );
  }
}
