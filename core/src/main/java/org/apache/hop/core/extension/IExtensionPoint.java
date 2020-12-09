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

public interface IExtensionPoint<T> {

  /**
   * This method is called by the Hop code
   *
   * @param log the logging channel to log debugging information to
   * @param variables The variables of the current context. This is usually the variables of the parent or the executing entity.
   * @param t   The subject object that is passed to the plugin code
   * @throws HopException In case the plugin decides that an error has occurred and the parent process should stop.
   */
  void callExtensionPoint( ILogChannel log, IVariables variables, T t ) throws HopException;
}
