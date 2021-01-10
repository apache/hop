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

package org.apache.hop.core.database.util;

import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.ILogChannel;

public interface ILogExceptionBehaviour {

  /**
   * When exception during logging is raised, depending on item settings we will throw exception up, or just put a log
   * record on this event.
   * <p>
   * Different behaviors are created in backward compatibility with existing code. See PDI-9790.
   *
   * @param packageClass
   * @param key
   * @param parameters
   */
  void registerException( ILogChannel log, Class<?> packageClass, String key, String... parameters ) throws HopDatabaseException;

  void registerException( ILogChannel log, Exception e, Class<?> packageClass, String key,
                          String... parameters ) throws HopDatabaseException;

}
