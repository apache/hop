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

package org.apache.hop.core.logging;

import java.util.Date;


public interface ILoggingObject {
  /**
   * Gets the object name.
   *
   * @return the name
   */
  String getObjectName();


  /**
   * Gets the filename.
   *
   * @return the filename
   */
  String getFilename();

  /**
   * Gets the log channel id.
   *
   * @return the log channel id
   */
  String getLogChannelId();

  /**
   * Gets the parent.
   *
   * @return the parent
   */
  ILoggingObject getParent();

  /**
   * Gets the object type.
   *
   * @return the objectType
   */
  LoggingObjectType getObjectType();

  /**
   * Gets a string identifying a copy in a series of transforms.
   *
   * @return A string identifying a copy in a series of transforms.
   */
  String getObjectCopy();

  /**
   * Gets the logging level of the log channel of this logging object.
   *
   * @return The logging level of the log channel of this logging object.
   */
  LogLevel getLogLevel();

  /**
   * Gets the execution container (Carte/DI server/BI Server) object id. We use this to see to which copy of the
   * workflow/pipeline hierarchy this object belongs. If it is null, we assume that we are running a single copy in
   * HopGui/Pan/Kitchen.
   *
   * @return The execution container (Carte/DI server/BI Server) object id.
   */
  String getContainerId();

  /**
   * Gets the registration date of this logging object. Null if it's not registered.
   *
   * @return The registration date of this logging object. Null if it's not registered.
   */
  Date getRegistrationDate();

  /**
   * Gets the boolean value of whether or not this object is gathering hop metrics during execution.
   *
   * @return true if this logging object is gathering hop metrics during execution
   */
  boolean isGatheringMetrics();

  /**
   * Enable of disable hop metrics gathering during execution
   *
   * @param gatheringMetrics set to true to enable metrics gathering during execution.
   */
  void setGatheringMetrics( boolean gatheringMetrics );

  /**
   * This option will force the create of a separate logging channel even if the logging concerns identical objects with
   * identical names.
   *
   * @param forcingSeparateLogging Set to true to force separate logging
   */
  void setForcingSeparateLogging( boolean forcingSeparateLogging );

  /**
   * @return True if the logging is forcibly separated out from even identical objects.
   */
  boolean isForcingSeparateLogging();
}
