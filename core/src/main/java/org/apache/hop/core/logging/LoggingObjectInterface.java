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

package org.apache.hop.core.logging;

import java.util.Date;


public interface LoggingObjectInterface {
  /**
   * Gets the object name.
   *
   * @return the name
   */
  public String getObjectName();


  /**
   * Gets the filename.
   *
   * @return the filename
   */
  public String getFilename();

  /**
   * Gets the log channel id.
   *
   * @return the log channel id
   */
  public String getLogChannelId();

  /**
   * Gets the parent.
   *
   * @return the parent
   */
  public LoggingObjectInterface getParent();

  /**
   * Gets the object type.
   *
   * @return the objectType
   */
  public LoggingObjectType getObjectType();

  /**
   * Gets a string identifying a copy in a series of transforms.
   *
   * @return A string identifying a copy in a series of transforms.
   */
  public String getObjectCopy();

  /**
   * Gets the logging level of the log channel of this logging object.
   *
   * @return The logging level of the log channel of this logging object.
   */
  public LogLevel getLogLevel();

  /**
   * Gets the execution container (Carte/DI server/BI Server) object id. We use this to see to which copy of the
   * job/pipeline hierarchy this object belongs. If it is null, we assume that we are running a single copy in
   * HopGui/Pan/Kitchen.
   *
   * @return The execution container (Carte/DI server/BI Server) object id.
   */
  public String getContainerObjectId();

  /**
   * Gets the registration date of this logging object. Null if it's not registered.
   *
   * @return The registration date of this logging object. Null if it's not registered.
   */
  public Date getRegistrationDate();

  /**
   * Gets the boolean value of whether or not this object is gathering kettle metrics during execution.
   *
   * @return true if this logging object is gathering kettle metrics during execution
   */
  public boolean isGatheringMetrics();

  /**
   * Enable of disable kettle metrics gathering during execution
   *
   * @param gatheringMetrics set to true to enable metrics gathering during execution.
   */
  public void setGatheringMetrics( boolean gatheringMetrics );

  /**
   * This option will force the create of a separate logging channel even if the logging concerns identical objects with
   * identical names.
   *
   * @param forcingSeparateLogging Set to true to force separate logging
   */
  public void setForcingSeparateLogging( boolean forcingSeparateLogging );

  /**
   * @return True if the logging is forcibly separated out from even identical objects.
   */
  public boolean isForcingSeparateLogging();
}
