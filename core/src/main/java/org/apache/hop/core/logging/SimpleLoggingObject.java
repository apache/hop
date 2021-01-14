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


public class SimpleLoggingObject implements ILoggingObject {

  private String objectName;
  private LoggingObjectType objectType;
  private ILoggingObject parent;
  private LogLevel logLevel = DefaultLogLevel.getLogLevel();
  private String containerObjectId;
  private Date registrationDate;
  private boolean gatheringMetrics;
  private boolean forcingSeparateLogging;

  /**
   * @param objectName
   * @param loggingObjectType
   * @param parent
   */
  public SimpleLoggingObject( String objectName, LoggingObjectType loggingObjectType, ILoggingObject parent ) {
    this.objectName = objectName;
    this.objectType = loggingObjectType;
    this.parent = parent;
    if ( parent != null ) {
      this.logLevel = parent.getLogLevel();
      this.containerObjectId = parent.getContainerId();
    }
  }

  /**
   * @return the name
   */
  @Override
  public String getObjectName() {
    return objectName;
  }

  /**
   * @param name the name to set
   */
  public void setObjectName( String name ) {
    this.objectName = name;
  }

  /**
   * @return the objectType
   */
  @Override
  public LoggingObjectType getObjectType() {
    return objectType;
  }

  /**
   * @param objectType the objectType to set
   */
  public void setObjectType( LoggingObjectType objectType ) {
    this.objectType = objectType;
  }

  /**
   * @return the parent
   */
  @Override
  public ILoggingObject getParent() {
    return parent;
  }

  /**
   * @param parent the parent to set
   */
  public void setParent( ILoggingObject parent ) {
    this.parent = parent;
  }

  @Override
  public String getFilename() {
    return null;
  }

  @Override
  public String getLogChannelId() {
    return null;
  }

  @Override
  public String getObjectCopy() {
    return null;
  }

  @Override
  public LogLevel getLogLevel() {
    return logLevel;
  }

  public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  @Override
  public String getContainerId() {
    return containerObjectId;
  }

  public void setContainerObjectId( String containerObjectId ) {
    this.containerObjectId = containerObjectId;
  }

  /**
   * @return the registrationDate
   */
  @Override
  public Date getRegistrationDate() {
    return registrationDate;
  }

  /**
   * @param registrationDate the registrationDate to set
   */
  public void setRegistrationDate( Date registrationDate ) {
    this.registrationDate = registrationDate;
  }

  /**
   * @return the gatheringMetrics
   */
  @Override
  public boolean isGatheringMetrics() {
    return gatheringMetrics;
  }

  /**
   * @param gatheringMetrics the gatheringMetrics to set
   */
  @Override
  public void setGatheringMetrics( boolean gatheringMetrics ) {
    this.gatheringMetrics = gatheringMetrics;
  }

  /**
   * @return the forcingSeparateLogging
   */
  @Override
  public boolean isForcingSeparateLogging() {
    return forcingSeparateLogging;
  }

  /**
   * @param forcingSeparateLogging the forcingSeparateLogging to set
   */
  @Override
  public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
    this.forcingSeparateLogging = forcingSeparateLogging;
  }
}
