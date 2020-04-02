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

package org.apache.hop.www;

import java.util.Date;

public class SocketPortAllocation {
  private boolean allocated;
  private int port;
  private Date lastRequested;

  private String pipelineName;
  private String clusterRunId;
  private String sourceSlaveName;
  private String sourceTransformName;
  private String sourceTransformCopy;
  private String targetSlaveName;
  private String targetTransformName;
  private String targetTransformCopy;

  public SocketPortAllocation( int port, Date lastRequested, String clusterRunId, String pipelineName,
                               String sourceSlaveName, String sourceTransformName, String sourceTransformCopy, String targetSlaveName,
                               String targetTransformName, String targetTransformCopy ) {
    this.port = port;
    this.lastRequested = lastRequested;
    this.clusterRunId = clusterRunId;
    this.pipelineName = pipelineName;
    this.sourceSlaveName = sourceSlaveName;
    this.sourceTransformName = sourceTransformName;
    this.sourceTransformCopy = sourceTransformCopy;

    this.targetSlaveName = targetSlaveName;
    this.targetTransformName = targetTransformName;
    this.targetTransformCopy = targetTransformCopy;
    this.allocated = true;
  }

  /**
   * @return the port
   */
  public int getPort() {
    return port;
  }

  /**
   * @param port the port to set
   */
  public void setPort( int port ) {
    this.port = port;
  }

  public boolean equals( Object obj ) {
    if ( obj == this ) {
      return true;
    }
    if ( !( obj instanceof SocketPortAllocation ) ) {
      return false;
    }

    SocketPortAllocation allocation = (SocketPortAllocation) obj;

    return allocation.getPort() == port;
  }

  public int hashCode() {
    return Integer.valueOf( port ).hashCode();
  }

  /**
   * @return the lastRequested
   */
  public Date getLastRequested() {
    return lastRequested;
  }

  /**
   * @param lastRequested the lastRequested to set
   */
  public void setLastRequested( Date lastRequested ) {
    this.lastRequested = lastRequested;
  }

  /**
   * @return the pipelineName
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * @param pipelineName the pipelineName to set
   */
  public void setPipelineName( String pipelineName ) {
    this.pipelineName = pipelineName;
  }

  /**
   * @return the allocated
   */
  public boolean isAllocated() {
    return allocated;
  }

  /**
   * @param allocated the allocated to set
   */
  public void setAllocated( boolean allocated ) {
    this.allocated = allocated;
  }

  /**
   * @return the sourceTransformName
   */
  public String getSourceTransformName() {
    return sourceTransformName;
  }

  /**
   * @param sourceTransformName the sourceTransformName to set
   */
  public void setSourceTransformName( String sourceTransformName ) {
    this.sourceTransformName = sourceTransformName;
  }

  /**
   * @return the sourceTransformCopy
   */
  public String getSourceTransformCopy() {
    return sourceTransformCopy;
  }

  /**
   * @param sourceTransformCopy the sourceTransformCopy to set
   */
  public void setSourceTransformCopy( String sourceTransformCopy ) {
    this.sourceTransformCopy = sourceTransformCopy;
  }

  /**
   * @return the targetTransformName
   */
  public String getTargetTransformName() {
    return targetTransformName;
  }

  /**
   * @param targetTransformName the targetTransformName to set
   */
  public void setTargetTransformName( String targetTransformName ) {
    this.targetTransformName = targetTransformName;
  }

  /**
   * @return the targetTransformCopy
   */
  public String getTargetTransformCopy() {
    return targetTransformCopy;
  }

  /**
   * @param targetTransformCopy the targetTransformCopy to set
   */
  public void setTargetTransformCopy( String targetTransformCopy ) {
    this.targetTransformCopy = targetTransformCopy;
  }

  /**
   * @return the sourceSlaveName
   */
  public String getSourceSlaveName() {
    return sourceSlaveName;
  }

  /**
   * @param sourceSlaveName the sourceSlaveName to set
   */
  public void setSourceSlaveName( String sourceSlaveName ) {
    this.sourceSlaveName = sourceSlaveName;
  }

  /**
   * @return the targetSlaveName
   */
  public String getTargetSlaveName() {
    return targetSlaveName;
  }

  /**
   * @param targetSlaveName the targetSlaveName to set
   */
  public void setTargetSlaveName( String targetSlaveName ) {
    this.targetSlaveName = targetSlaveName;
  }

  /**
   * @return the carteObjectId
   */
  public String getClusterRunId() {
    return clusterRunId;
  }

  /**
   * @param clusterRunId the carteObjectId to set
   */
  public void setClusterRunId( String clusterRunId ) {
    this.clusterRunId = clusterRunId;
  }

}
