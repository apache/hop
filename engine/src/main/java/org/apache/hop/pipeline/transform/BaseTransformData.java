/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transform;

import org.apache.hop.i18n.BaseMessages;

/**
 * This class is the base class for the ITransformData and contains the methods to set and retrieve the status of the
 * transform data.
 *
 * @author Matt
 * @since 20-jan-2005
 */
public abstract class BaseTransformData implements ITransformData {

  /**
   * The pkg used for i18n
   */
  private static Class<?> PKG = BaseTransform.class; // for i18n purposes, needed by Translator!!

  /**
   * The Enum TransformExecutionStatus.
   */
  public enum TransformExecutionStatus {

    /**
     * The status empty.
     */
    STATUS_EMPTY( BaseMessages.getString( PKG, "BaseTransform.status.Empty" ) ),

    /**
     * The status init.
     */
    STATUS_INIT( BaseMessages.getString( PKG, "BaseTransform.status.Init" ) ),

    /**
     * The status running.
     */
    STATUS_RUNNING( BaseMessages.getString( PKG, "BaseTransform.status.Running" ) ),

    /**
     * The status idle.
     */
    STATUS_IDLE( BaseMessages.getString( PKG, "BaseTransform.status.Idle" ) ),

    /**
     * The status finished.
     */
    STATUS_FINISHED( BaseMessages.getString( PKG, "BaseTransform.status.Finished" ) ),

    /**
     * The status stopped.
     */
    STATUS_STOPPED( BaseMessages.getString( PKG, "BaseTransform.status.Stopped" ) ),

    /**
     * The status disposed.
     */
    STATUS_DISPOSED( BaseMessages.getString( PKG, "BaseTransform.status.Disposed" ) ),

    /**
     * The status halted.
     */
    STATUS_HALTED( BaseMessages.getString( PKG, "BaseTransform.status.Halted" ) ),

    /**
     * The status paused.
     */
    STATUS_PAUSED( BaseMessages.getString( PKG, "BaseTransform.status.Paused" ) ),

    /**
     * The status halting.
     */
    STATUS_HALTING( BaseMessages.getString( PKG, "BaseTransform.status.Halting" ) );

    /**
     * The description.
     */
    private String description;

    /**
     * Instantiates a new transform execution status.
     *
     * @param description the description
     */
    private TransformExecutionStatus( String description ) {
      this.description = description;
    }

    /**
     * Gets the description.
     *
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
      return description;
    }
  }

  /**
   * The status.
   */
  private TransformExecutionStatus status;

  /**
   * Instantiates a new base transform data.
   */
  public BaseTransformData() {
    status = TransformExecutionStatus.STATUS_EMPTY;
  }

  /**
   * Set the status of the transform data.
   *
   * @param status the new status.
   */
  @Override public void setStatus( TransformExecutionStatus status ) {
    this.status = status;
  }

  /**
   * Get the status of this transform data.
   *
   * @return the status of the transform data
   */
  @Override public TransformExecutionStatus getStatus() {
    return status;
  }

  /**
   * Checks if is empty.
   *
   * @return true, if is empty
   */
  @Override public boolean isEmpty() {
    return status == TransformExecutionStatus.STATUS_EMPTY;
  }

  /**
   * Checks if is initialising.
   *
   * @return true, if is initialising
   */
  @Override public boolean isInitialising() {
    return status == TransformExecutionStatus.STATUS_INIT;
  }

  /**
   * Checks if is running.
   *
   * @return true, if is running
   */
  @Override public boolean isRunning() {
    return status == TransformExecutionStatus.STATUS_RUNNING;
  }

  /**
   * Checks if is idle.
   *
   * @return true, if is idle
   */
  @Override public boolean isIdle() {
    return status == TransformExecutionStatus.STATUS_IDLE;
  }

  /**
   * Checks if is finished.
   *
   * @return true, if is finished
   */
  @Override public boolean isFinished() {
    return status == TransformExecutionStatus.STATUS_FINISHED;
  }

  /**
   * Checks if is stopped.
   *
   * @return true, if is stopped
   */
  public boolean isStopped() {
    return status == TransformExecutionStatus.STATUS_STOPPED;
  }

  /**
   * Checks if is disposed.
   *
   * @return true, if is disposed
   */
  @Override public boolean isDisposed() {
    return status == TransformExecutionStatus.STATUS_DISPOSED;
  }
}
