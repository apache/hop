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

package org.apache.hop.pipeline.transform;

import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;

/**
 * This defines the basic interface for the data used by a thread. This will allow us to stop execution of threads and
 * restart them later on without loosing track of the situation. Typically the ITransformData implementing class will
 * contain result sets, temporary data, caching indexes, etc.
 *
 * @author Matt
 * @since 20-jan-2005
 */
public interface ITransformData {

  /**
   * Sets the status.
   *
   * @param status the new status
   */
  void setStatus( ComponentExecutionStatus status );

  /**
   * Gets the status.
   *
   * @return the status
   */
  ComponentExecutionStatus getStatus();

  /**
   * Checks if is empty.
   *
   * @return true, if is empty
   */
  boolean isEmpty();

  /**
   * Checks if is initialising.
   *
   * @return true, if is initialising
   */
  boolean isInitialising();

  /**
   * Checks if is running.
   *
   * @return true, if is running
   */
  boolean isRunning();

  /**
   * Checks if is idle.
   *
   * @return true, if is idle
   */
  boolean isIdle();

  /**
   * Checks if is finished.
   *
   * @return true, if is finished
   */
  boolean isFinished();

  /**
   * Checks if is disposed.
   *
   * @return true, if is disposed
   */
  boolean isDisposed();
}
