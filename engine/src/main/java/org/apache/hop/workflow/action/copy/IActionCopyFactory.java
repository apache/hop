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

package org.apache.hop.workflow.action.copy;

import org.apache.hop.pipeline.transform.copy.CopyContext;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

/**
 * Factory interface for copying action metadata instances with configurable behavior.
 *
 * <p>This factory provides centralized, consistent copying behavior for actions throughout the
 * workflow system, replacing inconsistent manual cloning implementations with a unified approach
 * that properly handles state preservation.
 *
 * <p>Key benefits:
 *
 * <ul>
 *   <li>Fixes race conditions where changed states are lost during cloning
 *   <li>Provides configurable copying strategies via {@link CopyContext}
 *   <li>Ensures consistent behavior across all action types
 *   <li>Centralizes copying logic for easier maintenance
 * </ul>
 *
 * <p>Usage examples:
 *
 * <pre>{@code
 * // Basic copying with default behavior
 * IAction copy = factory.copy(originalAction);
 *
 * // Lightweight copying without state preservation
 * IAction copy = factory.copy(originalAction, CopyContext.LIGHTWEIGHT);
 *
 * // Same workflow copying preserving parent references
 * IAction copy = factory.copy(originalAction, CopyContext.SAME_PIPELINE);
 *
 * // ActionMeta copying
 * ActionMeta copy = factory.copy(originalActionMeta);
 * }</pre>
 */
public interface IActionCopyFactory {

  /**
   * Creates a copy of the specified action with default copying behavior.
   *
   * @param source the action to copy
   * @return a new action instance that is a copy of the source, or null if source is null
   */
  IAction copy(IAction source);

  /**
   * Creates a copy of the specified action using the provided copy context.
   *
   * @param source the action to copy
   * @param context the copy context specifying how the copy should behave
   * @return a new action instance that is a copy of the source, or null if source is null
   */
  IAction copy(IAction source, CopyContext context);

  /**
   * Creates a copy of the specified action metadata with default copying behavior.
   *
   * @param source the action metadata to copy
   * @return a new action metadata instance that is a copy of the source, or null if source is null
   */
  ActionMeta copy(ActionMeta source);

  /**
   * Creates a copy of the specified action metadata using the provided copy context.
   *
   * @param source the action metadata to copy
   * @param context the copy context specifying how the copy should behave
   * @return a new action metadata instance that is a copy of the source, or null if source is null
   */
  ActionMeta copy(ActionMeta source, CopyContext context);
}
