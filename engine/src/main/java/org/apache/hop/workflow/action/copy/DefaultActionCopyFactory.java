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

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.pipeline.transform.copy.CopyContext;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

/**
 * Default implementation of {@link IActionCopyFactory} that provides robust, configurable copying
 * behavior for action instances and metadata.
 *
 * <p>This factory addresses the race condition where action cloning operations would lose the
 * changed state, causing save buttons to not activate properly after editing actions.
 *
 * <p>The factory supports different copying strategies:
 *
 * <ul>
 *   <li><b>DEFAULT</b>: Standard copying with proper state preservation
 *   <li><b>LIGHTWEIGHT</b>: Minimal copying without state preservation (for temporary use)
 *   <li><b>SAME_PIPELINE</b>: Copying within the same workflow, preserving parent references
 * </ul>
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Thread-safe singleton pattern
 *   <li>Proper changed state preservation
 *   <li>Comprehensive error handling
 *   <li>Configurable copying strategies
 *   <li>Backward compatibility with existing clone() methods
 * </ul>
 *
 * <p>This implementation fixes the core issue where:
 *
 * <ol>
 *   <li>Action dialogs work with action instances that have changed=true
 *   <li>When dialog closes, replaceMeta() calls action.clone()
 *   <li>clone() creates a NEW instance with changed=false (default state)
 *   <li>Original action (with changed=true) is replaced with new clone (changed=false)
 *   <li>Later hasChanged() checks see the new clone which returns false
 * </ol>
 */
public class DefaultActionCopyFactory implements IActionCopyFactory {

  private static final ILogChannel log = LogChannel.GENERAL;
  private static volatile DefaultActionCopyFactory instance;

  /** Private constructor to enforce singleton pattern. */
  private DefaultActionCopyFactory() {
    // Singleton pattern
  }

  /**
   * Gets the singleton instance of the default action copy factory.
   *
   * @return the singleton instance
   */
  public static DefaultActionCopyFactory getInstance() {
    if (instance == null) {
      synchronized (DefaultActionCopyFactory.class) {
        if (instance == null) {
          instance = new DefaultActionCopyFactory();
          log.logDetailed("DefaultActionCopyFactory singleton instance created");
        }
      }
    }
    return instance;
  }

  @Override
  public IAction copy(IAction source) {
    return copy(source, CopyContext.DEFAULT);
  }

  @Override
  public IAction copy(IAction source, CopyContext context) {
    if (source == null) {
      log.logDebug("Attempted to copy null action, returning null");
      return null;
    }

    if (context == null) {
      log.logDebug("Null context provided, using DEFAULT context");
      context = CopyContext.DEFAULT;
    }

    try {
      log.logRowlevel(
          "Copying action: " + source.getClass().getSimpleName() + " with context: " + context);

      // Capture the changed state before cloning
      boolean hadChanges = context.isPreserveChangedState() && source.hasChanged();

      // Perform the actual clone operation using the existing clone method
      IAction copy = (IAction) source.clone();

      if (copy == null) {
        log.logError(
            "Clone operation returned null for action: " + source.getClass().getSimpleName());
        return null;
      }

      // Apply context-specific behavior
      if (context.isPreserveChangedState() && hadChanges) {
        // Restore the changed state that was lost during cloning
        copy.setChanged();
        log.logRowlevel("Restored changed state to copied action");
      } else if (!context.isPreserveChangedState()) {
        // Explicitly clear changed state for lightweight copies
        copy.setChanged(false);
        log.logRowlevel("Cleared changed state for lightweight copy");
      }

      log.logRowlevel("Successfully copied action: " + source.getClass().getSimpleName());
      return copy;

    } catch (Exception e) {
      log.logError("Error copying action: " + source.getClass().getSimpleName(), e);

      // Fallback: try to use the original clone method
      try {
        return (IAction) source.clone();
      } catch (Exception fallbackException) {
        log.logError(
            "Fallback clone also failed for action: " + source.getClass().getSimpleName(),
            fallbackException);
        return null;
      }
    }
  }

  @Override
  public ActionMeta copy(ActionMeta source) {
    return copy(source, CopyContext.DEFAULT);
  }

  @Override
  public ActionMeta copy(ActionMeta source, CopyContext context) {
    if (source == null) {
      log.logDebug("Attempted to copy null ActionMeta, returning null");
      return null;
    }

    if (context == null) {
      log.logDebug("Null context provided, using DEFAULT context");
      context = CopyContext.DEFAULT;
    }

    try {
      log.logRowlevel(
          "Copying ActionMeta: "
              + (source.getAction() != null
                  ? source.getAction().getClass().getSimpleName()
                  : "null")
              + " with context: "
              + context);

      // Create a new ActionMeta instance
      ActionMeta copy = new ActionMeta();

      // Copy the underlying action using our action copy logic
      if (source.getAction() != null) {
        IAction actionCopy = copy(source.getAction(), context);
        copy.setAction(actionCopy);
      }

      // Copy other ActionMeta properties
      copy.setSelected(source.isSelected());
      if (source.getLocation() != null) {
        copy.setLocation(source.getLocation().x, source.getLocation().y);
      }
      copy.setLaunchingInParallel(source.isLaunchingInParallel());

      // Copy attributes map
      if (source.getAttributesMap() != null) {
        for (final var attribute : source.getAttributesMap().entrySet()) {
          copy.getAttributesMap().put(attribute.getKey(), attribute.getValue());
        }
      }

      // Set the parent workflow meta if context preserves it
      if (context == CopyContext.SAME_PIPELINE && source.getParentWorkflowMeta() != null) {
        copy.setParentWorkflowMeta(source.getParentWorkflowMeta());
      }

      // Apply changed state based on context - this fixes the race condition
      if (context.isPreserveChangedState()) {
        // This will call setChanged() which sets the changed state on the underlying action
        copy.setChanged();
        log.logRowlevel("Set changed state on copied ActionMeta");
      }

      log.logRowlevel("Successfully copied ActionMeta");
      return copy;

    } catch (Exception e) {
      log.logError(
          "Error copying ActionMeta: "
              + (source.getAction() != null
                  ? source.getAction().getClass().getSimpleName()
                  : "null"),
          e);

      // Fallback: try to use the original clone method
      try {
        return source.clone();
      } catch (Exception fallbackException) {
        log.logError("Fallback clone also failed for ActionMeta", fallbackException);
        return null;
      }
    }
  }
}
