/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transform.copy;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Default implementation of the transform metadata copy factory. This implementation provides
 * robust cloning with proper state preservation, handling all the edge cases that can occur during
 * transform metadata copying.
 */
public class DefaultTransformMetaCopyFactory implements ITransformMetaCopyFactory {

  private static final ILogChannel log = new LogChannel("DefaultTransformMetaCopyFactory");

  // Singleton instance for convenience
  private static final DefaultTransformMetaCopyFactory INSTANCE =
      new DefaultTransformMetaCopyFactory();

  /**
   * Get the singleton instance of the default copy factory.
   *
   * @return The singleton instance
   */
  public static DefaultTransformMetaCopyFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public TransformMeta copy(TransformMeta source) {
    return copy(source, CopyContext.DEFAULT);
  }

  @Override
  public TransformMeta copy(TransformMeta source, CopyContext context) {
    if (source == null) {
      return null;
    }

    if (log.isDebug()) {
      log.logDebug("Copying TransformMeta: " + source.getName() + " with context: " + context);
    }

    TransformMeta copy = new TransformMeta();

    // Copy basic properties
    copy.setTransformPluginId(source.getTransformPluginId());
    copy.setName(source.getName());
    copy.setSelected(source.isSelected());
    copy.setDistributes(source.isDistributes());
    copy.setRowDistribution(source.getRowDistribution());
    copy.setCopiesString(source.getCopiesString());
    copy.setDescription(source.getDescription());
    copy.setTerminator(source.hasTerminator());

    // Copy location
    if (source.getLocation() != null) {
      copy.setLocation(new Point(source.getLocation().x, source.getLocation().y));
    }

    // Copy inner transform metadata
    if (source.getTransform() != null) {
      ITransformMeta innerCopy = copy(source.getTransform(), context);
      copy.setTransform(innerCopy);
    }

    // Copy partitioning metadata if requested
    if (context.isCopyPartitioning()) {
      if (source.getTransformPartitioningMeta() != null) {
        copy.setTransformPartitioningMeta(source.getTransformPartitioningMeta().clone());
      }
      if (source.getTargetTransformPartitioningMeta() != null) {
        copy.setTargetTransformPartitioningMeta(
            source.getTargetTransformPartitioningMeta().clone());
      }
    }

    // Copy error handling metadata if requested
    if (context.isCopyErrorHandling() && source.getTransformErrorMeta() != null) {
      copy.setTransformErrorMeta(source.getTransformErrorMeta().clone());
    }

    // Copy attributes if requested
    if (context.isCopyAttributes()) {
      copy.setAttributesMap(copyAttributesMap(source.getAttributesMap()));
    }

    // Preserve parent references if requested
    if (context.isPreserveParentReferences()) {
      copy.setParentPipelineMeta(source.getParentPipelineMeta());
    }

    // Handle changed state preservation/clearing based on context
    if (context.isPreserveChangedState()) {
      // Check if the source or its inner transform has changes
      boolean hasChanges =
          source.hasChanged()
              || (source.getTransform() != null && source.getTransform().hasChanged());

      if (hasChanges) {
        if (log.isDebug()) {
          log.logDebug("Preserving changed state for TransformMeta: " + source.getName());
        }
        copy.setChanged();
        if (copy.getTransform() != null) {
          copy.getTransform().setChanged();
        }
      }
    } else {
      // Explicitly clear changed state if not preserving it
      if (log.isDebug()) {
        log.logDebug("Clearing changed state for TransformMeta: " + source.getName());
      }
      copy.setChanged(false);
      if (copy.getTransform() != null
          && copy.getTransform() instanceof org.apache.hop.pipeline.transform.BaseTransformMeta) {
        ((org.apache.hop.pipeline.transform.BaseTransformMeta) copy.getTransform())
            .setChanged(false);
      }
    }

    return copy;
  }

  @Override
  public ITransformMeta copy(ITransformMeta source) {
    return copy(source, CopyContext.DEFAULT);
  }

  @Override
  public ITransformMeta copy(ITransformMeta source, CopyContext context) {
    if (source == null) {
      return null;
    }

    if (log.isDebug()) {
      log.logDebug(
          "Copying ITransformMeta: "
              + source.getClass().getSimpleName()
              + " with context: "
              + context);
    }

    try {
      // Capture the changed state before cloning
      boolean hadChanges = context.isPreserveChangedState() && source.hasChanged();

      // Perform the clone
      ITransformMeta copy = (ITransformMeta) source.clone();

      // Restore changed state if needed
      if (hadChanges) {
        if (log.isDebug()) {
          log.logDebug(
              "Restoring changed state for ITransformMeta: " + source.getClass().getSimpleName());
        }
        copy.setChanged();
      }

      return copy;

    } catch (Exception e) {
      log.logError("Error copying ITransformMeta: " + source.getClass().getSimpleName(), e);
      throw new RuntimeException("Failed to copy transform metadata", e);
    }
  }

  /**
   * Creates a deep copy of the attributes map.
   *
   * @param source The source attributes map
   * @return A deep copy of the attributes map
   */
  private Map<String, Map<String, String>> copyAttributesMap(
      Map<String, Map<String, String>> source) {
    if (source == null) {
      return new HashMap<>();
    }

    Map<String, Map<String, String>> copy = new HashMap<>(source.size());
    for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
      Map<String, String> value = entry.getValue();
      HashMap<String, String> valueCopy = (value == null) ? null : new HashMap<>(value);
      copy.put(entry.getKey(), valueCopy);
    }
    return copy;
  }
}
