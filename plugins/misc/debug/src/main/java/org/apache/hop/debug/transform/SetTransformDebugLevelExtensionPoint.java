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

package org.apache.hop.debug.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;

@ExtensionPoint(
    id = "SetTransformDebugLevelExtensionPoint",
    description = "Set Transform Debug Level Extension Point Plugin",
    extensionPointId = "PipelineStartThreads")
/** set the debug level right before the transform starts to run */
public class SetTransformDebugLevelExtensionPoint
    implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline)
      throws HopException {
    Map<String, String> transformLevelMap =
        pipeline.getPipelineMeta().getAttributesMap().get(Defaults.DEBUG_GROUP);

    if (transformLevelMap != null) {
      log.logDetailed(
          "Set debug level information on pipeline : " + pipeline.getPipelineMeta().getName());

      // Figure out which transforms were involved from the map.
      // Trying to go after each transform in a very large pipeline might otherwise
      // slow things down.
      //
      List<String> transformNames = new ArrayList<>();
      for (String key : transformLevelMap.keySet()) {
        int index = key.indexOf(" : ");
        if (index > 0) {
          String transformName = key.substring(0, index);
          if (!transformNames.contains(transformName)) {
            transformNames.add(transformName);
          }
        }
      }

      for (String transformName : transformNames) {
        log.logDetailed("Handling debug level for transform : " + transformName);

        try {
          final TransformDebugLevel debugLevel =
              DebugLevelUtil.getTransformDebugLevel(transformLevelMap, transformName);
          if (debugLevel != null) {
            log.logDetailed("Found debug level info for transform " + transformName);

            List<IEngineComponent> transformCopies = pipeline.getComponentCopies(transformName);
            final LogLevel resolvedLogLevel =
                DebugLevelUtil.resolveLogLevel(variables, debugLevel.getLogLevel());

            if (!hasRowSelection(debugLevel)) {
              log.logDetailed(
                  "Set logging level for transform "
                      + transformName
                      + " to "
                      + resolvedLogLevel.getDescription());

              // Just a general log level on the transform
              //
              for (IEngineComponent transformCopy : transformCopies) {
                transformCopy.getLogChannel().setLogLevel(resolvedLogLevel);
                log.logDetailed(
                    "Applied logging level "
                        + resolvedLogLevel.getDescription()
                        + " on transform copy "
                        + transformCopy.getName()
                        + "."
                        + transformCopy.getCopyNr());
              }
            } else {
              // We need to look at every row to see whether the custom log level applies to it.
              //
              boolean readingRows =
                  DebugLevelUtil.isReadingRows(pipeline.getPipelineMeta(), transformName);

              log.logDetailed(
                  "Set logging level for the selected rows of transform "
                      + transformName
                      + " to "
                      + resolvedLogLevel.getDescription());

              for (IEngineComponent transformCopy : transformCopies) {
                transformCopy.addRowListener(
                    new TransformDebugLevelRowListener(
                        transformCopy,
                        debugLevel,
                        resolvedLogLevel,
                        transformCopy.getLogChannel().getLogLevel(),
                        readingRows));
              }
            }
          }
        } catch (Exception e) {
          log.logError("Unable to handle specific debug level for transform : " + transformName, e);
        }
      }
    }
  }

  /**
   * Does this configuration only apply the custom log level to a selection of the rows?
   *
   * @param debugLevel the custom logging configuration
   * @return true if a start row, an end row or a condition narrows the configuration down to a
   *     selection of rows
   */
  private static boolean hasRowSelection(TransformDebugLevel debugLevel) {
    return debugLevel.getStartRow() > 0
        || debugLevel.getEndRow() > 0
        || (debugLevel.getCondition() != null && !debugLevel.getCondition().isEmpty());
  }
}
