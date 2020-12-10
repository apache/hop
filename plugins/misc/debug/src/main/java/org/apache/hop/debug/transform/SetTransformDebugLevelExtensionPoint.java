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

package org.apache.hop.debug.transform;

import org.apache.hop.core.Condition;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.IRowListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@ExtensionPoint(
  id = "SetTransformDebugLevelExtensionPoint",
  description = "Set Transform Debug Level Extension Point Plugin",
  extensionPointId = "TransformationStartThreads"
)
/**
 * set the debug level right before the transform starts to run
 */
public class SetTransformDebugLevelExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
    Map<String, String> transformLevelMap = pipeline.getPipelineMeta().getAttributesMap().get( Defaults.DEBUG_GROUP );

    if ( transformLevelMap != null ) {

      log.logDetailed( "Set debug level information on pipeline : " + pipeline.getPipelineMeta().getName() );

      List<String> transformNames = new ArrayList<>();
      for ( String key : transformLevelMap.keySet() ) {

        int index = key.indexOf( " : " );
        if ( index > 0 ) {
          String transformName = key.substring( 0, index );
          if ( !transformNames.contains( transformName ) ) {
            transformNames.add( transformName );
          }
        }
      }

      for ( String transformName : transformNames ) {

        log.logDetailed( "Handling debug level for transform : " + transformName );

        try {

          final TransformDebugLevel debugLevel = DebugLevelUtil.getTransformDebugLevel( transformLevelMap, transformName );
          if ( debugLevel != null ) {

            log.logDetailed( "Found debug level info for transform " + transformName );

            List<IEngineComponent> transformCopies = pipeline.getComponentCopies( transformName );

            if ( debugLevel.getStartRow() < 0 && debugLevel.getEndRow() < 0 && debugLevel.getCondition().isEmpty() ) {

              log.logDetailed( "Set logging level for transform " + transformName + " to " + debugLevel.getLogLevel().getDescription() );

              // Just a general log level on the transform
              //
              String logLevelCode = transformLevelMap.get( transformName );
              for ( IEngineComponent transformCopy : transformCopies ) {
                LogLevel logLevel = debugLevel.getLogLevel();
                transformCopy.getLogChannel().setLogLevel( logLevel );
                log.logDetailed( "Applied logging level " + logLevel.getDescription() + " on transform copy " + transformCopy.getName() + "." + transformCopy.getCopyNr() );
              }
            } else {

              // We need to look at every row
              //
              for ( IEngineComponent transformCopy : transformCopies ) {

                final LogLevel baseLogLevel = transformCopy.getLogChannel().getLogLevel();
                final AtomicLong rowCounter = new AtomicLong( 0L );

                transformCopy.addRowListener( new IRowListener() {
                  @Override public void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                    rowCounter.incrementAndGet();
                    boolean enabled = false;

                    Condition condition = debugLevel.getCondition();

                    if ( debugLevel.getStartRow() > 0 && rowCounter.get() >= debugLevel.getStartRow() && debugLevel.getEndRow() >= 0 && debugLevel.getEndRow() >= rowCounter.get() ) {
                      // If we have a start and an end, we want to stay between start and end
                      enabled = true;
                    } else if ( debugLevel.getStartRow() <= 0 && debugLevel.getEndRow() >= 0 && rowCounter.get() <= debugLevel.getEndRow() ) {
                      // If don't have a start row, just and end...
                      enabled = true;
                    } else if ( debugLevel.getEndRow() <= 0 && debugLevel.getStartRow() >= 0 && rowCounter.get() >= debugLevel.getStartRow() ) {
                      enabled = true;
                    }

                    if ( ( debugLevel.getStartRow() <= 0 && debugLevel.getEndRow() <= 0 || enabled ) && !condition.isEmpty() ) {
                      enabled = condition.evaluate( rowMeta, row );
                    }

                    if ( enabled ) {
                      transformCopy.setLogLevel( debugLevel.getLogLevel() );
                    }
                  }

                  @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                    // Set the log level back to the original value.
                    //
                    transformCopy.getLogChannel().setLogLevel( baseLogLevel );
                  }

                  @Override public void errorRowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                  }
                } );

              }
            }
          }
        } catch ( Exception e ) {
          log.logError( "Unable to handle specific debug level for transform : " + transformName, e );
        }
      }

    }

  }
}
